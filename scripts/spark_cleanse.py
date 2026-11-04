"""
spark_cleanse.py
----------------
PySpark job that cleanses ONE entity (customers, products or orders).
Airflow runs it three times with spark-submit, e.g.:

    spark-submit --master spark://spark-master:7077 \
        /opt/airflow/scripts/spark_cleanse.py --entity customers --run-id 20261103T223000

Steps (in order):
  1. Read raw file in PERMISSIVE mode and drop corrupt lines
  2. Drop records with a null / invalid primary key
  3. Normalize strings (trim, case), parse mixed date formats to ISO, cast types
  4. Remove exact duplicates
  5. Remove near-duplicates (same ID): keep the most complete, then most recent record
  6. Validate values (orders: negative amounts, discount > total, future dates)
  7. Referential integrity (orders: remove orphans that reference missing customers)
  8. Fill remaining nulls using business rules
  9. Write clean data to Parquet: data/cleaned/<run_id>/<entity>/
 10. Write rejected records and statistics: data/reports/<run_id>/

Exit code is non-zero on any error, so the Airflow task fails.
"""

import argparse
import sys
import time
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_utils import (default_run_id, get_logger, get_spark, load_config,
                         run_path, save_json)

# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
ENTITIES = {
    "customers": {
        "file": "customers.csv", "format": "csv", "pk": "customer_id",
        "columns": ["customer_id", "first_name", "last_name", "email", "phone", "city",
                    "registration_date", "customer_type", "last_updated"],
        "output": ["customer_id", "first_name", "last_name", "email", "phone", "city",
                   "registration_date", "customer_type"],
    },
    "products": {
        "file": "products.json", "format": "json", "pk": "product_id",
        "columns": ["product_id", "product_name", "category", "brand", "unit_price",
                    "stock_quantity", "is_active", "last_updated"],
        "output": ["product_id", "product_name", "category", "brand", "unit_price",
                   "stock_quantity", "is_active"],
    },
    "orders": {
        "file": "orders.csv", "format": "csv", "pk": "order_id",
        "columns": ["order_id", "customer_id", "product_id", "quantity", "order_date",
                    "order_status", "payment_method", "total_amount", "discount_amount",
                    "shipping_cost", "last_updated"],
        "output": ["order_id", "customer_id", "product_id", "quantity", "order_date",
                   "order_status", "payment_method", "total_amount", "discount_amount",
                   "shipping_cost"],
    },
}

# Canonical spellings for categorical values (lower-case key -> correct value)
CUSTOMER_TYPES = {"regular": "Regular", "premium": "Premium", "vip": "VIP"}
ORDER_STATUSES = {"pending": "Pending", "shipped": "Shipped", "delivered": "Delivered"}
PAYMENT_METHODS = {"credit card": "Credit Card", "debit card": "Debit Card", "paypal": "PayPal",
                   "bank transfer": "Bank Transfer", "cash on delivery": "Cash on Delivery"}

# The three date styles found in the raw data
DATE_FORMATS = ["yyyy-MM-dd", "MM/dd/yyyy", "MMM d, yyyy"]
TIMESTAMP_FORMATS = ["yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy HH:mm", "MMM d, yyyy HH:mm:ss"]
ISO_PATTERN = r"^\d{4}-\d{2}-\d{2}"

MONEY = "decimal(10,2)"


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def clean_str(col):
    """Trim whitespace (including tabs); turn empty strings into null."""
    trimmed = F.regexp_replace(F.col(col), r"^\s+|\s+$", "")
    return F.when(trimmed == "", None).otherwise(trimmed)


def smart_case(col):
    """Fix ALL-CAPS or all-lower text to Title Case ("NEW YORK" -> "New York"),
    but leave mixed-case values alone so names like "DarkRed" or "McKee" survive."""
    c = clean_str(col)
    return F.when((c == F.upper(c)) | (c == F.lower(c)), F.initcap(c)).otherwise(c)


def canonical(col, mapping):
    """Map any capitalisation of a known value to its canonical spelling."""
    lookup = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return lookup[F.lower(clean_str(col))]


def parse_timestamp(col, formats):
    """Try each date format in turn; the first one that parses wins (else null)."""
    return F.coalesce(*[F.try_to_timestamp(clean_str(col), F.lit(fmt)) for fmt in formats])


def to_int(col):
    return clean_str(col).cast("double").cast("int")


def to_money(col):
    return clean_str(col).cast("double").cast(MONEY)


def not_null(*cols):
    """Number of non-null values among the given columns (record completeness)."""
    return reduce(lambda a, b: a + b, [F.col(c).isNotNull().cast("int") for c in cols])


# ---------------------------------------------------------------------------
# Cleanser: tracks statistics and rejected records as it goes
# ---------------------------------------------------------------------------
class Cleanser:
    def __init__(self, spark, entity, run_id, config, log):
        self.spark, self.entity, self.run_id = spark, entity, run_id
        self.config, self.log = config, log
        self.spec = ENTITIES[entity]
        self.pk = self.spec["pk"]
        self.dropped = {}
        self.fixed = {}
        self.rejected = []

    # -- bookkeeping -------------------------------------------------------
    def drop_where(self, df, condition, reason):
        """Remove rows matching `condition`, counting them and keeping a copy."""
        condition = F.coalesce(condition, F.lit(False))
        bad = df.filter(condition)
        n = bad.count()
        if n:
            self.rejected.append(self._as_rejected(bad, reason))
        self.dropped[reason] = self.dropped.get(reason, 0) + n
        self.log.info("  dropped %7d  %s", n, reason)
        return df.filter(~condition)

    def _as_rejected(self, df, reason):
        """Common shape for rejected rows: id, reason, full record as JSON."""
        cols = [c for c in df.columns if c != "_corrupt_record"]
        record = (F.col("_corrupt_record") if "_corrupt_record" in df.columns
                  and reason.startswith("corrupt") else F.to_json(F.struct(*cols)))
        return df.select(F.col(self.pk).cast("string").alias("record_id"),
                         F.lit(reason).alias("reason"),
                         record.cast("string").alias("record"))

    # -- step 1 ------------------------------------------------------------
    def read_raw(self):
        path = f"{self.config['paths']['raw']}/{self.spec['file']}"
        # Read every column as text first; types are applied after cleaning.
        schema = StructType([StructField(c, StringType()) for c in self.spec["columns"]]
                            + [StructField("_corrupt_record", StringType())])
        reader = (self.spark.read.schema(schema)
                  .option("mode", "PERMISSIVE")                     # keep bad lines, flag them
                  .option("columnNameOfCorruptRecord", "_corrupt_record"))
        if self.spec["format"] == "csv":
            df = reader.option("header", True).csv(path)
        else:
            df = reader.json(path)
        # Spark requires caching before filtering on the corrupt-record column
        return df.cache()

    # -- step 3: per-entity normalisation ------------------------------------
    def normalize(self, df):
        e = self.entity
        if e == "customers":
            df = df.select(
                F.col("customer_id"),
                smart_case("first_name").alias("first_name"),
                smart_case("last_name").alias("last_name"),
                F.lower(clean_str("email")).alias("email"),
                clean_str("phone").alias("phone"),
                smart_case("city").alias("city"),
                F.to_date(parse_timestamp("registration_date", DATE_FORMATS)).alias("registration_date"),
                canonical("customer_type", CUSTOMER_TYPES).alias("customer_type"),
                parse_timestamp("last_updated", TIMESTAMP_FORMATS).alias("last_updated"),
            )
        elif e == "products":
            df = df.select(
                F.col("product_id"),
                smart_case("product_name").alias("product_name"),
                smart_case("category").alias("category"),
                smart_case("brand").alias("brand"),
                to_money("unit_price").alias("unit_price"),
                to_int("stock_quantity").alias("stock_quantity"),
                F.lower(clean_str("is_active")).cast("boolean").alias("is_active"),
                parse_timestamp("last_updated", TIMESTAMP_FORMATS).alias("last_updated"),
            )
        else:  # orders
            df = df.select(
                F.col("order_id"),
                to_int("customer_id").alias("customer_id"),
                to_int("product_id").alias("product_id"),
                to_int("quantity").alias("quantity"),
                parse_timestamp("order_date", TIMESTAMP_FORMATS).alias("order_date"),
                canonical("order_status", ORDER_STATUSES).alias("order_status"),
                canonical("payment_method", PAYMENT_METHODS).alias("payment_method"),
                to_money("total_amount").alias("total_amount"),
                to_money("discount_amount").alias("discount_amount"),
                to_money("shipping_cost").alias("shipping_cost"),
                parse_timestamp("last_updated", TIMESTAMP_FORMATS).alias("last_updated"),
            )
        return df

    def count_fixes(self, raw):
        """How many values the normalisation step corrects (for the report)."""
        text_cols = [c for c in self.spec["columns"] if c not in (self.pk,)]
        date_cols = [c for c in ("registration_date", "order_date", "last_updated") if c in raw.columns]
        case_checks = {
            "customers": {"city": smart_case("city"), "email": F.lower(clean_str("email")),
                          "customer_type": canonical("customer_type", CUSTOMER_TYPES)},
            "products": {"category": smart_case("category"),
                         "product_name": smart_case("product_name")},
            "orders": {"order_status": canonical("order_status", ORDER_STATUSES),
                       "payment_method": canonical("payment_method", PAYMENT_METHODS)},
        }[self.entity]

        aggs = [F.sum((F.col(c) != F.regexp_replace(F.col(c), r"^\s+|\s+$", "")).cast("int"))
                .alias(f"whitespace_trimmed__{c}") for c in text_cols]
        aggs += [F.sum((clean_str(c) != expr).cast("int")).alias(f"case_normalized__{c}")
                 for c, expr in case_checks.items()]
        aggs += [F.sum((~clean_str(c).rlike(ISO_PATTERN)).cast("int")).alias(f"date_reformatted__{c}")
                 for c in date_cols]
        row = raw.agg(*aggs).first().asDict()
        summary = {}
        for key, value in row.items():
            kind, col = key.split("__")
            summary.setdefault(kind, {})[col] = int(value or 0)
        for kind, cols in summary.items():
            self.fixed[kind] = {"total": sum(cols.values()), "by_column": cols}

    # -- steps 4-5 -----------------------------------------------------------
    def deduplicate(self, df):
        before = df.count()
        df = df.dropDuplicates()                        # exact duplicates (after normalisation)
        exact = before - df.count()
        self.dropped["exact duplicate"] = exact
        self.log.info("  dropped %7d  exact duplicate", exact)

        # Near-duplicates: same ID, different data. Keep the most complete record,
        # and if equally complete, the most recently updated one.
        business_cols = [c for c in self.spec["output"] if c != self.pk]
        w = Window.partitionBy(self.pk).orderBy(not_null(*business_cols).desc(),
                                                F.col("last_updated").desc_nulls_last())
        ranked = df.withColumn("_rank", F.row_number().over(w)).cache()
        df = self.drop_where(ranked, F.col("_rank") > 1,
                             "near duplicate (kept most complete / most recent)").drop("_rank")
        return df

    # -- step 6 ------------------------------------------------------------
    def validate(self, df):
        if self.entity == "orders":
            df = self.drop_where(df, F.col("order_date").isNull(), "invalid or missing order_date")
            df = self.drop_where(df, F.col("order_date") > F.current_timestamp(),
                                 "impossible order_date (in the future)")
            df = self.drop_where(df, F.col("total_amount").isNull(), "missing total_amount")
            df = self.drop_where(df, F.col("total_amount") < 0, "negative total_amount")
            df = self.drop_where(df, F.col("discount_amount") > F.col("total_amount"),
                                 "discount_amount greater than total_amount")
            df = self.drop_where(df, F.col("quantity").isNull() | (F.col("quantity") <= 0),
                                 "invalid quantity")
            df = self.drop_where(df, F.col("product_id").isNull(), "missing product_id")
        elif self.entity == "products":
            df = self.drop_where(df, F.col("unit_price").isNull() | (F.col("unit_price") < 0),
                                 "missing or negative unit_price")
        elif self.entity == "customers":
            df = self.drop_where(df, F.col("registration_date") > F.current_date(),
                                 "impossible registration_date (in the future)")
            # e-mail must be unique: if two different customers share one, keep the lowest ID
            w = Window.partitionBy("email").orderBy(self.pk)
            df = df.withColumn("_email_rank", F.when(F.col("email").isNotNull(),
                                                     F.row_number().over(w)).otherwise(1))
            df = self.drop_where(df, F.col("_email_rank") > 1, "duplicate email on another customer")
            df = df.drop("_email_rank")
        return df

    # -- step 7 ------------------------------------------------------------
    def referential_integrity(self, df):
        if self.entity != "orders":
            return df
        customers_path = run_path(self.config, "cleaned", self.run_id, "customers")
        try:
            customer_ids = self.spark.read.parquet(customers_path).select("customer_id")
        except Exception as e:
            raise RuntimeError(f"Cleaned customers not found at {customers_path}. "
                               "spark_cleanse_customers must run before orders.") from e
        # Broadcast join: the customer ID list is small, so send it to every executor
        valid = F.broadcast(customer_ids.withColumn("_valid", F.lit(True)))
        joined = df.join(valid, on="customer_id", how="left")
        df = self.drop_where(joined, F.col("_valid").isNull(),
                             "orphan order (customer_id not in cleaned customers)").drop("_valid")
        return df

    # -- step 8 ------------------------------------------------------------
    def fill_nulls(self, df):
        rules = {
            "customers": {"first_name": "Unknown", "last_name": "Unknown",
                          "city": "Unknown", "customer_type": "Regular"},
            "products": {"product_name": "Unknown Product", "category": "Uncategorized",
                         "brand": "Unknown", "stock_quantity": 0, "is_active": False},
            "orders": {"order_status": "Pending", "payment_method": "Unknown",
                       "shipping_cost": 0.0, "discount_amount": 0.0},
        }[self.entity]
        counts = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in rules]).first()
        filled = {c: int(counts[c] or 0) for c in rules}
        self.fixed["nulls_filled"] = {"total": sum(filled.values()), "by_column": filled,
                                      "rules": {c: str(v) for c, v in rules.items()}}
        self.log.info("  filled nulls: %s", filled)
        if self.entity == "customers":
            self.log.info("  (null e-mails are kept as NULL and flagged in the stats - "
                          "there is no sensible default e-mail)")
        for col, value in rules.items():
            if col in ("shipping_cost", "discount_amount"):
                df = df.withColumn(col, F.coalesce(F.col(col), F.lit(value).cast(MONEY)))
            else:
                df = df.fillna({col: value})
        return df

    # -- main flow ---------------------------------------------------------
    def run(self):
        started = datetime.now()
        t0 = time.time()
        log = self.log
        log.info("=== Cleansing %s (run %s) ===", self.entity, self.run_id)

        raw = self.read_raw()
        records_in = raw.count()
        log.info("  read     %7d  raw records from %s", records_in, self.spec["file"])

        # 1. corrupt lines
        df = self.drop_where(raw, F.col("_corrupt_record").isNotNull(), "corrupt / malformed line")
        df = df.drop("_corrupt_record")

        # 2. null or non-numeric primary key
        df = df.withColumn(self.pk, to_int(self.pk))
        df = self.drop_where(df, F.col(self.pk).isNull(), "null or invalid primary key")

        # 3. normalise strings, dates and types
        self.count_fixes(df)
        df = self.normalize(df).cache()
        for kind, info in self.fixed.items():
            log.info("  fixed    %7d  %s", info["total"], kind.replace("_", " "))

        # 4-5. duplicates
        df = self.deduplicate(df)
        if self.entity == "customers":
            self.fixed["null_emails_flagged"] = {"total": df.filter(F.col("email").isNull()).count()}

        # 6-7. validation and referential integrity
        df = self.validate(df)
        df = self.referential_integrity(df)

        # 8. fill remaining nulls
        df = self.fill_nulls(df)

        # 9. write clean Parquet
        out = df.select(*self.spec["output"]).repartition(2)
        out_path = run_path(self.config, "cleaned", self.run_id, self.entity)
        out.write.mode("overwrite").parquet(out_path)
        records_out = self.spark.read.parquet(out_path).count()

        # 10. rejected records + statistics
        reports = run_path(self.config, "reports", self.run_id)
        if self.rejected:
            rejected = reduce(DataFrame.unionByName, self.rejected)
            (rejected.coalesce(1).write.mode("overwrite")
             .option("header", True).option("escape", "\"")
             .csv(f"{reports}/rejected_{self.entity}"))

        stats = {
            "entity": self.entity,
            "run_id": self.run_id,
            "records_in": records_in,
            "records_out": records_out,
            "records_dropped": records_in - records_out,
            "dropped_by_reason": self.dropped,
            "values_fixed": self.fixed,
            "output_path": out_path,
            "started_at": started.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "duration_seconds": round(time.time() - t0, 1),
        }
        save_json(stats, f"{reports}/cleansing_{self.entity}.json")

        log.info("  -------------------------------------------------")
        log.info("  records in:      %7d", records_in)
        log.info("  records dropped: %7d", records_in - records_out)
        log.info("  records out:     %7d  -> %s", records_out, out_path)
        log.info("  finished in %.1fs", stats["duration_seconds"])

        # Safety check: the primary key must now be unique
        dupes = out.groupBy(self.pk).count().filter("count > 1").count()
        if dupes:
            raise ValueError(f"{dupes} duplicate {self.pk} values remain after cleansing")
        return stats


def main():
    parser = argparse.ArgumentParser(description="Cleanse one entity with PySpark")
    parser.add_argument("--entity", required=True, choices=sorted(ENTITIES))
    parser.add_argument("--run-id", default=None, help="pipeline run ID (default: timestamp)")
    args = parser.parse_args()

    config = load_config()
    run_id = args.run_id or default_run_id()
    log = get_logger(f"spark_cleanse_{args.entity}", config)

    spark = None
    try:
        spark = get_spark(f"cleanse_{args.entity}", config)
        Cleanser(spark, args.entity, run_id, config, log).run()
    except Exception as e:  # any failure -> non-zero exit code -> Airflow task fails
        log.exception("Cleansing of %s FAILED: %s", args.entity, e)
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
