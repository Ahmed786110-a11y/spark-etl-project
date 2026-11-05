"""
benchmark_pandas_vs_spark.py
----------------------------
Optional benchmark for the report's "Comparison" section (bonus item):
runs the SAME simplified cleansing + aggregation on orders.csv twice,
once with pandas (the Project 1 approach) and once with PySpark, and times both.

Pipeline (identical logic in both versions):
    read CSV as text -> drop malformed lines -> trim text -> parse 3 date formats
    -> keep latest record per order_id -> drop invalid amounts -> revenue by month

Run inside Docker (Spark runs in local mode inside the container):
    python /opt/airflow/scripts/benchmark_pandas_vs_spark.py
    python /opt/airflow/scripts/benchmark_pandas_vs_spark.py --file /opt/airflow/data/bench/raw/orders.csv
"""

import argparse
import os
import time

import pandas as pd

DATE_FORMATS_PD = ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%b %d, %Y %H:%M:%S"]
DATE_FORMATS_SPARK = ["yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy HH:mm", "MMM d, yyyy HH:mm:ss"]


def run_pandas(path):
    t0 = time.time()
    df = pd.read_csv(path, dtype=str, on_bad_lines="skip", engine="python")
    df = df[df["customer_id"].notna() & df["order_id"].str.fullmatch(r"\s*\d+\s*", na=False)]
    for c in df.columns:
        df[c] = df[c].str.strip()
    parsed = None
    for fmt in DATE_FORMATS_PD:
        attempt = pd.to_datetime(df["order_date"], format=fmt, errors="coerce")
        parsed = attempt if parsed is None else parsed.fillna(attempt)
    df["order_date"] = parsed
    updated = None
    for fmt in DATE_FORMATS_PD:
        attempt = pd.to_datetime(df["last_updated"], format=fmt, errors="coerce")
        updated = attempt if updated is None else updated.fillna(attempt)
    df["last_updated"] = updated
    for c in ("total_amount", "discount_amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("last_updated", ascending=False).drop_duplicates("order_id")
    df = df[(df["total_amount"] >= 0) & (df["discount_amount"].fillna(0) <= df["total_amount"])]
    df = df[df["order_date"].notna()]
    df["revenue"] = df["total_amount"] - df["discount_amount"].fillna(0)
    monthly = df.groupby(df["order_date"].dt.strftime("%Y-%m"))["revenue"].sum()
    return time.time() - t0, len(df), round(monthly.sum(), 2)


def run_spark(path):
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    t_start = time.time()
    spark = (SparkSession.builder.master("local[2]").appName("benchmark")
             .config("spark.driver.memory", "512m")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.time() - t_start

    t0 = time.time()
    df = (spark.read.option("header", True).option("mode", "DROPMALFORMED")
          .csv(path))
    df = df.filter(F.col("customer_id").isNotNull() & F.col("order_id").rlike(r"^\s*\d+\s*$"))
    df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])

    def parse(col):
        return F.coalesce(*[F.try_to_timestamp(F.col(col), F.lit(f)) for f in DATE_FORMATS_SPARK])

    df = (df.withColumn("order_date", parse("order_date"))
            .withColumn("last_updated", parse("last_updated"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .withColumn("discount_amount", F.col("discount_amount").cast("double")))
    w = Window.partitionBy("order_id").orderBy(F.col("last_updated").desc_nulls_last())
    df = df.withColumn("_r", F.row_number().over(w)).filter("_r = 1").drop("_r")
    df = df.filter((F.col("total_amount") >= 0)
                   & (F.coalesce("discount_amount", F.lit(0.0)) <= F.col("total_amount"))
                   & F.col("order_date").isNotNull())
    df = df.withColumn("revenue", F.col("total_amount") - F.coalesce("discount_amount", F.lit(0.0))).cache()
    rows = df.count()
    total = df.groupBy(F.date_format("order_date", "yyyy-MM")).agg(F.sum("revenue").alias("r")) \
              .agg(F.sum("r")).first()[0]
    elapsed = time.time() - t0
    spark.stop()
    return elapsed, startup, rows, round(total, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=os.path.join(os.environ.get("DATA_DIR", "/opt/airflow/data"),
                                                       "raw", "orders.csv"))
    args = parser.parse_args()
    size_mb = os.path.getsize(args.file) / 1e6
    print(f"Benchmark on {args.file} ({size_mb:.1f} MB)")

    p_time, p_rows, p_total = run_pandas(args.file)
    print(f"  pandas : {p_time:6.1f}s   rows kept {p_rows:,}   revenue {p_total:,.2f}")

    s_time, s_start, s_rows, s_total = run_spark(args.file)
    print(f"  spark  : {s_time:6.1f}s   rows kept {s_rows:,}   revenue {s_total:,.2f}"
          f"   (+ {s_start:.1f}s to start Spark)")


if __name__ == "__main__":
    main()
