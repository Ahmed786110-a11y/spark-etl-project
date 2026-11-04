"""
load_to_postgres.py
-------------------
Loads the Spark output (cleaned Parquet + analytics Parquet) into PostgreSQL.
Used by the Airflow task `load_to_postgres`; can also be run by hand:

    python /opt/airflow/scripts/load_to_postgres.py --run-id 20261103T223000

How it works:
  1. Runs sql/schema.sql (CREATE TABLE IF NOT EXISTS - safe to repeat)
  2. In ONE transaction:
       - empties all data and analytics tables (full refresh)
       - bulk-loads every Parquet dataset with PostgreSQL COPY
         (customers before orders, because of the foreign key)
       - stores the cleansing before/after counts in cleansing_stats
  3. Commits - or rolls everything back if any step fails, so the database
     is never left half-loaded.
"""

import argparse
import csv
import io
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
SQL_FILE = Path(os.environ.get("ETL_SQL_FILE", "/opt/airflow/sql/schema.sql"))

# (PostgreSQL table, Parquet folder) in load order
CLEAN_TABLES = [
    ("customers", "cleaned/{run}/customers"),     # parent table first...
    ("products", "cleaned/{run}/products"),
    ("orders", "cleaned/{run}/orders"),           # ...then the table that references it
]
ANALYTICS = [
    "customer_metrics", "customer_type_distribution", "top_customers",
    "product_metrics", "top_products", "category_performance", "inventory_status",
    "sales_monthly", "sales_daily", "order_status_breakdown",
    "payment_method_distribution", "order_kpis",
    "revenue_by_customer_type", "top_categories_by_customer_type", "city_revenue",
]
ANALYTICS_TABLES = [(f"analytics_{name}", f"analytics/{{run}}/{name}") for name in ANALYTICS]

log = logging.getLogger("load_to_postgres")


def get_connection():
    """Connect using credentials from environment variables (.env via docker-compose)."""
    try:
        return psycopg2.connect(
            host=os.environ.get("ETL_DB_HOST", "etl-postgres"),
            port=int(os.environ.get("ETL_DB_PORT", "5432")),
            dbname=os.environ["ETL_DB_NAME"],
            user=os.environ["ETL_DB_USER"],
            password=os.environ["ETL_DB_PASSWORD"],
            connect_timeout=10,
        )
    except KeyError as e:
        raise RuntimeError(f"Missing environment variable {e} - check .env and docker-compose.yml") from e
    except psycopg2.OperationalError as e:
        log.error("Could not connect to PostgreSQL: %s", e)
        raise


def copy_parquet(cur, table, folder):
    """Bulk-load one Parquet folder into a table with COPY (much faster than INSERTs)."""
    if not folder.exists():
        raise FileNotFoundError(f"Spark output not found: {folder}")
    data = pq.read_table(folder)
    # integer_object_nulls keeps integers as integers even when some are NULL
    df = data.to_pandas(integer_object_nulls=True, date_as_object=True)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)
    columns = ", ".join(df.columns)
    cur.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    log.info("  loaded %-45s %7d rows", table, len(df))
    return len(df)


def load_cleansing_stats(cur, run_id):
    """Store the before/after cleansing counts (written by spark_analytics.py)."""
    path = DATA_DIR / "reports" / run_id / "cleansing_summary.csv"
    if not path.exists():
        log.warning("  no cleansing summary at %s - skipped", path)
        return 0
    with open(path, encoding="utf-8") as f:
        rows = [(run_id, r["entity"], r["type"], r["detail"], int(r["count"]))
                for r in csv.DictReader(f)]
    cur.execute("DELETE FROM cleansing_stats WHERE run_id = %s", (run_id,))
    cur.executemany("INSERT INTO cleansing_stats (run_id, entity, type, detail, count) "
                    "VALUES (%s, %s, %s, %s, %s)", rows)
    log.info("  loaded %-45s %7d rows", "cleansing_stats", len(rows))
    return len(rows)


def load_run(run_id):
    """Load one pipeline run. Returns {table: rows loaded}."""
    t0 = time.time()
    log.info("=== Loading run %s into PostgreSQL ===", run_id)
    tables = [(t, DATA_DIR / p.format(run=run_id)) for t, p in CLEAN_TABLES + ANALYTICS_TABLES]

    conn = get_connection()
    counts = {}
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))
            log.info("  schema ready (%s)", SQL_FILE.name)

            # Full refresh: one TRUNCATE for all tables keeps the foreign key happy
            cur.execute("TRUNCATE TABLE " + ", ".join(t for t, _ in tables))
            log.info("  emptied %d tables", len(tables))

            for table, folder in tables:
                counts[table] = copy_parquet(cur, table, folder)
            counts["cleansing_stats"] = load_cleansing_stats(cur, run_id)
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        log.error("Constraint violation (primary key / foreign key / check) - "
                  "transaction rolled back: %s", e)
        raise
    except Exception as e:
        conn.rollback()
        log.error("Load failed - transaction rolled back: %s", e)
        raise
    finally:
        conn.close()

    log.info("Loaded %d tables (%d rows) in %.1fs",
             len(counts), sum(counts.values()), time.time() - t0)
    return counts


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser(description="Load Spark output into PostgreSQL")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        load_run(args.run_id)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
