"""
etl_spark_pipeline.py
---------------------
Airflow DAG for Project 2: Airflow orchestrates, Spark does the processing.

    generate_data >> file_validation >> spark_cleanse_customers >> spark_cleanse_products
      >> spark_cleanse_orders >> spark_analytics >> load_to_postgres
      >> data_validation >> completion

- Spark work is submitted with spark-submit (BashOperator) to the Spark
  standalone cluster. BashOperator fails the task automatically if spark-submit
  returns a non-zero exit code.
- Every task in one run shares a run ID (a timestamp created by file_validation),
  which is used in all output paths: data/cleaned/<run_id>/, data/analytics/<run_id>/ ...
- The cleansing jobs run one after another (not in parallel) so the whole stack
  fits on a laptop with 8 GB of RAM.
"""

import logging
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

DAG_NAME = "etl_spark_pipeline"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
SCRIPTS_DIR = Path(os.environ.get("SCRIPTS_DIR", "/opt/airflow/scripts"))
SQL_FILE = Path(os.environ.get("ETL_SQL_FILE", "/opt/airflow/sql/schema.sql"))
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
RAW_FILES = ["customers.csv", "products.json", "orders.csv"]
ENTITIES = ["customers", "products", "orders"]
KEEP_RUNS = 3          # older run folders are deleted to save disk space

# The run ID created by file_validation, available to every later task
RUN_ID = "{{ ti.xcom_pull(task_ids='file_validation') }}"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_connection():
    """PostgreSQL connection; credentials come from environment variables (.env)."""
    sys.path.insert(0, str(SCRIPTS_DIR))
    from load_to_postgres import get_connection as connect
    return connect()


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_start(context):
    start = getattr(context.get("dag_run"), "start_date", None)
    return start.astimezone(timezone.utc).replace(tzinfo=None) if start else utc_now()


def write_audit(context, status, records, error=None):
    """Record one pipeline run in etl_audit."""
    run_id = context["ti"].xcom_pull(task_ids="file_validation") or "n/a"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))   # makes sure etl_audit exists
            cur.execute(
                """INSERT INTO etl_audit (run_id, dag_name, start_time, end_time, status,
                                          records_processed, error_message)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (run_id, DAG_NAME, run_start(context), utc_now(), status, records,
                 (str(error)[:1000] if error else None)))
        conn.commit()
    finally:
        conn.close()


def on_failure(context):
    """Any task failure (after retries): log it and record FAILED in etl_audit."""
    task_id = context["task_instance"].task_id
    error = context.get("exception")
    log.error("Pipeline FAILED at task '%s': %s", task_id, error)
    try:
        write_audit(context, "FAILED", 0, f"{task_id}: {error}")
    except Exception as e:   # never let the callback itself crash
        log.error("Could not write FAILED row to etl_audit: %s", e)


def spark_submit(script, args):
    """Build a spark-submit command for one PySpark script."""
    return (f"spark-submit --master {SPARK_MASTER} --deploy-mode client "
            f"--driver-memory 512m "
            f"{SCRIPTS_DIR / script} {args}")


# ---------------------------------------------------------------------------
# Python task functions
# ---------------------------------------------------------------------------
def file_validation(**context):
    """Check the raw files exist, are not empty and can be read; create the run ID."""
    problems = []
    for name in RAW_FILES:
        path = DATA_DIR / "raw" / name
        try:
            if not path.exists():
                problems.append(f"missing: {name}")
            elif path.stat().st_size == 0:
                problems.append(f"empty: {name}")
            else:
                with open(path, encoding="utf-8") as f:
                    first_line = f.readline()
                lines = sum(1 for _ in open(path, encoding="utf-8"))
                log.info("OK  %-15s %10d bytes  %8d lines  first line: %s",
                         name, path.stat().st_size, lines, first_line.strip()[:80])
        except OSError as e:
            problems.append(f"unreadable: {name} ({e})")
    if problems:
        raise FileNotFoundError("File validation failed: " + "; ".join(problems))

    run_id = utc_now().strftime("%Y%m%dT%H%M%S")
    log.info("All %d raw files OK. Run ID for this pipeline run: %s", len(RAW_FILES), run_id)
    return run_id      # pushed to XCom; later tasks use it in their output paths


def load_to_postgres(**context):
    run_id = context["ti"].xcom_pull(task_ids="file_validation")
    sys.path.insert(0, str(SCRIPTS_DIR))
    from load_to_postgres import load_run
    counts = load_run(run_id)
    return sum(counts[e] for e in ENTITIES)


def data_validation(**context):
    """Verify row counts, referential integrity and the analytics output."""
    import pyarrow.parquet as pq

    run_id = context["ti"].xcom_pull(task_ids="file_validation")
    errors = []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            def scalar(sql):
                cur.execute(sql)
                return cur.fetchone()[0]

            # 1. Row counts: database must match the Spark Parquet output
            for entity in ENTITIES:
                parquet_rows = sum(pq.read_metadata(f).num_rows
                                   for f in (DATA_DIR / "cleaned" / run_id / entity).glob("*.parquet"))
                db_rows = scalar(f"SELECT COUNT(*) FROM {entity}")
                log.info("%-10s parquet %7d   database %7d", entity, parquet_rows, db_rows)
                if parquet_rows != db_rows:
                    errors.append(f"{entity}: {db_rows} rows in DB, {parquet_rows} in Parquet")

            # 2. Referential integrity
            orphans = scalar("""SELECT COUNT(*) FROM orders o LEFT JOIN customers c
                                ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL""")
            log.info("orders without a valid customer: %d", orphans)
            if orphans:
                errors.append(f"{orphans} orphan orders")
            unknown_products = scalar("""SELECT COUNT(*) FROM orders o LEFT JOIN products p
                                         ON o.product_id = p.product_id WHERE p.product_id IS NULL""")
            log.info("orders with an unknown product: %d", unknown_products)
            if unknown_products:
                errors.append(f"{unknown_products} orders reference unknown products")

            # 3. Analytics output
            cur.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_name LIKE 'analytics_%' ORDER BY table_name""")
            tables = [r[0] for r in cur.fetchall()]
            for table in tables:
                n = scalar(f"SELECT COUNT(*) FROM {table}")
                log.info("%-45s %7d rows", table, n)
                if n == 0:
                    errors.append(f"{table} is empty")
            if len(tables) < 15:
                errors.append(f"only {len(tables)} analytics tables found (expected 15)")

            # 4. Consistency: analytics must agree with the clean data
            kpi_orders = scalar("SELECT total_orders FROM analytics_order_kpis")
            orders = scalar("SELECT COUNT(*) FROM orders")
            monthly = scalar("SELECT SUM(revenue) FROM analytics_sales_monthly")
            net = scalar("SELECT SUM(total_amount - discount_amount) FROM orders")
            log.info("order count  kpi %d / orders table %d", kpi_orders, orders)
            log.info("net revenue  monthly sum %s / orders table %s", monthly, net)
            if kpi_orders != orders:
                errors.append("analytics_order_kpis.total_orders does not match orders table")
            if abs(float(monthly) - float(net)) > 0.01:
                errors.append("monthly sales do not add up to total net revenue")

            total = scalar("SELECT (SELECT COUNT(*) FROM customers) + (SELECT COUNT(*) FROM products)"
                           " + (SELECT COUNT(*) FROM orders)")
    finally:
        conn.close()

    if errors:
        raise ValueError("Data validation failed: " + "; ".join(errors))
    log.info("Data validation passed (%d clean records)", total)
    return total


def completion(**context):
    """Record SUCCESS in etl_audit and tidy up old run folders."""
    run_id = context["ti"].xcom_pull(task_ids="file_validation")
    total = context["ti"].xcom_pull(task_ids="data_validation")
    write_audit(context, "SUCCESS", total)

    # Keep only the newest KEEP_RUNS run folders of each output type
    for kind in ("cleaned", "analytics", "reports"):
        folder = DATA_DIR / kind
        if not folder.exists():
            continue
        runs = sorted((p for p in folder.iterdir() if p.is_dir()),
                      key=lambda p: p.stat().st_mtime, reverse=True)
        for old in runs[KEEP_RUNS:]:
            shutil.rmtree(old, ignore_errors=True)
            log.info("Removed old output folder %s", old)

    log.info("Pipeline run %s completed successfully: %d clean records loaded", run_id, total)
    log.info("Outputs: %s/cleaned/%s, %s/analytics/%s, %s/reports/%s",
             DATA_DIR, run_id, DATA_DIR, run_id, DATA_DIR, run_id)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "ahmed",
    "retries": 1,                          # retry a failed task once
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure,     # FAILED row in etl_audit
}

with DAG(
    dag_id=DAG_NAME,
    description="Faker data -> Spark cleansing & analytics -> PostgreSQL",
    schedule="0 6 * * *",                  # daily at 06:00 UTC
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                     # one run at a time (shared data folders)
    default_args=default_args,
    tags=["spark", "etl", "data430"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"python {SCRIPTS_DIR / 'generate_data.py'}",
    )

    t_file_validation = PythonOperator(task_id="file_validation", python_callable=file_validation)

    cleanse = {
        entity: BashOperator(
            task_id=f"spark_cleanse_{entity}",
            bash_command=spark_submit("spark_cleanse.py", f"--entity {entity} --run-id {RUN_ID}"),
        )
        for entity in ENTITIES
    }

    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=spark_submit("spark_analytics.py", f"--run-id {RUN_ID}"),
    )

    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t_validation = PythonOperator(task_id="data_validation", python_callable=data_validation)
    t_completion = PythonOperator(task_id="completion", python_callable=completion)

    (
        generate_data
        >> t_file_validation
        >> cleanse["customers"]
        >> cleanse["products"]
        >> cleanse["orders"]          # needs cleaned customers (orphan check)
        >> spark_analytics
        >> t_load
        >> t_validation
        >> t_completion
    )
