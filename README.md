# Spark ETL Pipeline: Data Processing and Analytics with Apache Spark

**Course:** DATA 430, Project 2 (individual sprint) · **Author:** Ahmed
**Stack:** Apache Airflow 3.3.2 · Apache Spark 3.5.9 (PySpark) · PostgreSQL 16 · Docker Compose

---

## 1. Project Overview

This project extends the Project 1 Airflow pipeline so that **Airflow orchestrates and Spark does the processing**.

Every pipeline run:

1. **Generates** 100,000 customers, products and orders with Faker, deliberately injecting the 7 data quality problems from the brief (duplicates, nulls, mixed case, invalid values, orphan orders, extra whitespace, mixed date formats), plus a few corrupt lines.
2. **Validates** that the raw files exist and are readable.
3. **Cleanses** each entity with a PySpark job submitted by Airflow through `spark-submit` to a Spark standalone cluster. The jobs deduplicate, handle nulls, normalise text, standardise dates to ISO 8601, remove invalid values and orphan orders, and write **Parquet**.
4. **Computes 15 analytics tables** with Spark DataFrames and Spark SQL (customer lifetime value, monthly and daily sales, category performance, revenue by customer type, and more).
5. **Loads** the clean data and analytics into **PostgreSQL** in one transaction.
6. **Validates** row counts, referential integrity and analytics consistency, then records the run in `etl_audit`.

| Result of one run | Value |
|---|---|
| Raw records read (per entity) | 105,510 |
| Clean customers / products / orders | 100,000 / 100,000 / 94,865 |
| Analytics tables | 15 |
| Full pipeline duration (8 GB laptop) | about 5–10 minutes |

---

## 2. Architecture

```
            ┌────────────────────────── Docker Compose ──────────────────────────┐
            │                                                                    │
  Browser ──┼─► Airflow API server (UI :8080)                                    │
            │   Airflow scheduler (LocalExecutor) ── runs tasks ───────────┐     │
            │   Airflow DAG processor                                      │     │
            │   postgres (Airflow metadata)                                │     │
            │                                                             │     │
            │        spark-submit (client mode, driver in the scheduler)  ▼     │
            │   spark-master (:7077, UI :8090) ──► spark-worker (UI :8091)      │
            │                                          │  executors              │
            │   ./data  (shared volume: raw → cleaned Parquet → analytics)      │
            │                                          │                         │
            │   etl-postgres (:5433)  ◄── load_to_postgres (COPY) ◄─────────────┘
            └────────────────────────────────────────────────────────────────────┘
```

**DAG:** `generate_data >> file_validation >> spark_cleanse_customers >> spark_cleanse_products >> spark_cleanse_orders >> spark_analytics >> load_to_postgres >> data_validation >> completion`

- Airflow and Spark use the **same Python (3.11) and Spark (3.5.9) versions**, which Spark requires between the driver and the executors.
- The project is tuned for a **laptop with 8 GB of RAM**: Airflow uses the LocalExecutor (no Redis or Celery worker), Spark executors use 512 MB, and the cleansing jobs run one after another.

---

## 3. Repository Structure

```
spark-etl-project/
├── dags/
│   └── etl_spark_pipeline.py        # Airflow DAG (9 tasks)
├── scripts/
│   ├── generate_data.py             # Faker: 100K records per entity with quality issues
│   ├── spark_cleanse.py             # PySpark cleansing and deduplication (one entity per run)
│   ├── spark_analytics.py           # PySpark analytics (DataFrames + Spark SQL)
│   ├── spark_utils.py               # SparkSession, logging, config, output paths
│   ├── load_to_postgres.py          # Parquet -> PostgreSQL bulk load (COPY, one transaction)
│   └── benchmark_pandas_vs_spark.py # Optional pandas vs Spark benchmark
├── sql/
│   └── schema.sql                   # Clean tables, 15 analytics tables, etl_audit, cleansing_stats
├── config/
│   └── spark_config.json            # Spark settings and data paths
├── data/                            # Generated at runtime (not committed)
│   ├── raw/                         #   messy input files
│   ├── cleaned/<run_id>/            #   Parquet output of the cleansing jobs
│   ├── analytics/<run_id>/          #   Parquet + CSV analytics tables
│   └── reports/<run_id>/            #   cleansing statistics, rejected records, summary CSV
├── docker-compose.yml               # Airflow, Spark master/worker, PostgreSQL
├── Dockerfile                       # Airflow 3.3.2 (Python 3.11) + Java 17 + PySpark
├── Dockerfile.spark                 # Spark cluster image: Python 3.11 + Java 17 + PySpark
├── requirements.txt
├── .env.example                     # template for credentials (copy to .env)
└── .gitignore
```

---

## 4. Prerequisites

- **Docker Desktop** (Windows: WSL 2 backend) with **at least 4 GB of memory** for Docker
- **Git**
- Free ports: 8080 (Airflow), 8090/8091 (Spark UIs), 4040 (Spark job UI), 5433 (PostgreSQL)

**Windows with 8 GB of RAM:** limit WSL's memory so Docker and Windows both have room. Create `C:\Users\<you>\.wslconfig` with:

```
[wsl2]
memory=4GB
swap=4GB
```

Then run `wsl --shutdown` and restart Docker Desktop.

---

## 5. Setup

All commands are run from the project folder (Windows PowerShell or the VS Code terminal).

**1. Clone the repository**
```
git clone https://github.com/<Ahmed786110-a11y>/spark-etl-project.git
cd spark-etl-project
```

**2. Create `.env` from the template** and fill in your own values
```
Copy-Item .env.example .env
```
`.env` holds the database user and password and the Airflow Fernet key. It is listed in `.gitignore` and never committed.

**3. Build the images** (first time only, 15–25 minutes)
```
docker compose build
```

**4. Initialise Airflow** (first time only), wait for `exited with code 0`
```
docker compose up airflow-init
```

**5. Start everything**
```
docker compose up -d
```
Wait 2–3 minutes, then run `docker compose ps`. The Airflow services should show `healthy`, and `etl-postgres`, `spark-master` and `spark-worker` should show `Up`.

---

## 6. Running the Pipeline

1. Open **http://localhost:8080** and log in (`airflow` / `airflow`).
2. Switch on **`etl_spark_pipeline`**, then click **▶ Trigger**. It also runs daily at 06:00 UTC.
3. Follow the progress in the **Graph** view. While a Spark task runs, the job is visible at **http://localhost:8090** (Spark master) and **http://localhost:4040** (job details).

Command-line alternative:
```
docker compose exec airflow-scheduler airflow dags trigger etl_spark_pipeline
```

### Checking the results

```
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT run_id, status, records_processed FROM etl_audit ORDER BY audit_id DESC LIMIT 3;"
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT entity, detail, count FROM cleansing_stats WHERE type = 'total' ORDER BY entity, detail;"
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT * FROM analytics_sales_monthly ORDER BY month;"
```
Press `q` if the output ends with `(END)`.

The output files are under `data/`: cleaned Parquet in `data/cleaned/<run_id>/`, analytics as readable CSV in `data/analytics/<run_id>/csv/`, and cleansing statistics and rejected records in `data/reports/<run_id>/`.

### Running a step by hand (for testing)
```
docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_data.py
docker compose exec airflow-scheduler spark-submit --driver-memory 512m /opt/airflow/scripts/spark_cleanse.py --entity customers --run-id test1
docker compose exec airflow-scheduler spark-submit --driver-memory 512m /opt/airflow/scripts/spark_analytics.py --run-id test1
docker compose exec airflow-scheduler python /opt/airflow/scripts/load_to_postgres.py --run-id test1
```
Use `NUM_RECORDS=1000` for the small 1,000-record dataset, for example `docker compose exec -e NUM_RECORDS=1000 airflow-scheduler python /opt/airflow/scripts/generate_data.py`.

### Stopping
```
docker compose stop        # stop, keep everything
docker compose down        # remove containers, keep the database volume
docker compose down -v     # remove everything, including the database
```

---

## 7. Troubleshooting

| Problem | Fix |
|---|---|
| `500 Internal Server Error ... dockerDesktopLinuxEngine` | Docker's engine isn't running. Open Docker Desktop and wait for **Engine running**; if it hangs, run `wsl --shutdown` and restart Docker Desktop. |
| Everything freezes / pages stop loading | Out of memory. Close other programs, check `.wslconfig` (section 4), restart Docker Desktop. |
| `localhost:8080` doesn't load | Airflow needs 2–3 minutes after `docker compose up -d`. Check `docker compose ps`. |
| New DAG doesn't appear | Airflow scans for new files every 5 minutes. Force it: `docker compose exec airflow-dag-processor airflow dags reserialize`. |
| Manual run stays **Queued** | Only one run is allowed at a time; another (scheduled) run is probably in progress. |
| Spark task log repeats "Initial job has not accepted any resources" | The Spark worker is busy or down. Check http://localhost:8090; kill stuck applications there. |
| Spark warning "No Partition Defined for Window operation" | Harmless here: global rankings run on small aggregated tables. |

---

## 8. Further Documentation

The **Project Report** covers the architecture, every data quality issue and how Spark resolved it (with before/after counts), all analytics results, design decisions, the Spark vs pandas comparison and lessons learned.
