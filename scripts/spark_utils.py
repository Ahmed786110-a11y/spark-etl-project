"""
spark_utils.py
--------------
Shared helpers for the PySpark jobs:
  - load_config()     read config/spark_config.json
  - get_spark()       create a SparkSession connected to the Spark cluster
  - get_logger()      timestamped logging to the console and a log file
  - output paths that include the pipeline run ID
  - save_json()       write statistics / reports
"""

import json
import logging
import os
import socket
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

CONFIG_PATH = Path(os.environ.get("SPARK_CONFIG_PATH", "/opt/airflow/config/spark_config.json"))

# Used if the config file can't be found (e.g. running outside Docker)
DEFAULT_CONFIG = {
    "app_name_prefix": "etl_spark",
    "default_master": "local[*]",
    "log_level": "WARN",
    "spark_conf": {"spark.sql.shuffle.partitions": "8",
                   "spark.sql.session.timeZone": "UTC"},
    "paths": {
        "raw": "/opt/airflow/data/raw",
        "cleaned": "/opt/airflow/data/cleaned",
        "analytics": "/opt/airflow/data/analytics",
        "reports": "/opt/airflow/data/reports",
        "logs": "/opt/airflow/logs/spark",
    },
}


def load_config():
    """Read spark_config.json; fall back to defaults if it is missing."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as f:
            config = json.load(f)
    else:
        config = DEFAULT_CONFIG
    # DATA_DIR lets the same scripts run outside Docker for testing
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        for key in ("raw", "cleaned", "analytics", "reports"):
            config["paths"][key] = str(Path(data_dir) / key)
    return config


def get_logger(name, config=None):
    """Logger that writes timestamped lines to the console and to logs/spark/<name>.log."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_dir = Path((config or DEFAULT_CONFIG)["paths"].get("logs", "/opt/airflow/logs/spark"))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / f"{name}.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Could not open log file in %s; logging to console only", log_dir)
    return logger


def get_spark(app_name, config):
    """
    Create a SparkSession.
    The master URL comes from the SPARK_MASTER_URL environment variable
    (set in docker-compose.yml), so the same code can also run locally.
    """
    master = os.environ.get("SPARK_MASTER_URL", config.get("default_master", "local[*]"))
    builder = (SparkSession.builder
               .appName(f"{config.get('app_name_prefix', 'etl_spark')}_{app_name}")
               .master(master))

    for key, value in config.get("spark_conf", {}).items():
        builder = builder.config(key, value)

    if master.startswith("spark://"):
        # Executors in the Spark worker container must be able to connect back
        # to this driver (running in the Airflow worker container), so advertise
        # the container's IP address rather than its random hostname.
        builder = (builder
                   .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
                   .config("spark.driver.bindAddress", "0.0.0.0"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.get("log_level", "WARN"))
    return spark


def default_run_id():
    """Run ID used when none is passed in: a timestamp like 20261103T223000."""
    return datetime.now().strftime("%Y%m%dT%H%M%S")


def run_path(config, kind, run_id, *parts):
    """Build an output path that includes the run ID, e.g. data/cleaned/<run_id>/customers."""
    return str(Path(config["paths"][kind], run_id, *parts))


def save_json(data, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
