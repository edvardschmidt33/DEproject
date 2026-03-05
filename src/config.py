# src/config.py

import os
from pyspark.sql import SparkSession


# -----------------------
# Project identification
# -----------------------

PROJECT_TAG = "group42"


# -----------------------
# Spark cluster settings
# -----------------------

SPARK_MASTER_URL = os.getenv(
    "SPARK_MASTER_URL",
    "spark://<SPARK_MASTERNODE_IP>:7077"
)


# -----------------------
# Dataset paths
# -----------------------

PARQUET_INPUT = os.getenv(
    "MSD_PARQUET_INPUT",
    "data/parquet/msd.parquet"
)

OUTPUT_BASE = os.getenv(
    "MSD_OUTPUT_BASE",
    "data/results"
)


# -----------------------
# Dataset column names
# -----------------------

COL_YEAR = "year"
COL_TEMPO = "tempo"


# -----------------------
# Spark session creator
# -----------------------

def create_spark(app_name_suffix: str):

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER_URL)
        .appName(f"{PROJECT_TAG}-{app_name_suffix}")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "false")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.cores.max", "4")
        .getOrCreate()
    )

    return spark