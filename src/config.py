from pyspark.sql import SparkSession

# =========================
#   PATHS
# =========================

RAW_JSON_PATH = "/data/reddit/corpus-webis-tldr-17.json"
PARQUET_PATH = "/data/reddit/parquet/reddit_cleaned"
OUTPUT_PATH = "/data/reddit/output/wordcount_by_year"

# =========================
#   COLUMN NAMES
# =========================

TEXT_COL = "body"
TIME_COL = "created_utc"
YEAR_COL = "year"

# =========================
#   WORDS TO ANALYZE
# =========================

WORDS = ["WORD1", "WORD2", "WORD3", "WORD4", "WORD5"]

# =========================
#   SPARK SESSION
# =========================

def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )