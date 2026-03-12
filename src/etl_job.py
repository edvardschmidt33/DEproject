from pyspark.sql import functions as F
from config import RAW_JSON_PATH, PARQUET_PATH, TEXT_COL, TIME_COL, YEAR_COL, create_spark


def main():
    spark = create_spark("reddit-etl")

    df = spark.read.json(RAW_JSON_PATH)

    df_clean = (
        df
        .select(
            F.col(TEXT_COL).alias("text"),
            F.col(TIME_COL).alias("timestamp")
        )
        .filter(F.col("text").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.length(F.trim(F.col("text"))) > 0)
        .withColumn(
            YEAR_COL,
            F.year(F.from_unixtime(F.col("timestamp")))
        )
        .select(YEAR_COL, "text")
    )

    df_clean.write.mode("overwrite").parquet(PARQUET_PATH)

    spark.stop()


if __name__ == "__main__":
    main()