from pyspark.sql import functions as F
from config import PARQUET_PATH, OUTPUT_PATH, WORDS, YEAR_COL, create_spark


def main():
    spark = create_spark("reddit-analysis")

    df = spark.read.parquet(PARQUET_PATH)

    df_words = (
        df
        .withColumn("text_lower", F.lower(F.col("text")))
        .withColumn(
            "word",
            F.explode(
                F.split(F.col("text_lower"), r"[^a-zA-Z0-9]+")
            )
        )
        .filter(F.col("word") != "")
    )

    result = (
        df_words
        .filter(F.col("word").isin(WORDS))
        .groupBy(YEAR_COL, "word")
        .count()
        .orderBy(YEAR_COL, "word")
    )

    result.write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()