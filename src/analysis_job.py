from pyspark.sql import functions as F
from config import RAW_JSON_PATH, OUTPUT_PATH, WORDS, YEAR_COL, create_spark
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=RAW_JSON_PATH)
    parser.add_argument("--output", default=OUTPUT_PATH)
    parser.add_argument("--fraction", type=float, default=1.0)

    args = parser.parse_args()

    spark = create_spark("reddit-analysis")

    df = spark.read.json(args.input)

    if args.fraction < 1.0:
        df = df.sample(fraction=args.fraction, seed=42)

    df_words = (
        df
        .withColumn("text_lower", F.lower(F.col("body")))
        .withColumn(YEAR_COL, F.year(F.from_unixtime(F.col("created_utc"))))
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

    result.write.mode("overwrite").option("header", True).csv(args.output)


    spark.stop()


if __name__ == "__main__":
    main()