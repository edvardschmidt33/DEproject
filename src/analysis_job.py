# src/analysis_job.py

from pyspark.sql import functions as F
from config import create_spark, PARQUET_INPUT, OUTPUT_BASE, COL_YEAR, COL_TEMPO


def main():

    spark = create_spark("analysis")

    df = spark.read.parquet(PARQUET_INPUT)

    result = (
        df.groupBy(COL_YEAR)
          .agg(
              F.count("*").alias("n_songs"),
              F.avg(COL_TEMPO).alias("avg_tempo")
          )
          .orderBy(COL_YEAR)
    )

    result.write.mode("overwrite").option("header", True).csv(
        f"{OUTPUT_BASE}/tempo_by_year"
    )

    spark.stop()


if __name__ == "__main__":
    main()