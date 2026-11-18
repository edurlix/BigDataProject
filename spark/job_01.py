"""Cobertura por indicador y década con RDD, DataFrame, SQL y SQLite."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
INDICATORS = ROOT / "data" / "clean" / "indicators"
SQLITE = ROOT / "data" / "serving" / "observatorio.sqlite"
OUTPUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
EVENT_LOG = ROOT / "logs" / "spark-events"


def _decade(year: int) -> int:
    return (int(year) // 10) * 10


def _normalize(rows) -> dict[tuple[str, int], tuple[int, int, int, int]]:
    return {
        (str(row["indicator_id"]), int(row["decade"])): (
            int(row["n"]),
            int(row["countries"]),
            int(row["first_year"]),
            int(row["last_year"]),
        )
        for row in rows
    }


def coverage_rdd(df: DataFrame):
    """MapReduce explícito: clave, acumulador y reduceByKey."""
    return (
        df.select("indicator_id", "iso3", "year")
        .rdd.map(
            lambda row: (
                (row.indicator_id, _decade(row.year)),
                (1, {row.iso3}, int(row.year), int(row.year)),
            )
        )
        .reduceByKey(
            lambda left, right: (
                left[0] + right[0],
                left[1] | right[1],
                min(left[2], right[2]),
                max(left[3], right[3]),
            )
        )
        .map(
            lambda item: {
                "indicator_id": item[0][0],
                "decade": item[0][1],
                "n": item[1][0],
                "countries": len(item[1][1]),
                "first_year": item[1][2],
                "last_year": item[1][3],
            }
        )
        .collect()
    )


def coverage_dataframe(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("decade", F.floor(F.col("year") / 10) * 10)
        .groupBy("indicator_id", "decade")
        .agg(
            F.count("*").alias("n"),
            F.countDistinct("iso3").alias("countries"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy("indicator_id", "decade")
    )


def coverage_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("indicators_non_null")
    return spark.sql(
        """
        SELECT indicator_id,
               FLOOR(year / 10) * 10 AS decade,
               COUNT(*) AS n,
               COUNT(DISTINCT iso3) AS countries,
               MIN(year) AS first_year,
               MAX(year) AS last_year
        FROM indicators_non_null
        GROUP BY indicator_id, FLOOR(year / 10)
        ORDER BY indicator_id, decade
        """
    )


def coverage_sqlite() -> dict[tuple[str, int], tuple[int, int, int, int]]:
    query = """
        SELECT indicator_id,
               CAST(CAST(year AS INTEGER) / 10 AS INTEGER) * 10 AS decade,
               COUNT(*) AS n,
               COUNT(DISTINCT iso3) AS countries,
               MIN(CAST(year AS INTEGER)) AS first_year,
               MAX(CAST(year AS INTEGER)) AS last_year
        FROM indicators
        WHERE value IS NOT NULL AND year IS NOT NULL
        GROUP BY indicator_id, CAST(CAST(year AS INTEGER) / 10 AS INTEGER)
    """
    with sqlite3.connect(SQLITE) as connection:
        columns = (
            "indicator_id", "decade", "n", "countries", "first_year", "last_year"
        )
        return _normalize(dict(zip(columns, row)) for row in connection.execute(query))


def main() -> int:
    if not INDICATORS.is_dir():
        raise SystemExit(
            "Falta data/clean/indicators/. Ejecuta primero python -m src.pipeline."
        )
    if not SQLITE.is_file():
        raise SystemExit(
            "Falta data/serving/observatorio.sqlite. Ejecuta primero el pipeline."
        )

    EVENT_LOG.mkdir(parents=True, exist_ok=True)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("observatorio-cobertura-decada")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG.resolve().as_uri())
        .config("spark.eventLog.rolling.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        indicators = spark.read.parquet(str(INDICATORS))
        print("Particiones de entrada:", indicators.rdd.getNumPartitions())
        valid = (
            indicators.filter(F.col("value").isNotNull())
            .filter(F.col("year").isNotNull())
            .withColumn("year", F.col("year").cast("int"))
        )

        rdd_result = _normalize(coverage_rdd(valid))
        dataframe_result = coverage_dataframe(valid)
        sql_result = coverage_sql(spark, valid)

        print("Plan físico de la versión DataFrame:")
        dataframe_result.explain(mode="formatted")

        normalized_df = _normalize(row.asDict() for row in dataframe_result.collect())
        normalized_sql = _normalize(row.asDict() for row in sql_result.collect())
        normalized_sqlite = coverage_sqlite()

        if not (
            rdd_result == normalized_df
            and normalized_df == normalized_sql
            and normalized_sql == normalized_sqlite
        ):
            print("RDD:", rdd_result, file=sys.stderr)
            print("DataFrame:", normalized_df, file=sys.stderr)
            print("Spark SQL:", normalized_sql, file=sys.stderr)
            print("SQLite:", normalized_sqlite, file=sys.stderr)
            raise SystemExit("Las cuatro agregaciones difieren")

        dataframe_result.write.mode("overwrite").parquet(str(OUTPUT))
        print("Cobertura coincidente escrita en:", OUTPUT)
        print("Event log:", EVENT_LOG)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
