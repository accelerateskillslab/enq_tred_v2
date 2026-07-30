"""EMR Serverless Spark job: Aurora PostgreSQL orders to partitioned Parquet.

The PostgreSQL JDBC driver must be supplied when the job is submitted,
for example using the Spark submit --jars parameter.
"""

import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_JDBC_URL = (
    "jdbc:postgresql://"
    "practice-v2.cluster-csjw8ueqyu2c.us-east-1.rds.amazonaws.com:5432/"
    "practice?sslmode=require"
)

DEFAULT_SOURCE_TABLE = "public.orders"

DEFAULT_OUTPUT_PATH = (
    "s3://glue-test-bucket-316422224399/curated/orders_kolkata/"
)

JDBC_USERNAME = "postgres"
JDBC_PASSWORD = "postgres"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read Kolkata orders from Aurora PostgreSQL and write Parquet "
            "partitioned by update month."
        )
    )

    parser.add_argument(
        "--jdbc-url",
        default=DEFAULT_JDBC_URL,
        help="Aurora PostgreSQL JDBC URL.",
    )

    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help="Schema-qualified PostgreSQL source table.",
    )

    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help="S3 prefix for the partitioned Parquet dataset.",
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError("--jdbc-url must be a PostgreSQL JDBC URL.")

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*",
        args.source_table,
    ):
        raise ValueError("--source-table must use the schema.table format.")

    if not args.output_path.startswith("s3://"):
        raise ValueError("--output-path must be an S3 URI.")


def main() -> None:
    args = parse_args()
    validate_args(args)

    spark = (
        SparkSession.builder
        .appName("aurora-orders-kolkata-to-parquet")
        .getOrCreate()
    )

    try:
        orders = (
            spark.read.format("jdbc")
            .option("url", args.jdbc_url)
            .option("dbtable", args.source_table)
            .option("driver", "org.postgresql.Driver")
            .option("user", JDBC_USERNAME)
            .option("password", JDBC_PASSWORD)
            .option("fetchsize", "1000")
            .load()
        )

        required_columns = {"city", "updated_at"}
        missing_columns = sorted(
            required_columns.difference(orders.columns)
        )

        if missing_columns:
            raise RuntimeError(
                "The orders table is missing required columns: "
                + ", ".join(missing_columns)
            )

        kolkata_orders = (
            orders
            .filter(F.col("city") == F.lit("Kolkata"))
            .withColumn("month", F.month(F.col("updated_at")))
            .cache()
        )

        invalid_timestamp_exists = (
            kolkata_orders
            .filter(F.col("month").isNull())
            .limit(1)
            .count()
        )

        if invalid_timestamp_exists:
            raise RuntimeError(
                "At least one Kolkata order has a null or invalid updated_at."
            )

        row_count = kolkata_orders.count()

        (
            kolkata_orders.write
            .mode("overwrite")
            .partitionBy("month")
            .parquet(args.output_path)
        )

        print(
            f"Wrote {row_count} Kolkata order records as Parquet to "
            f"{args.output_path}, partitioned by month."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()