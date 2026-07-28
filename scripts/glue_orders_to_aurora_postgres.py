"""AWS Glue ETL job: Glue Data Catalog orders to Aurora PostgreSQL.

Optional Glue job parameters:
    --SOURCE_DATABASE <catalog database>  (default: practice)
    --SOURCE_TABLE <catalog table>        (default: orders)
    --TARGET_DATABASE <PostgreSQL database> (default: practice)
    --TARGET_TABLE <schema.table>         (default: public.orders)

The attached Glue JDBC connection named "Aurora connection" supplies the
PostgreSQL JDBC URL and credentials. Do not place database credentials in this
script or in job arguments.
"""

import re
import sys
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def optional_job_arg(name: str, default: str) -> str:
    """Read an optional Glue argument in --NAME value or --NAME=value form."""
    flag = f"--{name}"

    for index, value in enumerate(sys.argv):
        if value == flag and index + 1 < len(sys.argv):
            return sys.argv[index + 1]
        if value.startswith(f"{flag}="):
            return value.split("=", 1)[1]

    return default


def create_target_table_if_missing(
    glue_context: GlueContext,
    connection_name: str,
    target_database: str,
    target_table: str,
) -> None:
    """Create the Aurora PostgreSQL target table using the Glue connection."""
    jdbc_config = glue_context.extract_jdbc_conf(connection_name=connection_name)
    jdbc_url = jdbc_config.get("fullUrl")

    if not jdbc_url or not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(
            f'Glue connection "{connection_name}" must contain a complete '
            "PostgreSQL JDBC URL."
        )

    parsed_url = urlparse(jdbc_url.removeprefix("jdbc:"))
    if not parsed_url.hostname:
        raise ValueError(
            f'Glue connection "{connection_name}" has an invalid PostgreSQL '
            "JDBC URL. The URL must include a host."
        )

    if "." in target_table:
        schema_name, table_name = target_table.split(".", 1)
    else:
        schema_name, table_name = "public", target_table

    create_table_statement = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {}.{} (
            order_id BIGINT PRIMARY KEY,
            customer_id VARCHAR(32) NOT NULL,
            order_date DATE NOT NULL,
            product_category VARCHAR(100),
            city VARCHAR(100),
            order_amount NUMERIC(12, 2) NOT NULL,
            order_status VARCHAR(32),
            payment_status VARCHAR(32),
            updated_at TIMESTAMP NOT NULL
        )
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )

    # Credentials remain inside the Glue runtime and are never logged.
    with psycopg2.connect(
        host=parsed_url.hostname,
        port=parsed_url.port or 5432,
        dbname=target_database,
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        connect_timeout=30,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema_name)
                )
            )
            cursor.execute(create_table_statement)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

connection_name = "Aurora connection"
source_database = optional_job_arg("SOURCE_DATABASE", "practice")
source_table = optional_job_arg("SOURCE_TABLE", "orders")
target_database = optional_job_arg("TARGET_DATABASE", "practice")
target_table = optional_job_arg("TARGET_TABLE", "public.orders")

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_database):
    raise ValueError(
        "TARGET_DATABASE must be a valid PostgreSQL database identifier."
    )

if not re.fullmatch(
    r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?",
    target_table,
):
    raise ValueError(
        "TARGET_TABLE must be a table name or schema-qualified table name, "
        "for example public.orders."
    )

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source = glue_context.create_dynamic_frame.from_catalog(
    database=source_database,
    table_name=source_table,
    transformation_ctx="source_orders_catalog",
)

source_df = source.toDF()
expected_columns = {
    "order_id",
    "customer_id",
    "order_date",
    "product_category",
    "city",
    "order_amount",
    "order_status",
    "payment_status",
    "updated_at",
}
missing_columns = sorted(expected_columns.difference(source_df.columns))

if missing_columns:
    raise ValueError(
        "The Glue Catalog source is missing required columns: "
        + ", ".join(missing_columns)
    )

orders_df = source_df.select(
    F.col("order_id").cast("long").alias("order_id"),
    F.col("customer_id").cast("string").alias("customer_id"),
    F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
    F.col("product_category").cast("string").alias("product_category"),
    F.col("city").cast("string").alias("city"),
    F.col("order_amount").cast(DecimalType(12, 2)).alias("order_amount"),
    F.col("order_status").cast("string").alias("order_status"),
    F.col("payment_status").cast("string").alias("payment_status"),
    F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
).dropDuplicates(["order_id"])

invalid_row_exists = (
    orders_df.filter(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_date").isNull()
        | F.col("order_amount").isNull()
        | F.col("updated_at").isNull()
    )
    .limit(1)
    .count()
    > 0
)

if invalid_row_exists:
    raise ValueError(
        "At least one source record has an invalid required value, date, "
        "timestamp, or order amount. No rows were written."
    )

row_count = orders_df.count()
if row_count == 0:
    print("No source rows were available. Nothing was written.")
    job.commit()
    sys.exit(0)

create_target_table_if_missing(
    glue_context=glue_context,
    connection_name=connection_name,
    target_database=target_database,
    target_table=target_table,
)

orders = DynamicFrame.fromDF(
    orders_df,
    glue_context,
    "orders_for_aurora",
)

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=orders,
    catalog_connection=connection_name,
    connection_options={
        "database": target_database,
        "dbtable": target_table,
    },
    transformation_ctx="aurora_postgres_orders_sink",
)

print(
    f"Wrote {row_count} rows from {source_database}.{source_table} "
    f"to Aurora PostgreSQL table {target_table}."
)
job.commit()
