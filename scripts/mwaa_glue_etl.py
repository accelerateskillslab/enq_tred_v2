import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_path = "s3://glue-test-bucket-316422224399/raw/orders/"

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(source_path)
)

transformed_orders = (
    orders
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"), "yyyy-MM-dd"),
    )
    .withColumn(
        "order_amount",
        F.col("order_amount").cast(DecimalType(12, 2)),
    )
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.col("updated_at"),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city"))),
    )
    .withColumn(
        "updated_month",
        F.month(F.col("updated_at")),
    )
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "product_category",
        "city",
        "order_amount",
        "order_status",
        "payment_status",
        "updated_at",
        "updated_month",
    )
)

target_frame = DynamicFrame.fromDF(
    transformed_orders,
    glue_context,
    "target_frame",
)

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=target_frame,
    catalog_connection="Aurora connection",
    connection_options={
        "dbtable": "public.orders_mwaa",
        "database": "practice",
    },
    transformation_ctx="write_orders_to_aurora",
)

job.commit()