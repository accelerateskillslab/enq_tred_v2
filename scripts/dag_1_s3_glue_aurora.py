from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


default_args = {
    "owner": "ayush",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="s3_glue_aurora_orders",
    description=(
        "Run the S3-to-Aurora Glue job and trigger the "
        "city revenue materialized-view DAG"
    ),
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["mwaa", "glue", "aurora"],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id="run_orders_glue_job",
        job_name="practice-s3-orders-to-aurora",
        region_name="us-east-1",
        wait_for_completion=True,
        verbose=False,
    )

    trigger_revenue_dag = TriggerDagRunOperator(
        task_id="trigger_city_revenue_dag",
        trigger_dag_id="city_revenue_materialized_view",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    run_glue_job >> trigger_revenue_dag