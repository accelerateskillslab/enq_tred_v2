from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)


default_args = {
    "owner": "ayush",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

materialized_view_sql = """
DROP MATERIALIZED VIEW IF EXISTS public.city_wise_revenue;

CREATE MATERIALIZED VIEW public.city_wise_revenue AS
SELECT
    city,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_revenue,
    ROUND(AVG(order_amount), 2) AS average_order_value
FROM public.orders_mwaa
GROUP BY city;
"""

with DAG(
    dag_id="city_revenue_materialized_view",
    description=(
        "Create the city-wise revenue materialized view "
        "in Aurora PostgreSQL"
    ),
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["mwaa", "aurora", "materialized-view"],
) as dag:

    create_city_revenue_view = SQLExecuteQueryOperator(
        task_id="create_city_revenue_materialized_view",
        conn_id="aurora_practice",
        sql=materialized_view_sql,
        autocommit=True,
        split_statements=True,
    )