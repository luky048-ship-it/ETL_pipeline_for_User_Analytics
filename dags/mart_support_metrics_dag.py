from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SQL_MART_SUPPORT_METRICS = """
-- Убираем фильтр WHERE DATE = {{ ds }}, чтобы обработать весь массив данных из фактов
INSERT INTO dwh.mart_support_metrics (
    period_start, period_end, issue_type, total_tickets, resolved_tickets,
    avg_resolution_time_h, max_resolution_time_h, _calculated_at
)
SELECT 
    DATE(created_at) AS period_start,
    DATE(created_at) AS period_end,
    issue_type,
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) AS resolved_tickets,
    AVG(resolution_time_hours)::NUMERIC(10,2) AS avg_resolution_time_h,
    MAX(resolution_time_hours)::NUMERIC(10,2) AS max_resolution_time_h,
    NOW() AS _calculated_at
FROM dwh.fact_tickets
GROUP BY DATE(created_at), issue_type
ON CONFLICT (period_start, period_end, issue_type) DO UPDATE SET
    total_tickets = EXCLUDED.total_tickets,
    resolved_tickets = EXCLUDED.resolved_tickets,
    avg_resolution_time_h = EXCLUDED.avg_resolution_time_h,
    max_resolution_time_h = EXCLUDED.max_resolution_time_h,
    _calculated_at = EXCLUDED._calculated_at;
"""

with DAG(
    dag_id="mart_support_metrics",
    default_args=DEFAULT_ARGS,
    description="Витрина #2: Эффективность поддержки",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["mart", "analytics", "support"],
) as dag:

    create_mart = PostgresOperator(
        task_id="create_mart_support_metrics",
        postgres_conn_id="postgres_dwh",
        sql=SQL_MART_SUPPORT_METRICS,
    )
