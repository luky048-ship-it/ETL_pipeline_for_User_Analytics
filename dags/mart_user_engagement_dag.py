# dags/mart_user_engagement_dag.py
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

SQL_MART_USER_ENGAGEMENT = """
INSERT INTO dwh.mart_user_engagement (
    user_id, period_start, period_end, total_sessions, total_duration_seconds,
    avg_session_duration, unique_pages_visited, total_actions,
    devices_used, last_activity, _calculated_at
)
SELECT 
    user_id,
    DATE(start_time) AS period_start,
    DATE(start_time) AS period_end,
    COUNT(*) AS total_sessions,
    SUM(duration_seconds) AS total_duration_seconds,
    AVG(duration_seconds)::NUMERIC(10,2) AS avg_session_duration,
    SUM(pages_count) AS unique_pages_visited,
    SUM(actions_count) AS total_actions,
    jsonb_object_agg(device, 1) AS devices_used,
    MAX(end_time) AS last_activity,
    NOW() AS _calculated_at
FROM dwh.fact_sessions
GROUP BY user_id, DATE(start_time)
ON CONFLICT (user_id, period_start, period_end) DO UPDATE SET
    total_sessions = EXCLUDED.total_sessions,
    total_duration_seconds = EXCLUDED.total_duration_seconds,
    avg_session_duration = EXCLUDED.avg_session_duration,
    unique_pages_visited = EXCLUDED.unique_pages_visited,
    total_actions = EXCLUDED.total_actions,
    devices_used = EXCLUDED.devices_used,
    last_activity = EXCLUDED.last_activity,
    _calculated_at = EXCLUDED._calculated_at;
"""

with DAG(
    dag_id="mart_user_engagement",
    default_args=DEFAULT_ARGS,
    description="Витрина #1: Активность пользователей",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["mart", "analytics", "user-engagement"],
) as dag:

    create_mart = PostgresOperator(
        task_id="create_mart_user_engagement",
        postgres_conn_id="postgres_dwh",
        sql=SQL_MART_USER_ENGAGEMENT,
    )
