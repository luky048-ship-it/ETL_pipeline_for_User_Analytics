# dags/mongo_to_postgres_dag.py
"""
DAG: Репликация MongoDB → PostgreSQL (Staging + Fact)
Airflow 2.10.4 Compatible
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_load_staging(**kwargs):
    """Extract из MongoDB → Load в Staging таблицы"""
    pg_conn = None
    mongo_client = None

    try:
        mongo_conn = BaseHook.get_connection("mongo_default")
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        uri = f"mongodb://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}:{mongo_conn.port or 27017}/{mongo_conn.schema or 'etl_source_data'}?authSource=admin"
        mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        db = mongo_client["etl_source_data"]

        pg_conn = pg_hook.get_conn()

        with pg_conn.cursor() as cursor:
            # 1. UserSessions → stg_user_sessions
            cursor.execute("TRUNCATE TABLE dwh.stg_user_sessions CASCADE")
            docs = list(db["UserSessions"].find({}))
            for doc in docs:
                cursor.execute(
                    """
                    INSERT INTO dwh.stg_user_sessions 
                    (session_id, user_id, start_time, end_time, pages_visited, device, actions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO NOTHING
                """,
                    (
                        doc["session_id"],
                        doc["user_id"],
                        doc["start_time"],
                        doc["end_time"],
                        json.dumps(doc["pages_visited"]),
                        doc["device"],
                        json.dumps(doc["actions"]),
                    ),
                )
            logger.info(f"[OK] UserSessions: {len(docs)} записей")

            # 2. EventLogs → stg_event_logs
            cursor.execute("TRUNCATE TABLE dwh.stg_event_logs CASCADE")
            docs = list(db["EventLogs"].find({}))
            for doc in docs:
                cursor.execute(
                    """
                    INSERT INTO dwh.stg_event_logs 
                    (event_id, timestamp, event_type, details)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """,
                    (
                        doc["event_id"],
                        doc["timestamp"],
                        doc["event_type"],
                        doc["details"],
                    ),
                )
            logger.info(f"[OK] EventLogs: {len(docs)} записей")

            # 3. SupportTickets → stg_support_tickets
            cursor.execute("TRUNCATE TABLE dwh.stg_support_tickets CASCADE")
            docs = list(db["SupportTickets"].find({}))
            for doc in docs:
                cursor.execute(
                    """
                    INSERT INTO dwh.stg_support_tickets 
                    (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticket_id) DO NOTHING
                """,
                    (
                        doc["ticket_id"],
                        doc["user_id"],
                        doc["status"],
                        doc["issue_type"],
                        json.dumps(doc["messages"]),
                        doc["created_at"],
                        doc["updated_at"],
                    ),
                )
            logger.info(f"[OK] SupportTickets: {len(docs)} записей")

            # 4. ModerationQueue → stg_moderation_queue
            cursor.execute("TRUNCATE TABLE dwh.stg_moderation_queue CASCADE")
            docs = list(db["ModerationQueue"].find({}))
            for doc in docs:
                cursor.execute(
                    """
                    INSERT INTO dwh.stg_moderation_queue 
                    (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (review_id) DO NOTHING
                """,
                    (
                        doc["review_id"],
                        doc["user_id"],
                        doc["product_id"],
                        doc["review_text"],
                        doc["rating"],
                        doc["moderation_status"],
                        json.dumps(doc["flags"]),
                        doc["submitted_at"],
                    ),
                )
            logger.info(f"[OK] ModerationQueue: {len(docs)} записей")

            pg_conn.commit()

    except Exception as e:
        logger.error(f"[ERROR] ETL ошибка: {str(e)}")
        if pg_conn:
            pg_conn.rollback()
        raise
    finally:
        if mongo_client:
            mongo_client.close()
        if pg_conn:
            pg_conn.close()


def transform_staging_to_fact(**kwargs):
    """Transform: Staging → Fact Tables"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
    pg_conn = pg_hook.get_conn()

    try:
        with pg_conn.cursor() as cursor:
            # fact_sessions
            cursor.execute("""
                INSERT INTO dwh.fact_sessions (session_id, user_id, start_time, end_time, device, pages_count, actions_count)
                SELECT session_id, user_id, start_time, end_time, device,
                       jsonb_array_length(pages_visited),
                       jsonb_array_length(actions)
                FROM dwh.stg_user_sessions
                ON CONFLICT (session_id, start_time) DO NOTHING
            """)

            # fact_events
            cursor.execute("""
                INSERT INTO dwh.fact_events (event_id, timestamp, event_type, details)
                SELECT event_id, timestamp, event_type, details
                FROM dwh.stg_event_logs
                ON CONFLICT (event_id, timestamp) DO NOTHING
            """)

            # fact_tickets
            cursor.execute("""
                INSERT INTO dwh.fact_tickets (ticket_id, user_id, status, issue_type, created_at, updated_at, messages_count)
                SELECT ticket_id, user_id, status, issue_type, created_at, updated_at,
                       jsonb_array_length(messages)
                FROM dwh.stg_support_tickets
                ON CONFLICT (ticket_id) DO NOTHING
            """)

            # fact_reviews
            cursor.execute("""
                INSERT INTO dwh.fact_reviews (review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at)
                SELECT review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at
                FROM dwh.stg_moderation_queue
                ON CONFLICT (review_id) DO NOTHING
            """)

            # dim_users
            cursor.execute("""
                INSERT INTO dwh.dim_users (user_id, first_seen, last_seen, total_sessions, total_actions)
                SELECT user_id, MIN(start_time), MAX(end_time), COUNT(*),
                       SUM(jsonb_array_length(actions))
                FROM dwh.stg_user_sessions
                GROUP BY user_id
                ON CONFLICT (user_id) DO UPDATE SET
                    last_seen = EXCLUDED.last_seen,
                    total_sessions = dim_users.total_sessions + EXCLUDED.total_sessions,
                    total_actions = dim_users.total_actions + EXCLUDED.total_actions
            """)

            pg_conn.commit()
            logger.info("[OK] Transform Staging → Fact завершена")

    except Exception as e:
        logger.error(f"[ERROR] Transform ошибка: {str(e)}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


with DAG(
    dag_id="mongo_to_postgres_replication",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "replication", "mongodb"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_load_staging",
        python_callable=extract_load_staging,
    )

    transform = PythonOperator(
        task_id="transform_staging_to_fact",
        python_callable=transform_staging_to_fact,
    )

    extract_load >> transform
