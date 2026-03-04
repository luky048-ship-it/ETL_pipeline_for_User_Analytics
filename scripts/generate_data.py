# scripts/generate_data.py
"""
Генератор тестовых данных для MongoDB
Версия: 2.2 (Согласован с init_dwh.sql и DAGs)
"""

import random
import uuid
from datetime import datetime, timedelta

from faker import Faker
from pymongo import MongoClient

fake = Faker("ru_RU")
Faker.seed(42)

# --- КОНФИГУРАЦИЯ ---
MONGO_URI = "mongodb://admin:password@localhost:27017/etl_source_data?authSource=admin"
DB_NAME = "etl_source_data"
USER_POOL_SIZE = 500
RECORDS_PER_COLLECTION = 1000


def get_mongo_client():
    """Подключение к MongoDB"""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def generate_user_sessions(db, count=1000):
    """UserSessions → stg_user_sessions → fact_sessions"""
    collection = db["UserSessions"]
    data = []
    base_date = datetime(2024, 1, 1)
    devices = ["mobile", "desktop", "tablet"]
    pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/support"]
    actions_list = [
        "login",
        "view_product",
        "add_to_cart",
        "checkout",
        "logout",
        "search",
    ]

    for _ in range(count):
        start_time = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        duration_seconds = random.randint(300, 7200)
        end_time = start_time + timedelta(seconds=duration_seconds)

        doc = {
            "session_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, USER_POOL_SIZE):04d}",
            "start_time": start_time,
            "end_time": end_time,
            "pages_visited": random.sample(pages, random.randint(1, 5)),
            "device": random.choice(devices),
            "actions": random.sample(actions_list, random.randint(1, 5)),
        }
        data.append(doc)

    collection.delete_many({})
    collection.insert_many(data)
    print(f"[OK] UserSessions: {len(data)} записей")
    return len(data)


def generate_event_logs(db, count=2000):
    """EventLogs → stg_event_logs → fact_events"""
    collection = db["EventLogs"]
    data = []
    base_date = datetime(2024, 1, 1)
    event_types = ["click", "view", "scroll", "submit", "error"]

    for _ in range(count):
        timestamp = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        doc = {
            "event_id": str(uuid.uuid4()),
            "timestamp": timestamp,
            "event_type": random.choice(event_types),
            "details": f"/products/{random.randint(1, 100)}",
        }
        data.append(doc)

    collection.delete_many({})
    collection.insert_many(data)
    print(f"[OK] EventLogs: {len(data)} записей")
    return len(data)


def generate_support_tickets(db, count=500):
    """SupportTickets → stg_support_tickets → fact_tickets"""
    collection = db["SupportTickets"]
    data = []
    base_date = datetime(2024, 1, 1)
    statuses = ["open", "in_progress", "resolved", "closed"]
    issue_types = ["payment", "technical", "account", "shipping", "refund"]

    for _ in range(count):
        created_at = base_date + timedelta(
            days=random.randint(0, 365), hours=random.randint(0, 23)
        )
        resolution_hours = random.randint(1, 72)
        updated_at = created_at + timedelta(hours=resolution_hours)

        messages = [
            {
                "sender": "user",
                "message": fake.sentence(),
                "timestamp": created_at.isoformat(),
            },
            {
                "sender": "support",
                "message": fake.sentence(),
                "timestamp": updated_at.isoformat(),
            },
        ]

        doc = {
            "ticket_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, USER_POOL_SIZE):04d}",
            "status": random.choice(statuses),
            "issue_type": random.choice(issue_types),
            "messages": messages,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        data.append(doc)

    collection.delete_many({})
    collection.insert_many(data)
    print(f"[OK] SupportTickets: {len(data)} записей")
    return len(data)


def generate_user_recommendations(db, count=500):
    """UserRecommendations → stg_user_recommendations"""
    collection = db["UserRecommendations"]
    data = []
    base_date = datetime(2024, 1, 1)

    for i in range(count):
        last_updated = base_date + timedelta(
            days=random.randint(0, 365), hours=random.randint(0, 23)
        )

        doc = {
            "user_id": f"user_{i+1:04d}",
            "recommended_products": [
                f"prod_{random.randint(1, 1000):04d}"
                for _ in range(random.randint(1, 5))
            ],
            "last_updated": last_updated,
        }
        data.append(doc)

    collection.delete_many({})
    collection.insert_many(data)
    print(f"[OK] UserRecommendations: {len(data)} записей")
    return len(data)


def generate_moderation_queue(db, count=500):
    """ModerationQueue → stg_moderation_queue → fact_reviews"""
    collection = db["ModerationQueue"]
    data = []
    base_date = datetime(2024, 1, 1)
    statuses = ["pending", "approved", "rejected"]
    flag_options = ["contains_images", "contains_links", "suspicious_language", "spam"]

    for _ in range(count):
        submitted_at = base_date + timedelta(
            days=random.randint(0, 365), hours=random.randint(0, 23)
        )

        doc = {
            "review_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, USER_POOL_SIZE):04d}",
            "product_id": f"prod_{random.randint(1, 1000):04d}",
            "review_text": fake.paragraph(nb_sentences=3),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(statuses),
            "flags": (
                random.sample(flag_options, random.randint(0, 2))
                if random.random() > 0.5
                else []
            ),
            "submitted_at": submitted_at,
        }
        data.append(doc)

    collection.delete_many({})
    collection.insert_many(data)
    print(f"[OK] ModerationQueue: {len(data)} записей")
    return len(data)


def main():
    print("=" * 60)
    print("--- Генерация данных для MongoDB ---")
    print("=" * 60)

    client = get_mongo_client()
    db = client[DB_NAME]

    try:
        stats = {
            "UserSessions": generate_user_sessions(db, RECORDS_PER_COLLECTION),
            "EventLogs": generate_event_logs(db, RECORDS_PER_COLLECTION * 2),
            "SupportTickets": generate_support_tickets(db, RECORDS_PER_COLLECTION // 2),
            "UserRecommendations": generate_user_recommendations(
                db, RECORDS_PER_COLLECTION // 2
            ),
            "ModerationQueue": generate_moderation_queue(
                db, RECORDS_PER_COLLECTION // 2
            ),
        }

        print("\n" + "=" * 60)
        print("--- Генерация завершена ---")
        print("=" * 60)
        for coll, cnt in stats.items():
            print(f"  ✅ {coll}: {cnt} документов")
        print(f"\n[INFO] Всего: {sum(stats.values())} записей")

    finally:
        client.close()
        print("\n[INFO] Соединение закрыто")


if __name__ == "__main__":
    main()
