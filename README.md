# Отчёт по итоговому заданию Модуля 3
## ETL-процессы с использованием Apache Airflow, PostgreSQL и MongoDB

---

**Дисциплина:** ETL-процессы  
**Модуль:** 3  
**Студент:** Стас  
**Преподаватель:** Артём Озерков  
**Дата выполнения:** Март 2026  

---

## Содержание

1. [Введение](#1-введение)
2. [Описание задания](#2-описание-задания)
3. [Критерии оценивания](#3-критерии-оценивания)
4. [Архитектура решения](#4-архитектура-решения)
5. [Структура проекта](#5-структура-проекта)
6. [Технологический стек](#6-технологический-стек)
7. [Развёртывание баз данных](#7-развёртывание-баз-данных)
8. [Генерация тестовых данных](#8-генерация-тестовых-данных)
9. [ETL-пайплайны в Airflow](#9-etl-пайплайны-в-airflow)
10. [Трансформация данных](#10-трансформация-данных)
11. [Аналитические витрины](#11-аналитические-витрины)
12. [Проблемы и решения](#12-проблемы-и-решения)
13. [Верификация результатов](#13-верификация-результатов)
14. [Заключение](#14-заключение)
15. [Приложения](#15-приложения)

---

## 1. Введение

### 1.1. Актуальность темы

В современных условиях обработки больших объёмов данных ETL-процессы (Extract, Transform, Load) стали неотъемлемой частью архитектуры корпоративных систем хранения данных. Умение строить надёжные, масштабируемые и поддерживаемые ETL-пайплайны является ключевой компетенцией инженера данных.

Данная работа демонстрирует практическое применение инструментов Apache Airflow для оркестрации ETL-процессов между нереляционной (MongoDB) и реляционной (PostgreSQL) базами данных с последующим построением аналитических витрин.

### 1.2. Цель работы

Реализовать полный цикл ETL-процесса:
- Развёртывание MongoDB как источника данных
- Генерация реалистичных тестовых данных
- Настройка репликации в PostgreSQL через Airflow
- Трансформация сырых данных в формат, пригодный для аналитики
- Создание двух аналитических витрин для бизнес-отчётности

### 1.3. Задачи

1. Развернуть MongoDB и PostgreSQL в Docker-контейнерах
2. Сгенерировать тестовые данные для 5 коллекций MongoDB
3. Создать DAG для репликации данных из MongoDB в PostgreSQL
4. Реализовать этап трансформации (Staging → Fact таблицы)
5. Построить 2 аналитические витрины в Airflow
6. Обеспечить чистоту данных (без дублей, с партиционированием)
7. Документировать весь процесс

---

## 2. Описание задания

### 2.1. Требования к нереляционной базе данных (MongoDB)

Необходимо создать 5 коллекций со следующей структурой:

**1. UserSessions** — сессии пользователей:
| Поле | Тип | Описание |
|------|-----|----------|
| session_id | String | Уникальный идентификатор сессии |
| user_id | String | Идентификатор пользователя |
| start_time | DateTime | Время начала сессии |
| end_time | DateTime | Время завершения сессии |
| pages_visited | Array | Массив посещённых страниц |
| device | Object | Информация об устройстве |
| actions | Array | Массив действий пользователя |

**2. EventLogs** — логи событий:
| Поле | Тип | Описание |
|------|-----|----------|
| event_id | String | Уникальный идентификатор события |
| timestamp | DateTime | Время события |
| event_type | String | Тип события |
| details | String | Подробности |

**3. SupportTickets** — обращения в поддержку:
| Поле | Тип | Описание |
|------|-----|----------|
| ticket_id | String | Уникальный идентификатор тикета |
| user_id | String | Идентификатор пользователя |
| status | String | Статус тикета |
| issue_type | String | Тип проблемы |
| messages | Array | Массив сообщений |
| created_at | DateTime | Время создания |
| updated_at | DateTime | Время обновления |

**4. UserRecommendations** — рекомендации:
| Поле | Тип | Описание |
|------|-----|----------|
| user_id | String | Идентификатор пользователя |
| recommended_products | Array | Массив товаров |
| last_updated | DateTime | Время обновления |

**5. ModerationQueue** — модерация отзывов:
| Поле | Тип | Описание |
|------|-----|----------|
| review_id | String | Идентификатор отзыва |
| user_id | String | Идентификатор пользователя |
| product_id | String | Идентификатор товара |
| review_text | String | Текст отзыва |
| rating | Integer | Оценка (1-5) |
| moderation_status | String | Статус модерации |
| flags | Array | Массив флагов |
| submitted_at | DateTime | Время отправки |

### 2.2. Требования к реляционной базе данных (PostgreSQL)

Необходимо создать схему DWH со следующими слоями:

**Staging Area (stg_*)** — сырые данные из MongoDB
**Fact Tables (fact_*)** — факты с партиционированием по времени
**Dimension Tables (dim_*)** — справочники
**Mart Area (mart_*)** — аналитические витрины

### 2.3. Требования к Airflow

- Минимум 3 DAG: репликация + 2 витрины
- Этап трансформации данных
- Документация пайплайнов
- Обработка ошибок и retries

---

## 3. Критерии оценивания

| Критерий | Баллы | Статус |
|----------|-------|--------|
| Развёрнута реляционная база данных | 1 | ok |
| Развёрнута нереляционная база данных | 1 | ok |
| Сгенерированы данные для MongoDB | 0.5 | ok |
| Пайплайны репликации MongoDB → PostgreSQL + Airflow | 1 | ok |
| Этап трансформации данных | 1 | ok |
| Данные чистые (без дублей, партиционированы) | 1 | ok |
| Документация пайплайнов | 0.5 | ok |
| Пайплайны для аналитических витрин | 1 | ok |
| Создано 2 аналитические витрины | 1 | ok |
| **Итого** | **8** | **ok** |

---

## 4. Архитектура решения

### 4.1. Общая архитектура системы

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                  ETL PIPELINE ARCHITECTURE                              │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐      ┌──────────────┐       ┌──────────────────┐
│   MongoDB    │────▶│    Airflow   │────▶│  PostgreSQL  │────▶ │     Marts        │
│   (Source)   │     │    (ETL)     │      │   (DWH)      │       │ (Analytics)      │
│              │     │              │      │              │       │                  │
│ • UserSessions│    │ • Extract    │      │ • Staging    │       │ • Engagement     │
│ • EventLogs   │    │ • Transform  │      │ • Fact       │       │ • Support        │
│ • Tickets     │    │ • Load       │      │ • Dimension  │       │ • Recommendations│
│ • Moderation  │    │              │      │              │       │ • Mart           │
│               │    │              │      │              │       │                  │
└──────────────┘     └──────────────┘      └──────────────┘       └──────────────────┘
        │                   │                   │                          │
        ▼                   ▼                   ▼                          ▼
   Port 27017          Web UI:8080          Port 5433                   Grafana
   MongoDB             Airflow Scheduler    PostgreSQL                 Docker Net
                                                                      DWH (future)
```

### 4.2. Компоненты архитектуры

| Компонент | Версия | Назначение |
|-----------|--------|------------|
| MongoDB | 7 | Источник данных (NoSQL) |
| PostgreSQL | 16-alpine | Хранилище данных (DWH) |
| Apache Airflow | 2.10.4 | Оркестрация ETL-процессов |
| Docker Compose | 3.8 | Контейнеризация |
| Python | 3.12 | Скрипты генерации и DAG |

### 4.3. Поток данных

1. ГЕНЕРАЦИЯ
    scripts/generate_data.py → MongoDB (5 коллекций, 4500 документов)
2. EXTRACT & LOAD
    mongo_to_postgres_dag.py → PostgreSQL Staging (stg_* таблицы)
3. TRANSFORM
    mongo_to_postgres_dag.py → PostgreSQL Fact (fact_* таблицы)
4. MART CREATION
    mart_user_engagement_dag.py → dwh.mart_user_engagement
    mart_support_metrics_dag.py → dwh.mart_support_metrics


### 4.4. Схема данных DWH

```text
┌────────────────────────────────────────────────────────────────────┐
│                        DWH SCHEMA (dwh)                            │
├────────────────────────────────────────────────────────────────────┤
│  STAGING          FACT             MART                            │
│  ───────          ────             ────                            │
│ stg_user_sessions  →  fact_sessions  →  mart_user_engagement       │
│ stg_event_logs     →  fact_events    →  (входит в mart_user_…)     │
│ stg_support_tickets→  fact_tickets   →  mart_support_metrics       │
│ stg_moderation_queue→ fact_reviews   →  (входит в mart_support_…)  │
│ stg_user_recommendations                                           │
│                                                                    │
│                         DIMENSIONS                                 │
│                         ──────────                                 │
│ dim_users, dim_products, dim_date                                  │
└────────────────────────────────────────────────────────────────────┘
```

---

## 5. Структура проекта

### 5.1. Дерево файлов
```text
ETL_pipeline_for_User_Analytics/
├── docker-compose.yaml # Оркестрация контейнеров
├── .env # Переменные окружения
├── README.md # Документация проекта
├── dags/
│ ├── mongo_to_postgres_dag.py # ETL: MongoDB → PostgreSQL
│ ├── mart_user_engagement_dag.py # Витрина 1: Активность
│ └── mart_support_metrics_dag.py # Витрина 2: Поддержка
├── scripts/
│ ├── generate_data.py # Генератор данных MongoDB
│ └── init_connections.sh # Инициализация Airflow Connections
├── sql/
│ └── init_dwh.sql # Инициализация схемы DWH
├── config/
│ └── grafana/
│ └── provisioning/ # Настройки Grafana (будущее)
├── logs/ # Логи Airflow (автогенерация)
├── plugins/ # Плагины Airflow (пусто)
```
### 5.2. Описание файлов

| Файл | Назначение | Строк кода |
|------|------------|------------|
| docker-compose.yaml | Конфигурация 7 сервисов | ~200 |
| .env | Переменные окружения | ~20 |
| generate_data.py | Генерация 4500 документов | ~250 |
| init_dwh.sql | Создание схемы DWH | ~300 |
| mongo_to_postgres_dag.py | ETL пайплайн | ~200 |
| mart_user_engagement_dag.py | Витрина 1 | ~80 |
| mart_support_metrics_dag.py | Витрина 2 | ~80 |

---

## 6. Технологический стек

### 6.1. Базы данных

**MongoDB 7**
- Тип: Документоориентированная NoSQL БД
- Назначение: Источник сырых данных
- Коллекции: 5
- Документы: 4500
- Порт: 27017

**PostgreSQL 16**
- Тип: Реляционная БД
- Назначение: Хранилище данных (DWH)
- Схема: dwh
- Таблицы: 15+ (staging, fact, dimension, mart)
- Порт: 5433 (внешний)

### 6.2. Оркестрация

**Apache Airflow 2.10.4**
- Executor: LocalExecutor
- DAGs: 3
- Задачи: 5 (2 в ETL + 1 в каждой витрине)
- Web UI: http://localhost:8080

### 6.3. Контейнеризация

**Docker Compose 3.8**
- Сервисы: 7 (MongoDB, PostgreSQL×2, Airflow×2, Redis, Grafana)
- Сети: 2 (airflow_internal, analytics_network)
- Тома: 5 (persistent data)

### 6.4. Библиотеки Python

pymongo\==4.16.0 # MongoDB драйвер
psycopg2-binary # PostgreSQL драйвер
apache-airflow-providers-mongo
apache-airflow-providers-postgres
faker\==40.5.1 # Генерация данных
pandas # Обработка данных
numpy # Вычисления

---

## 7. Развёртывание баз данных

### 7.1. Docker Compose конфигурация

**Ключевые сервисы:**

```yaml
# MongoDB
mongodb:
  image: mongo:7
  environment:
    MONGO_INITDB_ROOT_USERNAME: admin
    MONGO_INITDB_ROOT_PASSWORD: password
  ports:
    - "27017:27017"
  volumes:
    - mongodb_/data/db

# PostgreSQL DWH
postgres_dwh:
  image: postgres:16-alpine
  environment:
    POSTGRES_USER: dwh_user
    POSTGRES_PASSWORD: dwh_password
    POSTGRES_DB: dwh_db
  volumes:
    - ./sql/init_dwh.sql:/docker-entrypoint-initdb.d/init_dwh.sql
  ports:
    - "5433:5432"

# Airflow Webserver
airflow-webserver:
  image: apache/airflow:2.10.4-python3.12
  command: webserver
  ports:
    - "8080:8080"
  depends_on:
    - airflow-init

# Airflow Scheduler
airflow-scheduler:
  image: apache/airflow:2.10.4-python3.12
  command: scheduler
  depends_on:
    - airflow-init
```

### 7.2. Инициализация DWH
Файл sql/init_dwh.sql создаёт:
1. Схема dwh с расширениями pgcrypto, uuid-ossp
2. Staging таблицы (5 таблиц) для сырых данных
3. Fact таблицы (4 таблицы) с партиционированием по кварталам 2024-2026
4. Dimension таблицы (3 справочника)
5. Mart таблицы (2 витрины)
6. Функции для генерации хэшей


**Партиционирование fact_sessions:**

```sql
CREATE TABLE dwh.fact_sessions_2024_q1 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
-- ... ещё 11 партиций до 2026_q4
```

### 7.3. Команды развёртывания

#### 1. Очистка старых контейнеров
docker compose down -v

#### 2. Запуск всех сервисов
docker compose up -d

#### 3. Ожидание инициализации (2-3 минуты)
sleep 120

#### 4. Проверка статуса
docker compose ps

#### 5. Проверка таблиц DWH
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "\dt dwh.*"

## 8. Генерация тестовых данных

### 8.1. Скрипт generate_data.py

**Расположение**: `scripts/generate_data.py`

**Функционал**:

- Подключение к MongoDB с аутентификацией
- Генерация пула из 500 уникальных пользователей
- Создание 5 коллекций с реалистичными данными
- Очистка существующих данных перед генерацией
- Статистика по завершении

### 8.2. Параметры генерации

| Коллекция            | Количество | Диапазон дат              |
|----------------------|------------|---------------------------|
| UserSessions         | 1000       | 2024-01-01 — 2024-12-31   |
| EventLogs            | 2000       | 2024-01-01 — 2024-12-31   |
| SupportTickets       | 500        | 2024-01-01 — 2024-12-31   |
| UserRecommendations  | 500        | 2024-01-01 — 2024-12-31   |
| ModerationQueue      | 500        | 2024-01-01 — 2024-12-31   |
| **Итого**            | **4500**   | —                         |

### 8.3. Пример сгенерированного документа

 **UserSessions:**

 ```json
 {
  "session_id": "550e8400-e29b-41d4-a716-446655440001",
  "user_id": "user_0239",
  "start_time": "2024-03-15T14:30:00",
  "end_time": "2024-03-15T16:34:00",
  "pages_visited": ["/home", "/products", "/cart"],
  "device": "desktop",
  "actions": ["login", "view_product", "add_to_cart"]
}
```

### 8.4. Команда запуска

#### Активация виртуального окружения
source .venv/bin/activate

#### Установка зависимостей
pip install pymongo faker

#### Запуск генератора
python scripts/generate_data.py


### 8.5. Результат генерации

```text

============================================================
--- Генерация данных для MongoDB ---
============================================================
[OK] Подключение к MongoDB успешно
[OK] UserSessions: 1000 записей
[OK] EventLogs: 2000 записей
[OK] SupportTickets: 500 записей
[OK] UserRecommendations: 500 записей
[OK] ModerationQueue: 500 записей
============================================================
--- Генерация завершена ---
============================================================
  ✅ UserSessions: 1000 документов
  ✅ EventLogs: 2000 документов
  ✅ SupportTickets: 500 документов
  ✅ UserRecommendations: 500 документов
  ✅ ModerationQueue: 500 документов

[INFO] Всего: 4500 записей

```

## 9. ETL-пайплайны в Airflow

### 9.1. Обзор DAGs

В проекте реализовано 3 DAG (Directed Acyclic Graph):

| DAG ID | Описание | Расписание | Задач |
|--------|----------|------------|-------|
| `mongo_to_postgres_replication` | Репликация MongoDB → PostgreSQL | Manual (on-demand) | 2 |
| `mart_user_engagement` | Витрина: Активность пользователей | @daily | 1 |
| `mart_support_metrics` | Витрина: Эффективность поддержки | @daily | 1 |

### 9.2. DAG репликации данных

**Файл:** `dags/mongo_to_postgres_dag.py`

**Структура пайплайна:**

```text
┌─────────────────────┐     ┌─────────────────────────────┐
│ extract_load_staging│───▶│ transform_staging_to_fact   │
│   (Extract + Load)  │     │     (Transform)             │
│                     │     │                             │
│ • Подключение к     │     │ • Staging → Fact            │
│   MongoDB           │     │                             │
│ • Чтение коллекций  │     │ • Дедупликация              │
│ • Запись в Staging  │     │ • Агрегация                 │
└─────────────────────┘     └─────────────────────────────┘
```

**Ключевые компоненты:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from pymongo import MongoClient

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

##### Функция extract_load_staging

- Получает подключения из Airflow Connections
- Подключается к MongoDB через pymongo
- Извлекает документы из 4 коллекций
- Загружает в staging таблицы PostgreSQL
- Обрабатывает конфликты (ON CONFLICT DO NOTHING)

##### Функция transform_staging_to_fact

- Читает данные из staging таблиц
- Трансформирует в формат fact таблиц
- Вычисляет производные поля (duration_seconds, messages_count)
- Обновляет справочник dim_users

### 9.3. Конфигурация Airflow Connections

**mongo_source:**

##### Параметры подключения к MongoDB (источник данных)

| Параметр     | Значение              | Описание                              |
|--------------|-----------------------|---------------------------------------|
| **Type**     | Mongo                 | Тип базы данных                       |
| **Login**    | admin                 | Имя пользователя (учётная запись)     |
| **Password** | password              | Пароль пользователя                   |
| **Host**     | mongodb               | Хост (имя сервиса в Docker-сети)      |
| **Port**     | 27017                 | Стандартный порт MongoDB              |
| **Schema**   | etl_source_data       | База данных (database) в MongoDB      |
| **Extra**    | `{"authSource": "admin"}` | Дополнительные параметры аутентификации (authSource указывает базу с учётными данными) |

**postgres_dwh:**

##### Параметры подключения к PostgreSQL (DWH — хранилище данных)

| Параметр     | Значение              | Описание                              |
|--------------|-----------------------|---------------------------------------|
| **Type**     | Postgres              | Тип базы данных                       |
| **Login**    | dwh_user              | Имя пользователя (роль/учётная запись)|
| **Password** | dwh_password          | Пароль пользователя                   |
| **Host**     | postgres_dwh          | Хост (имя сервиса в Docker-сети)      |
| **Port**     | 5432                  | Стандартный порт PostgreSQL           |
| **Schema**   | dwh_db                | Имя базы данных (database)            |

## 10. Трансформация данных

```text
┌─────────────────────────────────────────────────────────────────────┐
│                       TRANSFORMATION FLOW                           │
└─────────────────────────────────────────────────────────────────────┘

MongoDB                  Staging (PostgreSQL)           Fact (PostgreSQL)
─────────                ──────────────────             ─────────────────

UserSessions          →  stg_user_sessions          →  fact_sessions
  • session_id              • session_id                  • session_id (PK)
  • user_id                 • user_id                     • user_id
  • start_time              • start_time                  • start_time (PK)
  • end_time                • end_time                    • end_time
  • pages_visited[]         • pages_visited (JSONB)       • duration_seconds (calc)
  • device                  • device                      • pages_count (calc)
  • actions[]               • actions (JSONB)             • actions_count (calc)

EventLogs             →  stg_event_logs              →  fact_events
  • event_id                • event_id                    • event_id (PK)
  • timestamp               • timestamp                   • timestamp (PK)
  • event_type              • event_type                  • event_type
  • details                 • details                     • details

SupportTickets        →  stg_support_tickets         →  fact_tickets
  • ticket_id               • ticket_id                   • ticket_id (PK)
  • user_id                 • user_id                     • user_id
  • status                  • status                      • status
  • issue_type              • issue_type                  • issue_type
  • messages[]              • messages (JSONB)            • created_at
  • created_at              • created_at                  • updated_at
  • updated_at              • updated_at                  • resolution_time_hours (calc)
                                                            • messages_count (calc)

ModerationQueue       →  stg_moderation_queue         →  fact_reviews
  • review_id               • review_id                   • review_id (PK)
  • user_id                 • user_id                     • user_id
  • product_id              • product_id                  • product_id
  • review_text             • review_text                 • review_text
  • rating                  • rating                      • rating
  • moderation_status       • moderation_status           • moderation_status
  • flags[]                 • flags (JSONB)               • submitted_at
  • submitted_at            • submitted_at
  ```

## 11. Аналитические витрины

### 11.1. Витрина 1: User Engagement

**Назначение**: Анализ активности пользователей по времени, определение популярных страниц и действий.

**Таблица**: dwh.mart_user_engagement

**Структура**:

```sql
CREATE TABLE dwh.mart_user_engagement (
    user_id                 VARCHAR(36) NOT NULL,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    total_sessions          INTEGER DEFAULT 0,
    total_duration_seconds  BIGINT DEFAULT 0,
    avg_session_duration    NUMERIC(10,2),
    unique_pages_visited    INTEGER DEFAULT 0,
    total_actions           INTEGER DEFAULT 0,
    devices_used            JSONB DEFAULT '{}'::jsonb,
    last_activity           TIMESTAMP WITH TIME ZONE,
    _calculated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, period_start, period_end)
);
```
**SQL-запрос витрины:**

```sql
INSERT INTO dwh.mart_user_engagement (...)
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
WHERE DATE(start_time) = '{{ ds }}'
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
```

**Бизнес-метрики:**

| Метрика                | Описание                                      |
|------------------------|-----------------------------------------------|
| total_sessions         | Количество сессий пользователя за период      |
| total_duration_seconds | Общее время на сайте (секунды)                |
| avg_session_duration   | Средняя длительность сессии                   |
| unique_pages_visited   | Уникальных страниц просмотрено                |
| total_actions          | Всего действий выполнено                      |
| devices_used           | Используемые устройства (JSON)                |

**Пример результата:**

| user_id   | total_sessions | total_minutes | total_actions | devices_used          |
|-----------|----------------|---------------|---------------|-----------------------|
| user_0239 | 2              | 124           | 4             | {"desktop": 1}        |
| user_0084 | 1              | 119           | 2             | {"desktop": 1}        |
| user_0036 | 1              | 119           | 5             | {"desktop": 1}        |
| user_0115 | 1              | 119           | 2             | {"mobile": 1}         |
| user_0492 | 1              | 119           | 2             | {"tablet": 1}         |

![здесь должен быть скриншот]()

### 11.2. Витрина 2: Support Metrics

**Назначение**:  
Отслеживание статуса обращений, типа проблем, времени решения.

**Таблица**: dwh.mart_support_metrics

**Структура**:

```sql
CREATE TABLE dwh.mart_support_metrics (
    metric_id               BIGSERIAL PRIMARY KEY,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    issue_type              VARCHAR(50) NOT NULL,
    total_tickets           INTEGER DEFAULT 0,
    resolved_tickets        INTEGER DEFAULT 0,
    avg_resolution_time_h   NUMERIC(10,2),
    max_resolution_time_h   NUMERIC(10,2),
    _calculated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (period_start, period_end, issue_type)
);
```

**SQL-запрос витрины:**

```sql
INSERT INTO dwh.mart_support_metrics (...)
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
WHERE DATE(created_at) = '{{ ds }}'
GROUP BY DATE(created_at), issue_type
ON CONFLICT (period_start, period_end, issue_type) DO UPDATE SET
    total_tickets = EXCLUDED.total_tickets,
    resolved_tickets = EXCLUDED.resolved_tickets,
    avg_resolution_time_h = EXCLUDED.avg_resolution_time_h,
    max_resolution_time_h = EXCLUDED.max_resolution_time_h,
    _calculated_at = EXCLUDED._calculated_at;
```

**Бизнес-метрики:**

| Метрика                | Описание                              |
|------------------------|---------------------------------------|
| total_tickets          | Всего обращений за период             |
| resolved_tickets       | Решённых обращений                    |
| avg_resolution_time_h  | Среднее время решения (часы)          |
| max_resolution_time_h  | Максимальное время решения (часы)     |

### 11.3. DAG витрин

**Файл:** `dags/mart_user_engagement_dag.py`

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
```

**Файл:** `dags/mart_support_metrics_dag.py`

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
```

## 12. Проблемы и решения

### 12.1. Хронология проблем

В процессе выполнения задания было преодолено множество технических препятствий. Ниже приведена детальная хронология проблем и способы их решения.

### 12.2. Проблема 1: Конфликт версий Airflow

**Описание проблемы**:  
Изначально использовался образ `apache/airflow:3.1.7`, который имеет микросервисную архитектуру (api-server, dag-processor, triggerer, worker). Данная версия находится в разработке и имеет значительные отличия от стабильной ветки 2.x.

**Симптомы**:

- DAG не запускаются, падают сразу после планирования
- Ошибки импорта операторов (`airflow.providers.standard` не найден)
- Healthcheck не проходит (`/api/v2/version` не отвечает)
- Коннекты не создаются через CLI

**Попытки решения (неудачные)**:

* **Ручная установка провайдеров:** Попытка внедрить pip install apache-airflow-providers-standard в Dockerfile. Результат: конфликт зависимостей с предустановленными библиотеками ядра v3.

* **Изменение PYTHONPATH:** Попытка явно указать пути к DAG-файлам и плагинам через переменные окружения. Результат: dag-processor видел файлы, но worker не мог их десериализовать.

* **Использование Fast-API прокси:** Попытка дождаться ответа от api-server через кастомный healthcheck. Результат: API v3 имеет другую структуру эндпоинтов, несовместимую со стандартными скриптами проверки.

* **Адаптация Docker Compose:** Попытка объединить все компоненты Airflow в одну сеть с общими томами для библиотек Python. Результат: архитектурное ограничение — компоненты v3 требуют строгой изоляции и связи только через API.

**Финальное решение**: Переход на стабильную версию apache/airflow:2.10.4-python3.12 с монолитной архитектурой (webserver, scheduler).

**Изменения в docker-compose.yaml**:

```yaml
# Было (3.1.7 - микросервисы)
services:
  airflow-apiserver:
    command: api-server
  airflow-dag-processor:
    command: dag-processor
  airflow-triggerer:
    command: triggerer
  airflow-worker:
    command: celery worker

# Стало (2.10.4 - монолит)
services:
  airflow-webserver:
    command: webserver
  airflow-scheduler:
    command: scheduler
```

**Изменения в DAG:**

```python
# Было (3.1.7)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Стало (2.10.4)
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
```

### 12.3. Проблема 2: Аутентификация MongoDB

**Описание проблемы**:  
Скрипт генерации данных не мог подключиться к MongoDB из-за ошибки аутентификации.

**Симптомы**:

```text
pymongo.errors.OperationFailure: Authentication failed.
full error: {'ok': 0.0, 'errmsg': 'Authentication failed.', 'code': 18}
```

**Попытки решения (неудачные):**

```python
# Попытка 1: Неправильный URI
MONGO_URI = "mongodb://admin:password@localhost:27017/"
# Результат: Ошибка authSource

# Попытка 2: Отсутствие authSource
MONGO_URI = "mongodb://admin:password@localhost:27017/etl_source_data"
# Результат: Аутентификация в неправильной базе

# Попытка 3: Неправильный порт
MONGO_URI = "mongodb://admin:password@localhost:27018/"
# Результат: Connection refused
```

**Финальное решение:**
Корректный URI с указанием `authSource=admin:`

```python
MONGO_URI = "mongodb://mongo_admin:mongo_pass@localhost:27017/etl_source_data?authSource=admin"
```

12.4. Проблема 3: Airflow Connections

**Описание проблемы:**
DAG падал с ошибкой Connection 'mongo_source' not found.

**Симптомы:**

```sh
airflow.exceptions.AirflowNotFoundException: The conn_id `mongo_source` isn't defined
```

**Причина:**
Connections не были созданы при инициализации Airflow. - в случае с версией airflow 2.10.4
И незвестаная в случае с Airflow:latest версии, основная причина почему пришлось упростить проект

**Финальное решение:**
Добавление создания connections в `airflow-init:`

```yaml
airflow-init:
  command:
    - -c
    - |
      airflow db upgrade
      airflow users create ...
      airflow connections add 'mongo_source' \
        --conn-type 'mongo' \
        --conn-login 'admin' \
        --conn-password 'password' \
        --conn-host 'mongodb' \
        --conn-port '27017' \
        --conn-schema 'etl_source_data'
      airflow connections add 'postgres_dwh' \
        --conn-type 'postgres' \
        --conn-login 'dwh_user' \
        --conn-password 'dwh_password' \
        --conn-host 'postgres_dwh' \
        --conn-port '5432' \
        --conn-schema 'dwh_db'
```

### 12.5. Итоговая архитектура (после всех исправлений)

```text
┌────────────────────────────────────────────────────────────────────┐
│                    FINAL ARCHITECTURE (STABLE)                     │
└────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   MongoDB    │     │    Airflow   │     │  PostgreSQL  │
│   :27017     │────▶│   :8080      │────▶│   :5433      │
│              │     │              │     │              │
│ • 5 collections│   │ • 3 DAGs     │     │ • 15 tables  │
│ • 4500 docs  │     │ • 5 tasks    │     │ • Partitioned│
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
  generate_data.py    mongo_to_postgres    mart_*.py
                      _dag.py
```

**Компоненты:**
| Компонент       | Версия              | Статус     |
|-----------------|---------------------|------------|
| Apache Airflow  | 2.10.4-python3.12   | Stable  |
| MongoDB         | 7                   | Stable  |
| PostgreSQL      | 16-alpine           | Stable  |
| Docker Compose  | 3.8                 | Stable  |
| Python          | 3.11                | Stable  |


**Сервисы:**
| Сервис            | Контейнеры | Статус                  |
|-------------------|------------|-------------------------|
| mongodb           | 1          | Running              |
| postgres_meta     | 1          | Running              |
| postgres_dwh      | 1          | Running              |
| airflow-webserver | 1          | Running              |
| airflow-scheduler | 1          | Running              |

## 13. Верификация результатов

**13.1. Проверка MongoDB**

```sh
docker compose exec mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('etl_source_data').getCollectionNames()"
```

**Ожидаемый результат:**

```sh
[
  'UserSessions',
  'EventLogs',
  'SupportTickets',
  'UserRecommendations',
  'ModerationQueue'
]
```

**Подсчёт документов:**

```sh
db.getSiblingDB('etl_source_data').UserSessions.countDocuments()
// 1000

db.getSiblingDB('etl_source_data').EventLogs.countDocuments()
// 2000
```

### 13.2. Проверка PostgreSQL Staging

```sql
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "
SELECT 'stg_user_sessions' as table_name, COUNT(*) FROM dwh.stg_user_sessions
UNION ALL
SELECT 'stg_event_logs', COUNT(*) FROM dwh.stg_event_logs
UNION ALL
SELECT 'stg_support_tickets', COUNT(*) FROM dwh.stg_support_tickets
UNION ALL
SELECT 'stg_moderation_queue', COUNT(*) FROM dwh.stg_moderation_queue;
"
```
**Ожидаемый результат:**

```text
     table_name      │ count
─────────────────────┼───────
 stg_user_sessions   │  1000
 stg_event_logs      │  2000
 stg_support_tickets │   500
 stg_moderation_queue│   500
```

### 13.3. Проверка PostgreSQL Fact

``` sql
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "
SELECT 'fact_sessions' as table_name, COUNT(*) FROM dwh.fact_sessions
UNION ALL
SELECT 'fact_events', COUNT(*) FROM dwh.fact_events
UNION ALL
SELECT 'fact_tickets', COUNT(*) FROM dwh.fact_tickets
UNION ALL
SELECT 'fact_reviews', COUNT(*) FROM dwh.fact_reviews;
"
```
**Ожидаемый результат:**

```text
    table_name    │ count
──────────────────┼───────
 fact_sessions    │  1000
 fact_events      │  2000
 fact_tickets     │   500
 fact_reviews     │   500
```

###  13.4. Проверка витрин

```sql
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "
SELECT 'User Engagement' as mart, COUNT(*) FROM dwh.mart_user_engagement
UNION ALL
SELECT 'Support Metrics', COUNT(*) FROM dwh.mart_support_metrics;
"
```
**Ожидаемый результат:**

```text
       mart        │ count
───────────────────┼───────
 User Engagement   │   999
 Support Metrics   │   426
```

### 13.5. Проверка Airflow DAGs

```sh
 Список DAGs
docker compose exec airflow-webserver airflow dags list

# Статус DAGs
docker compose exec airflow-webserver airflow dags list --state running

# История запусков
docker compose exec airflow-webserver airflow dags history mongo_to_postgres_replication
```

**Ожидаемый результат:**

```text
                    DAG ID                    │ Schedule │ State
──────────────────────────────────────────────┼──────────┼───────
 mongo_to_postgres_replication                │ None     │ active
 mart_user_engagement                         │ @daily   │ active
 mart_support_metrics                         │ @daily   │ active
```

### 13.6. Скриншоты выполнения

**Рисунок 1:** Airflow UI — список DAGs

![скриншот](./skreenshot/2026-03-04_17-25-55.png)

**Рисунок 2:** Fact таблицы — количество записей

![скриншот](./skreenshot/2026-03-04_17-52-20.png)

**Рисунок 3:** Mart таблицы — количество записей

![скриншот](./skreenshot/2026-03-04_17-52-42.png)

**Рисунок 4:** Пример данных mart_user_engagement

![скриншот](./skreenshot/2026-03-04_17-52-05.png)


## 14. Заключение

### 14.1. Достигнутые результаты

В ходе выполнения итогового задания модуля 3 была успешно реализована полная ETL-система со следующими характеристиками:

**Инфраструктура**:

- Развёрнута нереляционная БД (MongoDB 7)
- Развёрнута реляционная БД (PostgreSQL 16)
- Настроен Apache Airflow 2.10.4 для оркестрации

**Данные**:

- Сгенерировано 4500 документов в 5 коллекциях MongoDB
- Данные реплицированы в PostgreSQL Staging
- Данные трансформированы в Fact таблицы
- Создано 2 аналитические витрины

**Пайплайны**:

- DAG репликации (2 задачи: Extract+Load, Transform)
- DAG витрины User Engagement (1 задача)
- DAG витрины Support Metrics (1 задача)

**Качество данных**:

- Отсутствие дублей (ON CONFLICT DO NOTHING)
- Партиционирование по кварталам (2024-2026)
- Вычисляемые поля (duration, resolution_time)
- Индексы для ускорения запросов


## 15. Приложения

### 15.1. Команды быстрого старта

```sh
# ============================================================================
# ПОЛНЫЙ ЦИКЛ ЗАПУСКА ПРОЕКТА
# ============================================================================

# 1. Очистка старой инфраструктуры
docker compose down -v
rm -rf dags/__pycache__ logs/dag_id=*

# 2. Запуск всех сервисов
docker compose up -d

# 3. Ожидание инициализации (2-3 минуты)
sleep 120

# 4. Проверка статуса контейнеров
docker compose ps

# 5. Генерация данных в MongoDB
pip install pymongo faker
python scripts/generate_data.py

# 6. Проверка MongoDB
docker compose exec mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('etl_source_data').getCollectionNames()"

# 7. Airflow UI
echo "Откройте в браузере: http://localhost:8080"
echo "Логин: admin / Пароль: admin"

# 8. Запуск ETL пайплайна
docker compose exec airflow-webserver airflow dags trigger mongo_to_postgres_replication

# 9. Ожидание завершения ETL (1-2 минуты)
sleep 60

# 10. Запуск витрин
docker compose exec airflow-webserver airflow dags trigger mart_user_engagement
docker compose exec airflow-webserver airflow dags trigger mart_support_metrics

# 11. Проверка данных в PostgreSQL
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "\dt dwh.*"
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "SELECT COUNT(*) FROM dwh.fact_sessions;"
docker compose exec postgres_dwh psql -U dwh_user -d dwh_db -c "SELECT COUNT(*) FROM dwh.mart_user_engagement;"

# 12. Просмотр логов (при проблемах)
docker compose logs -f airflow-webserver --tail=50
docker compose logs -f airflow-scheduler --tail=50
```

### 15.2. SQL-запросы для аналитики

**Топ-10 пользователей по времени на сайте:**

```sql
SELECT 
    user_id,
    total_sessions,
    total_duration_seconds / 60 AS total_minutes,
    avg_session_duration / 60 AS avg_session_minutes,
    total_actions
FROM dwh.mart_user_engagement
ORDER BY total_duration_seconds DESC
LIMIT 10;
```

**Эффективность поддержки по типам проблем:**

```sql
SELECT 
    issue_type,
    total_tickets,
    resolved_tickets,
    ROUND(100.0 * resolved_tickets / NULLIF(total_tickets, 0), 2) AS resolution_rate_pct,
    avg_resolution_time_h
FROM dwh.mart_support_metrics
ORDER BY total_tickets DESC;
```

```sql
SELECT 
    dd.day_name,
    COUNT(fs.session_id) AS total_sessions,
    AVG(fs.duration_seconds) / 60 AS avg_duration_minutes
FROM dwh.fact_sessions fs
JOIN dwh.dim_date dd ON DATE(fs.start_time) = dd.date_key
GROUP BY dd.day_name, dd.day_of_week
ORDER BY dd.day_of_week;
```

### 15.3. Ссылки на ресурсы

| Ресурс              | URL                        |
|---------------------|----------------------------|
| Airflow UI          | http://localhost:8080      |
| Grafana (будущее)   | http://localhost:3000      |
| Mongo Express (dev) | http://localhost:8081      |
| PostgreSQL DWH      | localhost:5433             |
| MongoDB             | localhost:27017            |


### Список использованных источников

1. Apache Airflow Documentation. https://airflow.apache.org/docs/
2. MongoDB Manual. https://docs.mongodb.com/manual/
3. PostgreSQL Documentation. https://www.postgresql.org/docs/
4. Docker Compose Specification. https://docs.docker.com/compose/

## 16. Использованиие генеративной модели

### 16.1. Соответствие требованиям НИУ ВШЭ

Настоящая работа выполнена в соответствии с «Методическими рекомендациями по применению технологий искусственного интеллекта в образовательной деятельности НИУ ВШЭ» (2024). Студент декларирует открытое использование инструментов ИИ при выполнении задания с соблюдением принципов академической добросовестности.

### 16.2. Области использования ИИ-инструментов

В процессе выполнения работы инструменты на базе искусственного интеллекта применялись в следующих аспектах:

| Этап работы | Роль ИИ | Роль студента |
|-------------|---------|---------------|
| Архитектурное проектирование | Предложения по структуре ETL-пайплайна | Выбор финальной архитектуры, утверждение схемы данных |
| Написание кода (DAG, скрипты) | Генерация шаблонного кода, рефакторинг | Логика бизнес-правил, отладка, интеграция компонентов |
| Поиск и устранение ошибок | Диагностика ошибок, предложения по исправлению | Принятие решений, тестирование решений, верификация |
| Документирование | Форматирование текста, проверка орфографии | Содержание, технические детали, выводы |
| SQL-запросы | Оптимизация запросов, проверка синтаксиса | Проектирование схемы, бизнес-логика витрин |

### 16.3. Используемые инструменты

| Инструмент | Назначение | Доля участия |
|------------|------------|--------------|
| LLM-ассистент (текстовый) | Генерация кода, отладка, документация | ~60% кода |
| LLM-ассистент (поиск) | Поиск решений технических проблем | Справочно |
| Стандартные IDE (Nvim, VScode) | Написание и тестирование кода | Основной инструмент |

### 16.4. Самостоятельно выполненные элементы

Следующие компоненты работы разработаны студентом самостоятельно без генерации ИИ:

- Общая архитектура решения и выбор технологического стека
- Проектирование схемы данных DWH (staging, fact, dimension, mart)
- Бизнес-логика аналитических витрин (метрики, расчёты)
- Диагностика и решение проблем с подключением MongoDB
- Тестирование и верификация результатов ETL-процессов
- Итоговая сборка, отладка и демонстрация работы пайплайнов

### 16.5. Верификация результатов

Все сгенерированные ИИ фрагменты кода прошли обязательную проверку студентом:

1. **Тестирование** — каждый DAG запущен минимум 1 раз с успешным завершением
2. **Валидация данных** — соответствие количества записей в источнике и приёмнике
3. **Интеграция** — все компоненты работают в единой Docker-инфраструктуре
4. **Документирование** — студент понимает логику каждого элемента кода

### 16.6. Академическая добросовестность

Студент подтверждает, что:

- Понимает назначение и логику работы всех представленных в работе компонентов
- Способен объяснить архитектурные решения и альтернативы
- Прошёл все этапы тестирования и отладки самостоятельно
- Использовал ИИ как инструмент поддержки обучения, а не замены собственного понимания

### 16.7. Рекомендации по использованию ИИ в учебных работах

На основе опыта выполнения данной работы студент сформировал следующие принципы ответственного использования ИИ:

| Принцип | Описание |
|---------|----------|
| Прозрачность | Открыто декларировать использование ИИ в работе |
| Верификация | Всегда проверять и тестировать сгенерированный код |
| Понимание | Не включать код, который не можешь объяснить |
| Документирование | Фиксировать какие этапы выполнены с помощью ИИ |
| Баланс | Использовать ИИ для ускорения рутины, а не замены мышления |


