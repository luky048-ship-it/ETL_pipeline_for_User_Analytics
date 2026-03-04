-- sql/init_dwh.sql
-- ----------------------------------------------------------------------------
-- 0. Подготовка: расширения и схема
-- ----------------------------------------------------------------------------
DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
SET search_path TO dwh, public;

-- ----------------------------------------------------------------------------
-- 1. STAGING AREA (stg_*) - Сырые данные из MongoDB
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.stg_user_sessions (
    session_id      VARCHAR(36) PRIMARY KEY,
    user_id         VARCHAR(36) NOT NULL,
    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    pages_visited   JSONB,
    device          VARCHAR(50),
    actions         JSONB,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_hash    VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dwh.stg_event_logs (
    event_id        VARCHAR(36) PRIMARY KEY,
    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    details         TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_hash    VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dwh.stg_support_tickets (
    ticket_id       VARCHAR(36) PRIMARY KEY,
    user_id         VARCHAR(36) NOT NULL,
    status          VARCHAR(30) NOT NULL,
    issue_type      VARCHAR(50) NOT NULL,
    messages        JSONB,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_hash    VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dwh.stg_user_recommendations (
    rec_id          SERIAL PRIMARY KEY,
    user_id         VARCHAR(36) NOT NULL,
    recommended_products JSONB NOT NULL,
    last_updated    TIMESTAMP WITH TIME ZONE NOT NULL,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_hash    VARCHAR(64),
    UNIQUE (user_id, last_updated)
);

CREATE TABLE IF NOT EXISTS dwh.stg_moderation_queue (
    review_id         VARCHAR(36) PRIMARY KEY,
    user_id           VARCHAR(36) NOT NULL,
    product_id        VARCHAR(36) NOT NULL,
    review_text       TEXT,
    rating            INTEGER CHECK (rating BETWEEN 1 AND 5),
    moderation_status VARCHAR(30) NOT NULL,
    flags             JSONB,
    submitted_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    _loaded_at        TIMESTAMP DEFAULT NOW(),
    _source_hash      VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_stg_sessions_user ON dwh.stg_user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_sessions_time ON dwh.stg_user_sessions(start_time, end_time);
CREATE INDEX IF NOT EXISTS idx_stg_events_time ON dwh.stg_event_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_events_type ON dwh.stg_event_logs(event_type);

-- ----------------------------------------------------------------------------
-- 2. DIMENSIONS (dim_*) - Справочники
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_users (
    user_id          VARCHAR(36) PRIMARY KEY,
    first_seen       TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen        TIMESTAMP WITH TIME ZONE NOT NULL,
    total_sessions   INTEGER DEFAULT 0,
    total_actions    INTEGER DEFAULT 0,
    _updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_id      VARCHAR(36) PRIMARY KEY,
    first_mentioned TIMESTAMP WITH TIME ZONE NOT NULL,
    review_count    INTEGER DEFAULT 0,
    avg_rating      NUMERIC(3,2),
    _updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN NOT NULL,
    quarter         INTEGER NOT NULL,
    month_name      VARCHAR(20)
);

INSERT INTO dwh.dim_date (date_key, year, month, day, day_of_week, day_name, is_weekend, quarter, month_name)
SELECT 
    d::DATE,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOW FROM d) IN (0, 6),
    EXTRACT(QUARTER FROM d)::INTEGER,
    TO_CHAR(d, 'Month')
FROM generate_series('2024-01-01'::DATE, '2029-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. FACT TABLES (fact_*) - Факты с партиционированием
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_sessions (
    session_id          VARCHAR(36) NOT NULL,
    user_id             VARCHAR(36) NOT NULL,
    start_time          TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time            TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_seconds    INTEGER GENERATED ALWAYS AS 
       ((EXTRACT(EPOCH FROM (end_time - start_time)))::INTEGER) STORED,
    device              VARCHAR(50),
    pages_count         INTEGER,
    actions_count       INTEGER,
    _loaded_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2024_q1 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2024_q2 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2024_q3 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2024_q4 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2025_q1 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2025_q2 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2025_q3 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2025_q4 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2026_q1 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2026_q2 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2026_q3 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_sessions_2026_q4 PARTITION OF dwh.fact_sessions 
    FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS dwh.fact_events (
    event_id        VARCHAR(36) NOT NULL,
    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    details         TEXT,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (event_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE TABLE IF NOT EXISTS dwh.fact_events_2024_q1 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2024_q2 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2024_q3 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2024_q4 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2025_q1 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2025_q2 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2025_q3 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2025_q4 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2026_q1 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2026_q2 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2026_q3 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS dwh.fact_events_2026_q4 PARTITION OF dwh.fact_events 
    FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');

CREATE TABLE IF NOT EXISTS dwh.fact_tickets (
    ticket_id               VARCHAR(36) PRIMARY KEY,
    user_id                 VARCHAR(36) NOT NULL,
    status                  VARCHAR(30) NOT NULL,
    issue_type              VARCHAR(50) NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    resolution_time_hours   NUMERIC(8,2) GENERATED ALWAYS AS 
       ((EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600)::NUMERIC(8,2)) STORED,
    messages_count          INTEGER,
    _loaded_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.fact_reviews (
    review_id           VARCHAR(36) PRIMARY KEY,
    user_id             VARCHAR(36) NOT NULL,
    product_id          VARCHAR(36) NOT NULL,
    review_text         TEXT,
    rating              INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    moderation_status   VARCHAR(30) NOT NULL,
    submitted_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    _loaded_at          TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 4. MART AREA (mart_*) - Витрины для аналитики
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.mart_user_engagement (
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

CREATE INDEX IF NOT EXISTS idx_mart_engagement_period 
    ON dwh.mart_user_engagement(period_start, period_end);

CREATE TABLE IF NOT EXISTS dwh.mart_support_metrics (
    metric_id               BIGSERIAL PRIMARY KEY,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    issue_type              VARCHAR(50) NOT NULL,
    total_tickets           INTEGER DEFAULT 0,
    resolved_tickets        INTEGER DEFAULT 0,
    avg_resolution_time_h   NUMERIC(10,2),
    max_resolution_time_h   NUMERIC(10,2),
    satisfaction_score      NUMERIC(3,2),
    _calculated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (period_start, period_end, issue_type)
);

-- ----------------------------------------------------------------------------
-- 5. Функции
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dwh.generate_source_hash(row_data JSONB)
RETURNS VARCHAR(64) AS $$
BEGIN
    RETURN encode(digest(row_data::text, 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;
