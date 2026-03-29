CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- API Keys table
CREATE TABLE IF NOT EXISTS api_keys (
    id          SERIAL PRIMARY KEY,
    key_hash    TEXT UNIQUE NOT NULL,
    email       TEXT NOT NULL,
    name        TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    last_used   TIMESTAMPTZ,
    is_active   BOOLEAN DEFAULT true,
    request_count INT DEFAULT 0
);

-- Raw job listings
CREATE TABLE IF NOT EXISTS jobs (
    id          SERIAL PRIMARY KEY,
    external_id TEXT UNIQUE,
    title       TEXT NOT NULL,
    company     TEXT,
    location    TEXT,
    remote      BOOLEAN DEFAULT false,
    salary_min  INT,
    salary_max  INT,
    currency    TEXT DEFAULT 'USD',
    description TEXT,
    source      TEXT,
    url         TEXT,
    scraped_at  DATE DEFAULT CURRENT_DATE
);

-- Aggregated skill counts per day per role category
-- One row per (skill, role, day) — re-running ETL on same day overwrites, never doubles
CREATE TABLE IF NOT EXISTS skills (
    id      SERIAL PRIMARY KEY,
    skill   TEXT NOT NULL,
    role    TEXT NOT NULL,
    count   INT DEFAULT 0,
    day     DATE NOT NULL,
    UNIQUE(skill, role, day)
);

-- API usage logs
CREATE TABLE IF NOT EXISTS api_usage_logs (
    id          SERIAL PRIMARY KEY,
    key_id      INT REFERENCES api_keys(id) ON DELETE SET NULL,
    endpoint    TEXT NOT NULL,
    method      TEXT DEFAULT 'GET',
    status_code INT,
    response_ms INT,
    timestamp   TIMESTAMPTZ DEFAULT now()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_skills_day       ON skills(day DESC);
CREATE INDEX IF NOT EXISTS idx_skills_role      ON skills(role);
CREATE INDEX IF NOT EXISTS idx_skills_skill     ON skills(skill);
CREATE INDEX IF NOT EXISTS idx_jobs_scraped     ON jobs(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_logs_key_id      ON api_usage_logs(key_id);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp   ON api_usage_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_keys_hash        ON api_keys(key_hash);

-- Cleanup: run periodically to purge old raw jobs (keeps aggregates intact)
-- The etl/cleanup.py script runs this automatically after each daily scrape.
-- Safe to run manually too.
CREATE OR REPLACE FUNCTION purge_old_jobs(days_to_keep INT DEFAULT 90)
RETURNS INT AS $$
DECLARE deleted INT;
BEGIN
  DELETE FROM jobs WHERE scraped_at < CURRENT_DATE - days_to_keep;
  GET DIAGNOSTICS deleted = ROW_COUNT;
  RETURN deleted;
END;
$$ LANGUAGE plpgsql;
