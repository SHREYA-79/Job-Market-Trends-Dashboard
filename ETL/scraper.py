"""
etl/scraper.py
Job Market Trends - ETL Pipeline
Scrapes RemoteOK (free, no key needed) + optionally Adzuna.
Run weekly via cron or GitHub Actions.
"""

import os
import re
import time
import hashlib
import logging
from datetime import date, timedelta
from collections import defaultdict

import httpx
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Skills to track ───────────────────────────────────────────────────────────
SKILLS = [
    "python", "sql", "spark", "kafka", "airflow", "dbt", "aws", "gcp", "azure",
    "docker", "kubernetes", "terraform", "pandas", "numpy", "scikit-learn",
    "tensorflow", "pytorch", "react", "typescript", "javascript", "java",
    "scala", "go", "rust", "postgres", "mysql", "mongodb", "redis", "elasticsearch",
    "fastapi", "django", "flask", "node", "graphql", "tableau", "power bi",
    "looker", "snowflake", "databricks", "mlflow", "pyspark", "hadoop",
]

# Role categories (keyword → canonical role name)
ROLE_PATTERNS = {
    "data engineer":      r"data engineer|data engineering",
    "ml engineer":        r"machine learning|ml engineer|mlops|ai engineer",
    "data scientist":     r"data scientist|data science",
    "data analyst":       r"data analyst|business analyst|analytics",
    "backend engineer":   r"backend|back.end|software engineer|swe",
    "frontend engineer":  r"frontend|front.end|ui engineer",
    "devops":             r"devops|platform engineer|sre|infrastructure",
    "fullstack":          r"full.?stack",
}


def classify_role(title: str) -> str:
    title_lower = title.lower()
    for role, pattern in ROLE_PATTERNS.items():
        if re.search(pattern, title_lower):
            return role
    return "other"


def extract_skills(text: str) -> list[str]:
    text_lower = text.lower()
    found = []
    for skill in SKILLS:
        # word boundary match
        if re.search(r"\b" + re.escape(skill) + r"\b", text_lower):
            found.append(skill)
    return found


def scrape_remoteok() -> list[dict]:
    """RemoteOK public API — no key required."""
    log.info("Scraping RemoteOK...")
    try:
        r = httpx.get(
            "https://remoteok.com/api",
            headers={"User-Agent": "job-market-trends-bot/1.0"},
            timeout=30,
            follow_redirects=True,
        )
        r.raise_for_status()
        data = r.json()
        jobs = [j for j in data if isinstance(j, dict) and "position" in j]
        log.info(f"RemoteOK: {len(jobs)} listings")
        return jobs
    except Exception as e:
        log.error(f"RemoteOK scrape failed: {e}")
        return []


def normalize_remoteok(raw: list[dict]) -> list[dict]:
    normalized = []
    for j in raw:
        normalized.append({
            "external_id": f"remoteok-{j.get('id', '')}",
            "title":       j.get("position", ""),
            "company":     j.get("company", ""),
            "location":    j.get("location", "Remote"),
            "remote":      True,
            "salary_min":  j.get("salary_min") or None,
            "salary_max":  j.get("salary_max") or None,
            "description": j.get("description", "") + " " + " ".join(j.get("tags", [])),
            "source":      "remoteok",
            "url":         j.get("url", ""),
        })
    return normalized


def upsert_jobs(conn, jobs: list[dict]):
    if not jobs:
        return
    cur = conn.cursor()
    sql = """
        INSERT INTO jobs (external_id, title, company, location, remote,
                          salary_min, salary_max, description, source, url)
        VALUES %s
        ON CONFLICT (external_id) DO NOTHING
    """
    rows = [(
        j["external_id"], j["title"], j["company"], j["location"], j["remote"],
        j["salary_min"], j["salary_max"], j["description"], j["source"], j["url"]
    ) for j in jobs]
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    log.info(f"Upserted {len(rows)} jobs")


def aggregate_skills(jobs: list[dict]) -> dict:
    """Returns {(skill, role): count}"""
    counts = defaultdict(int)
    for job in jobs:
        role = classify_role(job["title"])
        skills = extract_skills(job["description"])
        for skill in skills:
            counts[(skill, role)] += 1
    return counts


def upsert_skill_counts(conn, counts: dict):
    """
    Writes daily skill counts. Uses INSERT ... ON CONFLICT DO UPDATE with
    a SET (not increment), so re-running the ETL on the same day is idempotent
    -- it overwrites today's row rather than doubling it.
    """
    if not counts:
        return
    today = date.today()
    cur = conn.cursor()
    sql = """
        INSERT INTO skills (skill, role, count, day)
        VALUES %s
        ON CONFLICT (skill, role, day)
        DO UPDATE SET count = EXCLUDED.count
    """
    rows = [(skill, role, cnt, today) for (skill, role), cnt in counts.items()]
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    log.info(f"Upserted {len(rows)} skill rows for {today}")


def run():
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)

    # Scrape
    raw = scrape_remoteok()
    jobs = normalize_remoteok(raw)

    # Store raw jobs
    upsert_jobs(conn, jobs)

    # Aggregate & store skills
    counts = aggregate_skills(jobs)
    upsert_skill_counts(conn, counts)

    conn.close()
    log.info("ETL complete.")


if __name__ == "__main__":
    run()
