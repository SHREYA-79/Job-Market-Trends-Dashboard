"""
etl/cleanup.py
Purges raw job listings older than KEEP_DAYS days.
The aggregated `skills` table is NEVER touched — trend data is preserved forever.
Run automatically after each daily ETL via GitHub Actions.

Why this is safe:
  - `jobs` holds raw text (large). We only need it for ~90 days of lookback.
  - `skills` holds pre-aggregated counts by (skill, role, day). This is tiny
    and is the source of truth for all API queries and charts.
  - Deleting old jobs does NOT affect skill counts at all.
"""

import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KEEP_DAYS = int(os.environ.get("CLEANUP_KEEP_DAYS", "90"))


def purge_old_jobs(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM jobs WHERE scraped_at < CURRENT_DATE - %s",
        (KEEP_DAYS,)
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    return deleted


def purge_old_logs(conn, keep_days: int = 365) -> int:
    """Keep a full year of API usage logs for analytics, then prune."""
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM api_usage_logs WHERE timestamp < now() - interval '%s days'",
        (keep_days,)
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    return deleted


def report_storage(conn):
    """Log approximate table sizes for visibility."""
    cur = conn.cursor()
    cur.execute("""
        SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
        FROM pg_class
        WHERE relname IN ('jobs', 'skills', 'api_keys', 'api_usage_logs')
        ORDER BY pg_total_relation_size(oid) DESC
    """)
    rows = cur.fetchall()
    cur.close()
    for table, size in rows:
        log.info(f"  {table:<22} {size}")


def run():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])

    log.info(f"Storage before cleanup:")
    report_storage(conn)

    jobs_deleted = purge_old_jobs(conn)
    log.info(f"Deleted {jobs_deleted} job listings older than {KEEP_DAYS} days")

    logs_deleted = purge_old_logs(conn)
    log.info(f"Deleted {logs_deleted} API log entries older than 365 days")

    log.info(f"Storage after cleanup:")
    report_storage(conn)

    conn.close()
    log.info("Cleanup complete.")


if __name__ == "__main__":
    run()
