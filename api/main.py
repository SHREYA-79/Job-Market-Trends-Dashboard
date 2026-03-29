"""
api/main.py
Job Market Trends — Public REST API
FastAPI + PostgreSQL + API key auth + usage logging
"""

import os
import time
import hashlib
import secrets
import smtplib
from email.message import EmailMessage
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, Header, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

# ── DB Connection Pool ────────────────────────────────────────────────────────

pool: psycopg2.pool.SimpleConnectionPool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = psycopg2.pool.SimpleConnectionPool(1, 10, os.environ["DATABASE_URL"])
    yield
    pool.closeall()


def get_conn():
    return pool.getconn()

def release_conn(conn):
    pool.putconn(conn)


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Job Market Trends API",
    description="Track in-demand skills, salaries, and hiring trends from live job data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Auth + Usage Logging ──────────────────────────────────────────────────────

def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def verify_api_key(x_api_key: str = Header(..., description="Your API key")):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM api_keys WHERE key_hash = %s AND is_active = true",
            (hash_key(x_api_key),)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid or inactive API key.")
        # Update last_used + count
        cur.execute(
            "UPDATE api_keys SET last_used = now(), request_count = request_count + 1 WHERE id = %s",
            (row[0],)
        )
        conn.commit()
        cur.close()
        return row[0]
    finally:
        release_conn(conn)


def log_request(key_id: int, endpoint: str, method: str, status: int, ms: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO api_usage_logs (key_id, endpoint, method, status_code, response_ms) VALUES (%s,%s,%s,%s,%s)",
            (key_id, endpoint, method, status, ms)
        )
        conn.commit()
        cur.close()
    finally:
        release_conn(conn)


# ── Models ────────────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    name: Optional[str] = None


# ── Routes — Public ───────────────────────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    return {
        "name": "Job Market Trends API",
        "version": "1.0.0",
        "docs": "/docs",
        "register": "/v1/keys/register",
    }


@app.get("/health", tags=["Info"])
def health():
    return {"status": "ok"}


@app.post("/v1/keys/register", tags=["Auth"])
def register_key(body: RegisterRequest):
    """Register for a free API key. Returns the key once — save it."""
    raw_key = secrets.token_urlsafe(32)
    kh = hash_key(raw_key)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO api_keys (key_hash, email, name) VALUES (%s, %s, %s)",
            (kh, body.email, body.name)
        )
        conn.commit()
        cur.close()
    finally:
        release_conn(conn)

    # Optional: send email (configure SMTP env vars)
    _try_send_welcome_email(body.email, body.name, raw_key)

    return {
        "api_key": raw_key,
        "message": "Save this key — it won't be shown again.",
        "docs": "https://your-app.onrender.com/docs",
    }


# ── Routes — Authenticated ────────────────────────────────────────────────────

@app.get("/v1/skills/trending", tags=["Skills"])
def trending_skills(
    role: Optional[str] = Query(None, description="Filter by role, e.g. 'data engineer'"),
    weeks: int = Query(4, ge=1, le=52, description="Lookback window in days"),
    limit: int = Query(15, ge=1, le=50),
    key_id: int = Depends(verify_api_key),
):
    """Top skills by total mention count across recent job listings."""
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        if role:
            cur.execute("""
                SELECT skill, SUM(count) AS total
                FROM skills
                WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
                  AND role ILIKE %s
                GROUP BY skill
                ORDER BY total DESC
                LIMIT %s
            """, (weeks, f"%{role}%", limit))
        else:
            cur.execute("""
                SELECT skill, SUM(count) AS total
                FROM skills
                WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
                GROUP BY skill
                ORDER BY total DESC
                LIMIT %s
            """, (weeks, limit))
        rows = cur.fetchall()
        cur.close()
    finally:
        release_conn(conn)

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/skills/trending", "GET", 200, ms)
    return {"data": [{"skill": r[0], "count": int(r[1])} for r in rows], "params": {"role": role, "weeks": weeks}}


@app.get("/v1/skills/by-role", tags=["Skills"])
def skills_by_role(
    weeks: int = Query(4, ge=1, le=52),
    limit: int = Query(10, ge=1, le=30),
    key_id: int = Depends(verify_api_key),
):
    """Top skills broken down by role category."""
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT role, skill, SUM(count) AS total
            FROM skills
            WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY role, skill
            ORDER BY role, total DESC
        """, (weeks,))
        rows = cur.fetchall()
        cur.close()
    finally:
        release_conn(conn)

    # Group into dict
    result = {}
    counts = {}
    for role, skill, total in rows:
        if role not in counts:
            counts[role] = 0
        if counts[role] < limit:
            result.setdefault(role, []).append({"skill": skill, "count": int(total)})
            counts[role] += 1

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/skills/by-role", "GET", 200, ms)
    return {"data": result, "weeks": weeks}


@app.get("/v1/jobs/count", tags=["Jobs"])
def job_count(
    skill: Optional[str] = Query(None, description="Filter by skill keyword"),
    role:  Optional[str] = Query(None, description="Filter by role keyword"),
    days:  int = Query(30, ge=1, le=365),
    key_id: int = Depends(verify_api_key),
):
    """Count of job listings matching filters over the past N days."""
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        filters = ["scraped_at >= CURRENT_DATE - %s"]
        params: list = [days]
        if skill:
            filters.append("description ILIKE %s")
            params.append(f"%{skill}%")
        if role:
            filters.append("title ILIKE %s")
            params.append(f"%{role}%")
        where = " AND ".join(filters)
        cur.execute(f"SELECT COUNT(*) FROM jobs WHERE {where}", params)
        count = cur.fetchone()[0]
        cur.close()
    finally:
        release_conn(conn)

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/jobs/count", "GET", 200, ms)
    return {"count": count, "params": {"skill": skill, "role": role, "days": days}}


@app.get("/v1/jobs/recent", tags=["Jobs"])
def recent_jobs(
    role:  Optional[str] = Query(None),
    skill: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    key_id: int = Depends(verify_api_key),
):
    """Recent job listings with optional filters."""
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        filters = []
        params: list = []
        if role:
            filters.append("title ILIKE %s")
            params.append(f"%{role}%")
        if skill:
            filters.append("description ILIKE %s")
            params.append(f"%{skill}%")
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        cur.execute(f"""
            SELECT title, company, location, remote, salary_min, salary_max, url, scraped_at
            FROM jobs {where}
            ORDER BY scraped_at DESC
            LIMIT %s
        """, params)
        cols = ["title", "company", "location", "remote", "salary_min", "salary_max", "url", "scraped_at"]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        release_conn(conn)

    # Make dates serializable
    for r in rows:
        r["scraped_at"] = str(r["scraped_at"])

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/jobs/recent", "GET", 200, ms)
    return {"data": rows}


@app.get("/v1/trends/weekly", tags=["Trends"])
def weekly_trends(
    skill: str = Query(..., description="Skill to track over time"),
    weeks: int = Query(12, ge=2, le=52),
    key_id: int = Depends(verify_api_key),
):
    """Weekly mention count for a specific skill — great for charting trends."""
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT day, SUM(count) AS total
            FROM skills
            WHERE skill ILIKE %s
              AND day >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY day
            ORDER BY day ASC
        """, (f"%{skill}%", weeks))
        rows = cur.fetchall()
        cur.close()
    finally:
        release_conn(conn)

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/trends/weekly", "GET", 200, ms)
    return {
        "skill": skill,
        "data": [{"day": str(r[0]), "count": int(r[1])} for r in rows],
    }


# ── Admin (env-var protected) ─────────────────────────────────────────────────

@app.get("/admin/stats", tags=["Admin"])
def admin_stats(x_admin_key: str = Header(...)):
    if x_admin_key != os.environ.get("ADMIN_KEY", "changeme"):
        raise HTTPException(status_code=403, detail="Forbidden")

    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM api_keys WHERE is_active = true")
        total_keys = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM api_usage_logs")
        total_requests = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM api_usage_logs WHERE timestamp > now() - interval '24 hours'")
        requests_24h = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cur.fetchone()[0]

        cur.execute("""
            SELECT DATE(timestamp), COUNT(*) 
            FROM api_usage_logs 
            WHERE timestamp > now() - interval '30 days'
            GROUP BY DATE(timestamp) 
            ORDER BY DATE(timestamp)
        """)
        daily = [{"date": str(r[0]), "requests": r[1]} for r in cur.fetchall()]

        cur.execute("""
            SELECT endpoint, COUNT(*) as cnt
            FROM api_usage_logs
            GROUP BY endpoint ORDER BY cnt DESC LIMIT 10
        """)
        top_endpoints = [{"endpoint": r[0], "count": r[1]} for r in cur.fetchall()]

        cur.execute("SELECT ROUND(AVG(response_ms)) FROM api_usage_logs WHERE timestamp > now() - interval '24 hours'")
        avg_ms = cur.fetchone()[0] or 0

        cur.close()
    finally:
        release_conn(conn)

    return {
        "api_keys": total_keys,
        "total_requests": total_requests,
        "requests_last_24h": requests_24h,
        "total_jobs_indexed": total_jobs,
        "avg_response_ms_24h": int(avg_ms),
        "daily_requests": daily,
        "top_endpoints": top_endpoints,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _try_send_welcome_email(email: str, name: str, api_key: str):
    smtp_host = os.environ.get("SMTP_HOST")
    if not smtp_host:
        return  # Email not configured — skip silently
    try:
        msg = EmailMessage()
        msg["Subject"] = "Your Job Market Trends API key"
        msg["From"]    = os.environ.get("SMTP_FROM", "noreply@example.com")
        msg["To"]      = email
        msg.set_content(f"""Hi {name or 'there'},

Your API key: {api_key}

Save this — it won't be shown again.

Docs: https://your-app.onrender.com/docs

Happy building!
""")
        with smtplib.SMTP(smtp_host, int(os.environ.get("SMTP_PORT", 587))) as s:
            s.starttls()
            s.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
            s.send_message(msg)
    except Exception as e:
        print(f"Email send failed: {e}")
