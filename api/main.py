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
from fastapi import FastAPI, Header, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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


def verify_api_key(x_api_key: str = Header(...)):
    # 🔥 FIX: remove spaces/newlines
    clean_key = x_api_key.strip()
    hashed = hash_key(clean_key)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM api_keys WHERE key_hash = %s AND is_active = true",
            (hashed,)
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=401, detail="Invalid or inactive API key.")

        # update usage
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

    return {
        "api_key": raw_key,
        "message": "Save this key — it won't be shown again.",
        "docs": "https://your-app.onrender.com/docs",
    }


# ── Routes — Authenticated ────────────────────────────────────────────────────

@app.get("/v1/skills/trending", tags=["Skills"])
def trending_skills(
    role: Optional[str] = Query(None),
    weeks: int = Query(4, ge=1, le=52),
    limit: int = Query(15, ge=1, le=50),
    key_id: int = Depends(verify_api_key),
):
    t0 = time.time()
    conn = get_conn()
    try:
        cur = conn.cursor()
        if role:
            cur.execute("""
                SELECT skill, SUM(count)
                FROM skills
                WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
                  AND role ILIKE %s
                GROUP BY skill
                ORDER BY SUM(count) DESC
                LIMIT %s
            """, (weeks, f"%{role}%", limit))
        else:
            cur.execute("""
                SELECT skill, SUM(count)
                FROM skills
                WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
                GROUP BY skill
                ORDER BY SUM(count) DESC
                LIMIT %s
            """, (weeks, limit))

        rows = cur.fetchall()
        cur.close()
    finally:
        release_conn(conn)

    ms = int((time.time() - t0) * 1000)
    log_request(key_id, "/v1/skills/trending", "GET", 200, ms)

    return {
        "data": [{"skill": r[0], "count": int(r[1])} for r in rows]
    }


# ── Helper ────────────────────────────────────────────────────────────────────

def _try_send_welcome_email(email: str, name: str, api_key: str):
    smtp_host = os.environ.get("SMTP_HOST")
    if not smtp_host:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = "Your API key"
        msg["From"] = os.environ.get("SMTP_FROM", "noreply@example.com")
        msg["To"] = email
        msg.set_content(f"Your API key: {api_key}")

        with smtplib.SMTP(smtp_host, int(os.environ.get("SMTP_PORT", 587))) as s:
            s.starttls()
            s.login(os.environ["SMTP_USER"], os.environ["SMTP
