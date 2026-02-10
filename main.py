import os
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="Audit CoE API")

# CORS locked to your Vercel frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://audit-coe-poc.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _db() -> psycopg.Connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    # Supabase requires TLS; sslmode=require is commonly embedded in the URL already.
    return psycopg.connect(DATABASE_URL)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/debug/db")
def debug_db() -> Dict[str, Any]:
    if not DATABASE_URL:
        return {"status": "error", "detail": "DATABASE_URL not set"}

    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM accounts;")
                accounts = cur.fetchone()[0]
        return {"status": "success", "message": "Connected to Postgres", "accounts": accounts}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/audit-runs")
def list_audit_runs(
    account_id: Optional[str] = Query(default=None, description="Filter by account UUID"),
    limit: int = Query(default=50, ge=1, le=500),
) -> Dict[str, Any]:
    """
    Returns audit runs. Safe, minimal fields. If your DB uses a different table/column naming,
    the error will surface clearly.
    """
    sql = """
        SELECT
            id,
            account_id,
            template_id,
            status,
            started_at,
            due_at,
            created_at
        FROM audit_runs
        WHERE (%(account_id)s IS NULL OR account_id = %(account_id)s)
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """
    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"account_id": account_id, "limit": limit})
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return {"items": rows, "count": len(rows)}
    except Exception as e:
        # Keep the payload JSON-friendly for quick debugging
        return {"error": "audit_runs_query_failed", "detail": str(e)}


@app.get("/tasks")
def list_tasks(
    audit_run_id: Optional[str] = Query(default=None, description="Filter by audit_run UUID"),
    account_id: Optional[str] = Query(default=None, description="Filter by account UUID"),
    status: Optional[str] = Query(default=None, description="Filter by task status"),
    limit: int = Query(default=200, ge=1, le=1000),
) -> Dict[str, Any]:
    """
    Returns tasks. Filters are optional to keep the UI wiring simple.
    """
    sql = """
        SELECT
            id,
            audit_run_id,
            account_id,
            template_task_id,
            title,
            description,
            status,
            owner_user_id,
            due_at,
            created_at
        FROM tasks
        WHERE
            (%(audit_run_id)s IS NULL OR audit_run_id = %(audit_run_id)s)
            AND (%(account_id)s IS NULL OR account_id = %(account_id)s)
            AND (%(status)s IS NULL OR status = %(status)s)
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """
    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "audit_run_id": audit_run_id,
                        "account_id": account_id,
                        "status": status,
                        "limit": limit,
                    },
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return {"items": rows, "count": len(rows)}
    except Exception as e:
        return {"error": "tasks_query_failed", "detail": str(e)}
