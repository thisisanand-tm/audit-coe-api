import os
from typing import Any, Dict, List, Optional

import psycopg
from psycopg import sql
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="Audit CoE API")

# CORS:
# - Vercel production frontend
# - Local development (Create React App default port 3000)
# - Optional common dev ports (5173/4173) if you ever switch tooling
ALLOWED_ORIGINS = [
    "https://audit-coe-poc.vercel.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _db() -> psycopg.Connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(DATABASE_URL)


def _table_columns(conn: psycopg.Connection, table: str, schema: str = "public") -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            ORDER BY ordinal_position;
            """,
            {"schema": schema, "table": table},
        )
        return [r[0] for r in cur.fetchall()]


def _select_intersection(existing: List[str], desired: List[str]) -> List[str]:
    existing_set = set(existing)
    return [c for c in desired if c in existing_set]


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


@app.get("/debug/columns")
def debug_columns(
    table: str = Query(..., description="Table name (e.g., tasks, audit_runs)"),
    schema: str = Query(default="public", description="Schema name (default: public)"),
) -> Dict[str, Any]:
    try:
        with _db() as conn:
            cols = _table_columns(conn, table=table, schema=schema)
        return {"schema": schema, "table": table, "columns": cols, "count": len(cols)}
    except Exception as e:
        return {"error": "debug_columns_failed", "detail": str(e)}


@app.get("/audit-runs")
def list_audit_runs(
    account_id: Optional[str] = Query(default=None, description="Filter by account UUID"),
    limit: int = Query(default=50, ge=1, le=500),
) -> Dict[str, Any]:
    desired = ["id", "account_id", "template_id", "status", "started_at", "due_at", "created_at"]
    try:
        with _db() as conn:
            cols = _table_columns(conn, "audit_runs")
            select_cols = _select_intersection(cols, desired)
            if not select_cols:
                return {"error": "audit_runs_no_known_columns", "detail": f"Found columns: {cols}"}

            select_sql = sql.SQL(", ").join(sql.Identifier(c) for c in select_cols)

            where_parts = []
            params: Dict[str, Any] = {"limit": limit}
            if account_id and "account_id" in cols:
                where_parts.append(sql.SQL("account_id = %(account_id)s"))
                params["account_id"] = account_id

            where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_parts) if where_parts else sql.SQL("")

            order_col = "created_at" if "created_at" in cols else select_cols[0]

            query = sql.SQL("""
                SELECT {select_cols}
                FROM {table}
                {where_clause}
                ORDER BY {order_col} DESC
                LIMIT %(limit)s;
            """).format(
                select_cols=select_sql,
                table=sql.Identifier("audit_runs"),
                where_clause=where_sql,
                order_col=sql.Identifier(order_col),
            )

            with conn.cursor() as cur:
                cur.execute(query, params)
                out_cols = [d[0] for d in cur.description]
                rows = [dict(zip(out_cols, r)) for r in cur.fetchall()]

        return {"items": rows, "count": len(rows), "selected_columns": select_cols}
    except Exception as e:
        return {"error": "audit_runs_query_failed", "detail": str(e)}


@app.get("/tasks")
def list_tasks(
    audit_run_id: Optional[str] = Query(default=None, description="Filter by audit_run UUID"),
    account_id: Optional[str] = Query(default=None, description="Filter by account UUID (via join if needed)"),
    status: Optional[str] = Query(default=None, description="Filter by task status"),
    limit: int = Query(default=200, ge=1, le=1000),
) -> Dict[str, Any]:
    desired = [
        "id",
        "audit_run_id",
        "template_task_id",
        "title",
        "description",
        "status",
        "owner_user_id",
        "due_at",
        "created_at",
    ]

    try:
        with _db() as conn:
            task_cols = _table_columns(conn, "tasks")
            run_cols = _table_columns(conn, "audit_runs")

            select_cols = _select_intersection(task_cols, desired)
            if not select_cols:
                return {"error": "tasks_no_known_columns", "detail": f"Found columns: {task_cols}"}

            params: Dict[str, Any] = {"limit": limit}
            where_parts = []
            join_sql = sql.SQL("")
            from_sql = sql.SQL("{tasks} t").format(tasks=sql.Identifier("tasks"))

            if audit_run_id and "audit_run_id" in task_cols:
                where_parts.append(sql.SQL("t.audit_run_id = %(audit_run_id)s"))
                params["audit_run_id"] = audit_run_id

            if status and "status" in task_cols:
                where_parts.append(sql.SQL("t.status = %(status)s"))
                params["status"] = status

            if account_id:
                if "account_id" in task_cols:
                    where_parts.append(sql.SQL("t.account_id = %(account_id)s"))
                    params["account_id"] = account_id
                else:
                    if "audit_run_id" in task_cols and "id" in run_cols and "account_id" in run_cols:
                        join_sql = sql.SQL("JOIN {runs} r ON r.id = t.audit_run_id").format(
                            runs=sql.Identifier("audit_runs")
                        )
                        where_parts.append(sql.SQL("r.account_id = %(account_id)s"))
                        params["account_id"] = account_id
                    else:
                        return {
                            "error": "tasks_cannot_filter_account_id",
                            "detail": "tasks.account_id missing and cannot join to audit_runs (missing required columns).",
                            "tasks_columns": task_cols,
                            "audit_runs_columns": run_cols,
                        }

            select_sql = sql.SQL(", ").join(sql.SQL("t.") + sql.Identifier(c) for c in select_cols)
            where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_parts) if where_parts else sql.SQL("")
            order_col = "created_at" if "created_at" in task_cols else select_cols[0]

            query = sql.SQL("""
                SELECT {select_cols}
                FROM {from_clause}
                {join_clause}
                {where_clause}
                ORDER BY t.{order_col} DESC
                LIMIT %(limit)s;
            """).format(
                select_cols=select_sql,
                from_clause=from_sql,
                join_clause=join_sql,
                where_clause=where_sql,
                order_col=sql.Identifier(order_col),
            )

            with conn.cursor() as cur:
                cur.execute(query, params)
                out_cols = [d[0] for d in cur.description]
                rows = [dict(zip(out_cols, r)) for r in cur.fetchall()]

        return {"items": rows, "count": len(rows), "selected_columns": select_cols}
    except Exception as e:
        return {"error": "tasks_query_failed", "detail": str(e)}
