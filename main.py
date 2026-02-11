import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg
from psycopg import sql
from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

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


# ---------------------------------------------------------------------------
# PRE-FLIGHT CHECKLIST (Server-side)
# - DATABASE_URL is set in Render environment
# - Supabase Postgres is reachable from Render (DNS + SSL)
# - Table exists: public.task_responses
# - Table exists: public.tasks
# - task_responses has a FK column pointing to tasks (usually task_id)
# - CORS allowlist includes your Vercel domain + localhost for dev
#
# NO-RUNTIME-EXCEPTIONS RULE
# This module must not raise unhandled exceptions during request handling.
# All endpoints return structured error JSON on failure.
# ---------------------------------------------------------------------------

def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except Exception:
        return False


def _err(code: str, detail: str, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"error": code, "detail": detail}
    if extra:
        out.update(extra)
    return out


def _table_column_meta(conn: psycopg.Connection, table: str, schema: str = "public") -> Dict[str, Dict[str, Any]]:
    """Return column metadata keyed by column_name, using information_schema."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              column_name,
              is_nullable,
              column_default,
              data_type,
              udt_name
            FROM information_schema.columns
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            ORDER BY ordinal_position;
            """,
            {"schema": schema, "table": table},
        )
        meta: Dict[str, Dict[str, Any]] = {}
        for (name, is_nullable, column_default, data_type, udt_name) in cur.fetchall():
            meta[name] = {
                "is_nullable": is_nullable,
                "column_default": column_default,
                "data_type": data_type,
                "udt_name": udt_name,
            }
        return meta


def _pick_first(existing: set, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in existing:
            return c
    return None


def _missing_required_columns(meta: Dict[str, Dict[str, Any]], provided: set) -> List[str]:
    missing = []
    for col, m in meta.items():
        if col in provided:
            continue
        if m.get("is_nullable") == "NO" and not m.get("column_default"):
            # If it's required and there's no default, we must provide it.
            missing.append(col)
    return missing


class TaskResponseIn(BaseModel):
    task_id: str = Field(..., description="Task UUID")
    response_text: Optional[str] = Field(default=None, description="Free-text response")
    response_type: Optional[str] = Field(default=None, description="text|yes_no|number|file_placeholder")
    value_bool: Optional[bool] = Field(default=None)
    value_number: Optional[float] = Field(default=None)
    user_id: Optional[str] = Field(default=None, description="Responder user UUID (optional if DB allows)")


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


@app.post("/task-responses")
def create_task_response(payload: TaskResponseIn = Body(...)) -> Dict[str, Any]:
    """Persist a task response in Postgres.

    Defensive behavior:
    - Validates UUIDs where provided
    - Inserts only into columns that actually exist
    - Detects required NOT NULL columns without defaults and fails with 400
    - Avoids writing task.status when it's USER-DEFINED (enum) to prevent constraint errors
    """
    # Validate UUID-ish inputs (do not trust client).
    if not _is_uuid(payload.task_id):
        return _err("invalid_task_id", "task_id must be a UUID", task_id=payload.task_id)

    if payload.user_id and not _is_uuid(payload.user_id):
        return _err("invalid_user_id", "user_id must be a UUID", user_id=payload.user_id)

    try:
        with _db() as conn:
            # Ensure required tables exist by checking columns.
            tr_meta = _table_column_meta(conn, "task_responses")
            if not tr_meta:
                return _err("missing_table", "Table public.task_responses not found or has no columns")

            t_meta = _table_column_meta(conn, "tasks")
            if not t_meta:
                return _err("missing_table", "Table public.tasks not found or has no columns")

            tr_cols = set(tr_meta.keys())
            t_cols = set(t_meta.keys())

            # Identify column names in task_responses.
            task_fk_col = _pick_first(tr_cols, ["task_id", "tasks_id", "task_uuid"])
            if not task_fk_col:
                return _err("schema_mismatch", "task_responses missing task FK column (expected task_id-like)", columns=sorted(tr_cols))

            responder_col = _pick_first(tr_cols, ["user_id", "responder_user_id", "created_by_user_id", "created_by", "owner_user_id"])
            text_col = _pick_first(tr_cols, ["response_text", "text", "comment", "response", "answer_text"])
            type_col = _pick_first(tr_cols, ["response_type", "type"])
            bool_col = _pick_first(tr_cols, ["value_bool", "bool_value", "response_bool"])
            num_col = _pick_first(tr_cols, ["value_number", "number_value", "response_number"])

            insert_data: Dict[str, Any] = {task_fk_col: payload.task_id}

            if responder_col and payload.user_id:
                insert_data[responder_col] = payload.user_id

            if text_col and payload.response_text is not None:
                insert_data[text_col] = payload.response_text

            if type_col and payload.response_type is not None:
                insert_data[type_col] = payload.response_type

            if bool_col and payload.value_bool is not None:
                insert_data[bool_col] = payload.value_bool

            if num_col and payload.value_number is not None:
                insert_data[num_col] = payload.value_number

            # Check required NOT NULL columns that we did not populate and that have no defaults.
            missing = _missing_required_columns(tr_meta, set(insert_data.keys()))
            # But allow common autogenerated columns even if metadata looks strict.
            # (Some schemas use generated columns or triggers not visible as defaults.)
            allowlist = {"id", "created_at", "updated_at"}
            missing = [c for c in missing if c not in allowlist]
            if missing:
                return _err(
                    "missing_required_fields",
                    "task_responses has required columns without defaults that were not provided",
                    missing_columns=missing,
                    provided_columns=sorted(insert_data.keys()),
                    available_columns=sorted(tr_cols),
                )

            # Confirm task exists.
            if "id" not in t_cols:
                return _err("schema_mismatch", "tasks table missing id column", tasks_columns=sorted(t_cols))

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM tasks WHERE id = %(task_id)s LIMIT 1;",
                    {"task_id": payload.task_id},
                )
                if cur.fetchone() is None:
                    return _err("task_not_found", "No task found for task_id", task_id=payload.task_id)

            # Insert task response.
            insert_cols = list(insert_data.keys())
            insert_sql_cols = sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols)
            insert_sql_vals = sql.SQL(", ").join(sql.Placeholder(c) for c in insert_cols)

            returning_cols = [c for c in ["id", "task_id", "created_at"] if c in tr_cols]
            returning_sql = sql.SQL(", ").join(sql.Identifier(c) for c in returning_cols) if returning_cols else sql.SQL("")

            insert_stmt = sql.SQL("""
                INSERT INTO {table} ({cols})
                VALUES ({vals})
                {returning};
            """).format(
                table=sql.Identifier("task_responses"),
                cols=insert_sql_cols,
                vals=insert_sql_vals,
                returning=sql.SQL("RETURNING ") + returning_sql if returning_cols else sql.SQL(""),
            )

            out_row: Dict[str, Any] = {}
            with conn.cursor() as cur:
                cur.execute(insert_stmt, insert_data)
                if returning_cols:
                    rec = cur.fetchone()
                    if rec is not None:
                        out_row = dict(zip(returning_cols, rec))

            # Best-effort task update (defensive): set responded_at if present.
            update_fields: Dict[str, Any] = {}
            if "responded_at" in t_cols:
                update_fields["responded_at"] = datetime.utcnow()

            # Only set status if it's not USER-DEFINED (enum-like).
            if "status" in t_cols:
                status_meta = t_meta.get("status") or {}
                if status_meta.get("data_type") != "USER-DEFINED":
                    update_fields["status"] = "responded"

            if update_fields:
                set_sql = sql.SQL(", ").join(
                    sql.SQL("{col} = {ph}").format(col=sql.Identifier(k), ph=sql.Placeholder(k))
                    for k in update_fields.keys()
                )
                update_stmt = sql.SQL("UPDATE {table} SET {set_sql} WHERE id = %(task_id)s;").format(
                    table=sql.Identifier("tasks"),
                    set_sql=set_sql,
                )
                params = dict(update_fields)
                params["task_id"] = payload.task_id
                with conn.cursor() as cur:
                    cur.execute(update_stmt, params)

            # All good.
            return {"status": "saved", "task_id": payload.task_id, **out_row}

    except Exception as e:
        return _err("task_response_create_failed", str(e))


# ---------------------------------------------------------------------------
# SELF-TEST BLOCK (runs only when executing this file directly)
# Usage:
#   DATABASE_URL=... python main.py
# Exits:
#   0 = OK, 1 = failure
# ---------------------------------------------------------------------------

def _self_test() -> int:
    """Basic runtime safety checks for DB connectivity and required tables."""
    if not DATABASE_URL:
        print("SELF-TEST FAIL: DATABASE_URL not set")
        return 1
    try:
        with _db() as conn:
            for tbl in ["tasks", "task_responses"]:
                cols = _table_columns(conn, tbl)
                if not cols:
                    print(f"SELF-TEST FAIL: table {tbl} missing or has no columns")
                    return 1
            print("SELF-TEST OK: DB reachable; tasks + task_responses present")
            return 0
    except Exception as e:
        print(f"SELF-TEST FAIL: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_self_test())
