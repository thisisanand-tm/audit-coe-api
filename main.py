import os
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from db import get_conn

APP_ORIGIN = os.getenv("APP_ORIGIN", "https://audit-coe-poc.vercel.app")

app = FastAPI(title="Audit COE API", version="0.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[APP_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/audit-runs")
def list_audit_runs(limit: int = 50):
    sql = """
      select
        ar.id, ar.name, ar.cohort_type, ar.status, ar.created_at,
        a.name as account_name,
        (select count(*) from tasks t where t.audit_run_id = ar.id) as task_count
      from audit_runs ar
      join accounts a on a.id = ar.account_id
      order by ar.created_at desc
      limit %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

@app.get("/tasks")
def list_tasks(limit: int = 200):
    sql = """
      select
        t.id, t.status, t.due_at, t.created_at,
        ar.name as audit_name,
        a.name as account_name,
        d.name as domain_name,
        u1.email as assignee_email,
        ti.question_text
      from tasks t
      join audit_runs ar on ar.id = t.audit_run_id
      join accounts a on a.id = ar.account_id
      left join domains d on d.id = t.domain_id
      left join users u1 on u1.id = t.assignee_user_id
      left join template_items ti on ti.id = t.template_item_id
      order by t.due_at nulls last, t.created_at desc
      limit %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

@app.post("/tasks/{task_id}/respond")
def respond(task_id: str, payload: dict = Body(...)):
    for k in ["response_status", "actor_email"]:
        if k not in payload:
            raise HTTPException(status_code=400, detail=f"Missing {k}")

    response_status = payload["response_status"]
    comment_text = payload.get("comment_text")
    actor_email = payload["actor_email"]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select id from users where email=%s", (actor_email,))
        actor = cur.fetchone()
        if not actor:
            raise HTTPException(status_code=400, detail="actor_email not found in users")

        cur.execute("select id, audit_run_id from tasks where id=%s", (task_id,))
        t = cur.fetchone()
        if not t:
            raise HTTPException(status_code=404, detail="task not found")

        cur.execute(
            """
            insert into task_responses (task_id, response_status, comment_text, created_by_user_id, is_admin_override)
            values (%s, %s, %s, %s, false)
            returning id
            """
            ,
            (task_id, response_status, comment_text, actor["id"]),
        )
        resp = cur.fetchone()

        cur.execute("update tasks set status='submitted', submitted_at=now() where id=%s", (task_id,))
        conn.commit()

    return {"ok": True, "task_response_id": resp["id"]}
