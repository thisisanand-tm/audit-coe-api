import os
import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="Audit CoE API")

# CORS (lock to your frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://audit-coe-poc.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug/db")
def debug_db():
    if not DATABASE_URL:
        return {
            "status": "error",
            "detail": "DATABASE_URL not set"
        }

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM accounts;")
                count = cur.fetchone()[0]

        return {
            "status": "success",
            "message": "Connected to Postgres",
            "accounts": count
        }

    except Exception as e:
        return {
            "status": "error",
            "detail": str(e)
        }
