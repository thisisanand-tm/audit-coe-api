import os
import psycopg
from psycopg.rows import dict_row

def get_conn():
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")

    # Supabase requires SSL
    if "sslmode=" not in dsn:
        joiner = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{joiner}sslmode=require"

    return psycopg.connect(dsn, row_factory=dict_row)
