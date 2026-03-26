import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "o2c.db")

def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def execute_query(sql, params=None):
    """Execute read-only SQL queries or execute general queries."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        if sql.strip().upper().startswith(("SELECT", "PRAGMA")):
            return [dict(row) for row in cursor.fetchall()]
        conn.commit()
    finally:
        conn.close()

def get_all_tables():
    """Return list of all tables."""
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    rows = execute_query(sql)
    return [row["name"] for row in rows] if rows else []

def get_schema():
    """Return schema (table -> columns)."""
    tables = get_all_tables()
    schema = {}
    for table in tables:
        columns_info = execute_query(f'PRAGMA table_info("{table}");') or []
        schema[table] = [col["name"] for col in columns_info]
    return schema
