import os
import json
import csv
import re
import sqlite3
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
# __file__  →  backend/services/db.py
# one dirname  →  backend/services/
# two dirnames →  backend/              ← we want this
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "sap-o2c-data")
DB_PATH   = os.path.join(BASE_DIR, "o2c.db")


# ── Connection ────────────────────────────────────────────────────────────────
def _get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ── Query helper ──────────────────────────────────────────────────────────────
def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Execute a SQL statement and return results as a list of dicts."""
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP")):
            conn.commit()
            return [{"status": "success", "rows_affected": cursor.rowcount}]

        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return [{"error": f"Database error: {e}"}]
    except Exception as e:
        logger.error(f"Unexpected DB error: {e}")
        return [{"error": f"Unexpected database error: {e}"}]
    finally:
        if "conn" in locals() and conn:
            conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_name(name: str) -> str:
    """Convert camelCase / special-char identifiers to safe snake_case."""
    s = re.sub(r"([A-Z])", r"_\1", name).lower().lstrip("_")
    return re.sub(r"[^a-z0-9_]", "_", s)


def _dedup_columns(columns: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            result.append(col)
    return result


def _read_records(filepath: str):
    """
    Yield (raw_columns, safe_columns, rows) from a JSONL or CSV file.
    Returns None if the file cannot be parsed.
    """
    ext = os.path.splitext(filepath)[1].lower()
    records: List[Dict[str, Any]] = []

    try:
        if ext == ".jsonl":
            with open(filepath, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        elif ext == ".csv":
            with open(filepath, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    records.append(dict(row))
        else:
            return None

    except Exception as e:
        logger.error(f"[seed] Cannot read '{filepath}': {e}")
        return None

    if not records:
        return None

    raw_columns  = list(records[0].keys())
    safe_columns = _dedup_columns([_safe_name(c) for c in raw_columns])
    return raw_columns, safe_columns, records


# ── Seed ──────────────────────────────────────────────────────────────────────
def seed_database() -> None:
    """
    Recursively scan DATA_PATH for .jsonl / .csv files, create one SQLite
    table per entity folder, and insert all rows.

    Idempotent: skips any table that already contains data.
    Never crashes the server — all errors are logged and skipped.
    """
    logger.info("Seeding database...")
    logger.info(f"[seed] BASE_DIR  = {BASE_DIR}")
    logger.info(f"[seed] DATA_PATH = {DATA_PATH}")
    logger.info(f"[seed] DB_PATH   = {DB_PATH}")

    if not os.path.isdir(DATA_PATH):
        logger.error(
            f"[seed] Data path not found: {DATA_PATH}  "
            f"(contents of BASE_DIR: {os.listdir(BASE_DIR) if os.path.isdir(BASE_DIR) else 'N/A'})"
        )
        return

    # Collect all data files
    all_files: List[str] = []
    for root, _dirs, files in os.walk(DATA_PATH):
        for fname in sorted(files):
            if fname.endswith((".jsonl", ".csv")):
                all_files.append(os.path.join(root, fname))

    logger.info(f"[seed] Detected {len(all_files)} data file(s): {[os.path.basename(f) for f in all_files]}")

    if not all_files:
        logger.warning("[seed] No data files found — nothing to seed.")
        return

    conn = _get_connection()
    try:
        # Group files by entity (parent folder name)
        entities: Dict[str, List[str]] = {}
        for fpath in all_files:
            folder = os.path.basename(os.path.dirname(fpath))
            table  = _safe_name(folder)
            entities.setdefault(table, []).append(fpath)

        for table_name, file_list in sorted(entities.items()):
            # ── Discover schema from first readable file ───────────────────
            schema = None
            for fpath in file_list:
                schema = _read_records(fpath)
                if schema:
                    break
            if not schema:
                logger.warning(f"[seed] No readable records for '{table_name}', skipping.")
                continue

            raw_columns, safe_columns, _ = schema

            # ── Create table ───────────────────────────────────────────────
            col_defs = ", ".join(f'"{c}" TEXT' for c in safe_columns)
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
            conn.commit()
            logger.info(f"[seed] Table created / verified: {table_name}")

            # ── Skip if already populated ─────────────────────────────────
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            if row_count > 0:
                logger.info(f"[seed] '{table_name}' already has {row_count} rows — skipping.")
                continue

            # ── Insert from every file for this entity ─────────────────────
            placeholders = ", ".join("?" for _ in safe_columns)
            quoted_cols  = ", ".join(f'"{c}"' for c in safe_columns)
            insert_sql   = f'INSERT INTO "{table_name}" ({quoted_cols}) VALUES ({placeholders})'

            total = 0
            for fpath in file_list:
                logger.info(f"[seed] Loading file: {os.path.basename(fpath)}")
                result = _read_records(fpath)
                if not result:
                    continue
                _, _, records = result

                batch: List[tuple] = []
                for rec in records:
                    values = tuple(
                        str(rec.get(raw, "") if rec.get(raw) is not None else "")
                        for raw in raw_columns
                    )
                    batch.append(values)

                    if len(batch) >= 500:
                        conn.executemany(insert_sql, batch)
                        total += len(batch)
                        batch = []

                if batch:
                    conn.executemany(insert_sql, batch)
                    total += len(batch)

                conn.commit()

            logger.info(f"[seed] Inserted {total} rows into '{table_name}'")

    except Exception as e:
        logger.error(f"[seed] Fatal error during seeding: {e}")
    finally:
        conn.close()

    logger.info("Database seeding complete.")
