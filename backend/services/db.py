import os
import json
import sqlite3
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
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


# ── Safe column name ───────────────────────────────────────────────────────────
def _safe_col(name: str) -> str:
    """Convert a camelCase/special-char column name to a safe snake_case identifier."""
    import re
    # Insert underscore before uppercase letters, then lowercase everything
    s = re.sub(r"([A-Z])", r"_\1", name).lower().lstrip("_")
    # Replace any remaining non-alphanumeric chars (except _) with _
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s


# ── Seed ──────────────────────────────────────────────────────────────────────
def seed_database() -> None:
    """
    Scan backend/data/sap-o2c-data/, read every JSONL file found in
    each entity sub-folder, create a SQLite table named after the folder,
    and insert all rows. Skips tables that already contain data so the
    function is safe to call on every server startup.
    """
    if not os.path.isdir(DATA_PATH):
        logger.warning(f"Data path not found: {DATA_PATH}. Skipping seed.")
        return

    conn = _get_connection()

    try:
        for entity_dir in sorted(os.listdir(DATA_PATH)):
            entity_path = os.path.join(DATA_PATH, entity_dir)
            if not os.path.isdir(entity_path):
                continue

            table_name = entity_dir.replace("-", "_").lower()

            # Collect all JSONL files for this entity
            jsonl_files = sorted(
                os.path.join(entity_path, f)
                for f in os.listdir(entity_path)
                if f.endswith(".jsonl")
            )
            if not jsonl_files:
                logger.info(f"[seed] No JSONL files in '{entity_dir}', skipping.")
                continue

            # ── Parse first valid record to discover schema ────────────────
            first_record: Dict[str, Any] | None = None
            for fpath in jsonl_files:
                try:
                    with open(fpath, "r", encoding="utf-8") as fh:
                        for line in fh:
                            line = line.strip()
                            if line:
                                first_record = json.loads(line)
                                break
                except Exception as e:
                    logger.error(f"[seed] Error reading '{fpath}': {e}")
                if first_record:
                    break

            if not first_record:
                logger.warning(f"[seed] No valid records in '{entity_dir}', skipping.")
                continue

            raw_columns   = list(first_record.keys())
            safe_columns  = [_safe_col(c) for c in raw_columns]

            # ── Ensure unique column names (dedup with suffix) ─────────────
            seen: Dict[str, int] = {}
            deduped: List[str] = []
            for col in safe_columns:
                if col in seen:
                    seen[col] += 1
                    deduped.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    deduped.append(col)
            safe_columns = deduped

            # ── Create table if needed ─────────────────────────────────────
            col_defs = ", ".join(f'"{c}" TEXT' for c in safe_columns)
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})'
            )
            conn.commit()

            # ── Skip if already populated ─────────────────────────────────
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]
            if count > 0:
                logger.info(
                    f"[seed] Table '{table_name}' already has {count} rows — skipping."
                )
                continue

            # ── Insert all records ─────────────────────────────────────────
            placeholders = ", ".join("?" for _ in safe_columns)
            insert_sql = (
                f'INSERT INTO "{table_name}" '
                f'({", ".join(chr(34) + c + chr(34) for c in safe_columns)}) '
                f"VALUES ({placeholders})"
            )

            total_inserted = 0
            for fpath in jsonl_files:
                try:
                    with open(fpath, "r", encoding="utf-8") as fh:
                        batch: List[tuple] = []
                        for line in fh:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                                values = tuple(
                                    str(record.get(raw, "")) if record.get(raw) is not None else ""
                                    for raw in raw_columns
                                )
                                batch.append(values)
                            except json.JSONDecodeError as e:
                                logger.warning(f"[seed] Bad JSON line in '{fpath}': {e}")
                                continue

                            if len(batch) >= 500:
                                conn.executemany(insert_sql, batch)
                                total_inserted += len(batch)
                                batch = []

                        if batch:
                            conn.executemany(insert_sql, batch)
                            total_inserted += len(batch)

                    conn.commit()
                except Exception as e:
                    logger.error(f"[seed] Failed to process '{fpath}': {e}")
                    continue

            logger.info(
                f"[seed] ✔ '{table_name}': inserted {total_inserted} rows."
            )

    except Exception as e:
        logger.error(f"[seed] Fatal error during seeding: {e}")
    finally:
        conn.close()
