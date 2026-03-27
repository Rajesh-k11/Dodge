import sqlite3
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

DB_PATH = "o2c_graph.db"

def _get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Executes a SQL query and returns the results as a list of dictionaries."""
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        
        if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP")):
            conn.commit()
            return [{"status": "success", "rows_affected": cursor.rowcount}]
            
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        return result
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return [{"error": f"Database error: {str(e)}"}]
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return [{"error": f"Unexpected database error: {str(e)}"}]
    finally:
        if 'conn' in locals() and conn:
            conn.close()
