import os
import time
import logging
import google.generativeai as genai
from dotenv import load_dotenv

from backend.services.db import execute_query

load_dotenv()

logger = logging.getLogger(__name__)

api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)
else:
    logger.warning("GEMINI_API_KEY environment variable is not set.")

_SCHEMA = """
  - products, product_descriptions, product_plants, product_storage_locations, plants
  - business_partners, business_partner_addresses
  - customer_company_assignments, customer_sales_area_assignments
  - sales_order_headers, sales_order_items, sales_order_schedule_lines
  - outbound_delivery_headers, outbound_delivery_items
  - billing_document_headers, billing_document_items, billing_document_cancellations
  - payments_accounts_receivable, journal_entry_items_accounts_receivable
Key columns use snake_case (e.g. sales_order, billing_document, product_group, created_by_user).
"""

def _is_quota_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "quota" in msg or "rate" in msg or "resource_exhausted" in msg

def _generate_with_retry(model, prompt: str, max_retries: int = 2) -> str:
    """Call model.generate_content with exponential backoff on quota errors."""
    for attempt in range(max_retries + 1):
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            if _is_quota_error(e) and attempt < max_retries:
                wait = 2 ** attempt  # 1s, 2s
                logger.warning(f"[llm] Quota hit, retrying in {wait}s (attempt {attempt + 1})…")
                time.sleep(wait)
            else:
                raise

def ask_database(query: str) -> dict:
    """Convert natural-language query → SQL → execute → return JSON."""
    if not query or not str(query).strip():
        return {"error": "Invalid query. Please provide a non-empty string."}

    if not api_key:
        return {"error": "GEMINI_API_KEY is not configured on the server."}

    try:
        model = genai.GenerativeModel("gemini-2.0-flash")

        prompt = f"""You are an expert SQL assistant for an SAP Order-to-Cash (O2C) SQLite database.
Write a valid SQLite SELECT query for the question below.
Return ONLY the raw SQL — no markdown, no backticks, no explanation.

Available tables:{_SCHEMA}

Question: {query}"""

        try:
            sql_query = _generate_with_retry(model, prompt)
        except Exception as e:
            if _is_quota_error(e):
                logger.warning(f"[llm] Quota exceeded after retries: {e}")
                return {"error": "⚠️ AI quota exceeded. Please wait a moment and try again."}
            raise

        # Strip markdown fences if any
        for fence in ("```sql", "```"):
            if sql_query.startswith(fence):
                sql_query = sql_query[len(fence):]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()

        if not sql_query.upper().startswith("SELECT"):
            return {"error": "Only SELECT queries are permitted for security reasons."}

        results = execute_query(sql_query)

        if results and len(results) == 1 and "error" in results[0]:
            return {"error": results[0]["error"], "sql": sql_query}

        if not results:
            return {"data": [], "message": "No results found.", "sql": sql_query}

        return {"data": results, "sql": sql_query}

    except Exception as e:
        logger.error(f"[llm] Unexpected error: {e}")
        return {"error": "Internal service error while processing the request."}
