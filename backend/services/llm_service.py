import os
import json
import hashlib
import sqlite3
import google.generativeai as genai
from dotenv import load_dotenv
from db import get_schema, execute_query

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)

# ─── In-memory query cache ────────────────────────────────────────────────────
QUERY_CACHE: dict = {}

def _cache_key(question: str) -> str:
    return hashlib.md5(question.strip().lower().encode()).hexdigest()

def get_model():
    return genai.GenerativeModel("gemini-2.5-flash")

# ─── SQL Generation ───────────────────────────────────────────────────────────
def generate_sql(question: str) -> dict:
    schema = get_schema()
    prompt = f"""
You are an expert SQLite data analyst.
Here is the database schema (dictionary mapping tables to an array of their columns):
{json.dumps(schema, indent=2)}

The user asks: "{question}"

Instructions:
1. Generate a valid, safe, read-only SQLite query to answer the question.
2. Limit results to a maximum of 50 rows using 'LIMIT 50'.
3. CRITICAL: ENSURE STRICT SQLITE SYNTAX. Do not use commas instead of dots for table aliases (e.g. use T1.column_name, NEVER T1,column_name). Use standard JOINs.
4. CRITICAL: HANDLE MISSING FLOWS. If the user asks for a full document flow (e.g. Sales Order -> Delivery -> Billing -> Journal Entry) but the database lacks the full chain, DO NOT return an empty query. Instead, return the closest related entities available (e.g., product -> billing -> user).
5. Return EXACTLY a JSON structure with NO markdown, NO backticks, NO extra text:
{{
  "sql": "SELECT ...",
  "explanation": "Brief explanation. If a full flow was requested but is missing, explicitly state 'This represents a partial flow based on available dataset relationships.', else just briefly explain."
}}
"""
    try:
        model = get_model()
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        text = response.text.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        return json.loads(text.strip())
    except Exception as e:
        err = str(e)
        if "429" in err or "quota" in err.lower() or "rate" in err.lower():
            return {"quota_error": True, "error": "API quota exceeded."}
        return {"error": f"SQL generation failed: {err}"}


# ─── Main pipeline ────────────────────────────────────────────────────────────
def ask_database(question: str) -> dict:
    # Guardrail
    if not is_valid_query(question):
        return {"error": "This system answers questions about the O2C dataset only."}

    # Cache check
    key = _cache_key(question)
    if key in QUERY_CACHE:
        cached = dict(QUERY_CACHE[key])
        cached["cached"] = True
        return cached

    # Step 1: Generate SQL
    sql_response = generate_sql(question)

    if sql_response.get("quota_error"):
        # Fallback: return raw DB sample without LLM answer
        fallback_data = execute_query("SELECT * FROM products LIMIT 10") or []
        return {
            "answer": "⚠️ AI quota exceeded. Showing available data from database.",
            "data": fallback_data,
            "sql": None,
            "cached": False,
        }

    if "error" in sql_response:
        return sql_response

    sql_query = sql_response.get("sql")
    explanation = sql_response.get("explanation", "")

    if not sql_query:
        return {"error": "Malformed response from AI: missing 'sql' key."}

    # Step 2: Execute SQL
    try:
        if "limit" not in sql_query.lower():
            sql_query = sql_query.rstrip(";") + " LIMIT 50;"
        db_results = execute_query(sql_query)
        if not db_results:
            # If 0 rows, return a structured fallback response rather than a hard error
            return {
                "answer": "The dataset does not contain a complete transactional chain for this request. However, here are the related entities available.",
                "explanation": explanation,
                "sql": sql_query,
                "data": []
            }
        db_results = db_results[:50]
    except sqlite3.Error as e:
        return {"error": f"SQL execution error: {str(e)}", "sql": sql_query}
    except Exception as e:
        return {"error": f"Database error: {str(e)}", "sql": sql_query}

    # Step 3: Generate NL Answer
    answer_prompt = f"""
You are a concise business data analyst.
User asked: "{question}"
SQL executed: {sql_query}
Data returned: {json.dumps(db_results[:10], indent=2)}

Provide a 2-3 sentence natural language answer with specific numbers from the data.
"""
    try:
        model = get_model()
        answer = model.generate_content(answer_prompt, generation_config={"temperature": 0.1})
        result = {
            "sql": sql_query,
            "explanation": explanation,
            "data": db_results,
            "answer": answer.text.strip(),
            "cached": False,
        }
    except Exception as e:
        err = str(e)
        if "429" in err or "quota" in err.lower():
            # SQL worked, data is good — just skip NL answer
            result = {
                "sql": sql_query,
                "explanation": explanation,
                "data": db_results,
                "answer": f"⚠️ AI quota exceeded. Here are the raw results ({len(db_results)} rows returned).",
                "cached": False,
            }
        else:
            result = {
                "sql": sql_query,
                "explanation": explanation,
                "data": db_results,
                "answer": f"Data retrieved successfully ({len(db_results)} rows). AI explanation unavailable.",
                "cached": False,
            }

    # Store in cache
    QUERY_CACHE[key] = result
    return result


# ─── Guardrail ────────────────────────────────────────────────────────────────
def is_valid_query(question: str) -> bool:
    allowed = [
        "order", "product", "customer", "invoice", "flow", "chain", "trace",
        "delivery", "payment", "sales", "billing", "document", "journal", "entry",
        "user", "division", "group", "sector", "industry", "relationship",
        "show", "list", "find", "get", "how many", "count", "top", "associate"
    ]
    q = question.lower()
    return any(kw in q for kw in allowed)