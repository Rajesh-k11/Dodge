import os
import logging
import google.generativeai as genai
from dotenv import load_dotenv

# Absolute imports per requirement
from backend.services.db import execute_query

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Initialize the Gemini client
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)
else:
    logger.warning("GEMINI_API_KEY environment variable is not set.")

def ask_database(query: str) -> dict:
    """
    Converts user query to SQL using LLM, executes it, and returns structured JSON.
    """
    if not query or not str(query).strip():
        return {"error": "Invalid query provided. Please provide a non-empty string."}
        
    if not api_key:
        return {"error": "GEMINI_API_KEY is not configured on the server."}
        
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        
        prompt = f"""
        You are an expert SQL assistant for an Order-to-Cash (O2C) system. 
        Given the following natural language query, write a valid SQLite query to retrieve the data.
        Return ONLY the raw SQL query as plain text, without any markdown formatting, backticks, or explanation.
        
        Query: {query}
        """
        
        response = model.generate_content(prompt)
        sql_query = response.text.strip()
        
        # Clean up possible markdown formatting
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
            
        sql_query = sql_query.strip()
        
        # Security check: only allow read queries
        if not sql_query.upper().startswith("SELECT"):
            return {"error": "Only SELECT queries are allowed for security reasons."}
            
        # Execute query
        try:
            results = execute_query(sql_query)
            
            # Handle error from db.py
            if results and len(results) == 1 and "error" in results[0]:
                return {"error": results[0]["error"], "sql": sql_query}
                
        except Exception as e:
            return {"error": f"Failed to execute database query: {str(e)}", "sql": sql_query}
            
        if not results:
            return {"data": [], "message": "No results found for your query.", "sql": sql_query}
            
        return {"data": results, "sql": sql_query}
        
    except Exception as e:
        logger.error(f"LLM API failure or unexpected error: {str(e)}")
        return {"error": "Internal service error while processing the request."}