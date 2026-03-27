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
        You are an expert SQL assistant for an SAP Order-to-Cash (O2C) SQLite database.
        Given the following natural language query, write a valid SQLite SELECT query.
        Return ONLY the raw SQL query as plain text — no markdown, no backticks, no explanation.

        The database contains these tables:
          - products               (product, productType, product_old_id, product_group, division, industry_sector, base_unit, gross_weight, net_weight, weight_unit, created_by_user, creation_date, last_change_date, is_marked_for_deletion)
          - product_descriptions   (product, language, product_description)
          - product_plants         (product, plant, profit_center, valuation_type)
          - product_storage_locations (product, plant, storage_location)
          - plants                 (plant, plant_name, country, region, company_code)
          - business_partners      (business_partner, business_partner_category, business_partner_type, first_name, last_name, organization_name, industry, creation_date)
          - business_partner_addresses (business_partner, address_id, street, house_number, city, postal_code, country, region)
          - customer_company_assignments (customer, company_code, account_group, reconciliation_account, payment_terms)
          - customer_sales_area_assignments (customer, sales_org, distribution_channel, division, customer_group, price_list)
          - sales_order_headers    (sales_order, sales_org, distribution_channel, division, sold_to_party, ship_to_party, document_date, delivery_date, net_value, currency, document_type, created_by_user, creation_date)
          - sales_order_items      (sales_order, sales_order_item, product, plant, order_quantity, base_unit, net_price, currency, item_category)
          - sales_order_schedule_lines (sales_order, sales_order_item, schedule_line, requested_delivery_date, confirmed_delivery_date, schedule_line_quantity)
          - outbound_delivery_headers (outbound_delivery, sales_order, actual_goods_movement_date, delivery_date, ship_to_party, shipping_point)
          - outbound_delivery_items (outbound_delivery, outbound_delivery_item, product, actual_delivered_qty, base_unit, sales_order, sales_order_item)
          - billing_document_headers (billing_document, sales_org, distribution_channel, billing_type, payer, document_date, net_value, currency, created_by_user)
          - billing_document_items  (billing_document, billing_document_item, sales_order, sales_order_item, product, billing_quantity, base_unit, net_value, currency)
          - billing_document_cancellations (billing_document, cancellation_billing_document, cancellation_date)
          - payments_accounts_receivable (accounting_document, company_code, fiscal_year, customer, amount, currency, document_date, clearing_date, payment_reference)
          - journal_entry_items_accounts_receivable (accounting_document, line_item, company_code, fiscal_year, customer, gl_account, amount, currency, document_date, assignment)

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