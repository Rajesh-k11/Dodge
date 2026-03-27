import os
import time
import logging
from groq import Groq
from dotenv import load_dotenv

from backend.services.db import execute_query

load_dotenv()

logger = logging.getLogger(__name__)

api_key = os.getenv("GROQ_API_KEY")
client = None
if api_key:
    client = Groq(api_key=api_key)
else:
    logger.warning("GROQ_API_KEY environment variable is not set.")

_SCHEMA = """
  - billing_document_cancellations (accounting_document, billing_document, billing_document_date, billing_document_is_cancelled, billing_document_type, cancelled_billing_document, company_code, creation_date, creation_time_hours, creation_time_minutes, creation_time_seconds, fiscal_year, last_change_date_time, sold_to_party, total_net_amount, transaction_currency)
  - billing_document_headers (accounting_document, billing_document, billing_document_date, billing_document_is_cancelled, billing_document_type, cancelled_billing_document, company_code, creation_date, creation_time_hours, creation_time_minutes, creation_time_seconds, fiscal_year, last_change_date_time, sold_to_party, total_net_amount, transaction_currency)
  - billing_document_items (billing_document, billing_document_item, billing_quantity, billing_quantity_unit, material, net_amount, reference_sd_document, reference_sd_document_item, transaction_currency)
  - business_partner_addresses (address_id, address_time_zone, address_uuid, business_partner, city_name, country, po_box, po_box_deviating_city_name, po_box_deviating_country, po_box_deviating_region, po_box_is_without_number, po_box_lobby_name, po_box_postal_code, postal_code, region, street_name, tax_jurisdiction, transport_zone, validity_end_date, validity_start_date)
  - business_partners (business_partner, business_partner_category, business_partner_full_name, business_partner_grouping, business_partner_is_blocked, business_partner_name, correspondence_language, created_by_user, creation_date, creation_time_hours, creation_time_minutes, creation_time_seconds, customer, first_name, form_of_address, industry, is_marked_for_archiving, last_change_date, last_name, organization_bp_name1, organization_bp_name2)
  - customer_company_assignments (accounting_clerk, accounting_clerk_fax_number, accounting_clerk_internet_address, accounting_clerk_phone_number, alternative_payer_account, company_code, customer, customer_account_group, deletion_indicator, payment_blocking_reason, payment_methods_list, payment_terms, reconciliation_account)
  - customer_sales_area_assignments (billing_is_blocked_for_customer, complete_delivery_is_defined, credit_control_area, currency, customer, customer_payment_terms, delivery_priority, distribution_channel, division, exchange_rate_type, incoterms_classification, incoterms_location1, sales_district, sales_group, sales_office, sales_organization, shipping_condition, sls_unlmtd_ovrdeliv_is_allwd, supplying_plant)
  - journal_entry_items_accounts_receivable (accounting_document, accounting_document_item, accounting_document_type, amount_in_company_code_currency, amount_in_transaction_currency, assignment_reference, clearing_accounting_document, clearing_date, clearing_doc_fiscal_year, company_code, company_code_currency, cost_center, customer, document_date, financial_account_type, fiscal_year, gl_account, last_change_date_time, posting_date, profit_center, reference_document, transaction_currency)
  - outbound_delivery_headers (actual_goods_movement_date, actual_goods_movement_time_hours, actual_goods_movement_time_minutes, actual_goods_movement_time_seconds, creation_date, creation_time_hours, creation_time_minutes, creation_time_seconds, delivery_block_reason, delivery_document, hdr_general_incompletion_status, header_billing_block_reason, last_change_date, overall_goods_movement_status, overall_picking_status, overall_proof_of_delivery_status, shipping_point)
  - outbound_delivery_items (actual_delivery_quantity, batch, delivery_document, delivery_document_item, delivery_quantity_unit, item_billing_block_reason, plant, reference_sd_document, reference_sd_document_item, storage_location)
  - payments_accounts_receivable (accounting_document, accounting_document_item, amount_in_company_code_currency, amount_in_transaction_currency, clearing_accounting_document, clearing_date, clearing_doc_fiscal_year, company_code, company_code_currency, customer, document_date, financial_account_type, fiscal_year, gl_account, posting_date, profit_center, transaction_currency)
  - plants (address_id, default_purchasing_organization, distribution_channel, division, factory_calendar, is_marked_for_archiving, language, plant, plant_category, plant_customer, plant_name, plant_supplier, sales_organization, valuation_area)
  - product_descriptions (language, product, product_description)
  - product_plants (availability_check_type, country_of_origin, fiscal_year_variant, mrp_type, plant, product, production_invtry_managed_loc, profit_center, region_of_origin)
  - product_storage_locations (physical_inventory_block_ind, plant, product, storage_location)
  - products (base_unit, created_by_user, creation_date, cross_plant_status, division, gross_weight, industry_sector, is_marked_for_deletion, last_change_date, last_change_date_time, net_weight, product, product_group, product_old_id, product_type, weight_unit)
  - sales_order_headers (created_by_user, creation_date, customer_payment_terms, delivery_block_reason, distribution_channel, header_billing_block_reason, incoterms_classification, incoterms_location1, last_change_date_time, organization_division, overall_delivery_status, overall_ord_reltd_billg_status, overall_sd_doc_reference_status, pricing_date, requested_delivery_date, sales_group, sales_office, sales_order, sales_order_type, sales_organization, sold_to_party, total_net_amount, transaction_currency)
  - sales_order_items (item_billing_block_reason, material, material_group, net_amount, production_plant, requested_quantity, requested_quantity_unit, sales_document_rjcn_reason, sales_order, sales_order_item, sales_order_item_category, storage_location, transaction_currency)
  - sales_order_schedule_lines (confd_order_qty_by_matl_avail_check, confirmed_delivery_date, order_quantity_unit, sales_order, sales_order_item, schedule_line)
Note: 'material' in item tables corresponds to 'product' in the products table.
"""

def _is_quota_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "quota" in msg or "rate" in msg or "resource_exhausted" in msg

def _generate_with_retry(prompt: str, max_retries: int = 2) -> str:
    """Call Groq API with exponential backoff on quota errors."""
    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            return response.choices[0].message.content.strip()
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

    if not client:
        return {"error": "GROQ_API_KEY is not configured on the server."}

    try:
        prompt = f"""You are an expert SQL assistant for an SAP Order-to-Cash (O2C) SQLite database.
Write a valid SQLite SELECT query for the question below.
Return ONLY the raw SQL — no markdown, no backticks, no explanation.

Available tables:{_SCHEMA}

Question: {query}"""

        try:
            sql_query = _generate_with_retry(prompt)
            logger.info(f"[llm] Generated SQL: {sql_query}")
        except Exception as e:
            if _is_quota_error(e):
                logger.warning(f"[llm] Quota exceeded after retries: {e}")
                return {"error": "⚠️ AI quota exceeded. Please wait a moment and try again."}
            raise

        # Strip markdown fences if any
        for fence in ("```sql", "```", "'''"):
            if sql_query.startswith(fence):
                sql_query = sql_query[len(fence):]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        if sql_query.endswith("'''"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()

        if not sql_query:
            logger.warning("[llm] LLM produced an empty query.")
            return {"error": "The AI could not generate a valid query for your request."}

        if not sql_query.upper().startswith("SELECT"):
            logger.warning(f"[llm] Blocked non-SELECT query: {sql_query}")
            return {"error": "Only SELECT queries are permitted for security reasons."}

        results = execute_query(sql_query)
        logger.info(f"[llm] Query returned {len(results)} rows.")

        if results and len(results) == 1 and "error" in results[0]:
            return {"error": results[0]["error"], "sql": sql_query}

        # Even if data is empty, we MUST return 'data: []' to keep the frontend happy
        return {
            "data": results if results else [],
            "message": f"Found {len(results)} results." if results else "No results found for your query.",
            "sql": sql_query
        }

    except Exception as e:
        logger.error(f"[llm] Unexpected error: {e}", exc_info=True)
        return {"error": f"Internal service error: {str(e)}"}

