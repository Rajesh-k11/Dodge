import os
import sys
import json
import google.generativeai as genai

# Ensure backend directory is in the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db import get_all_tables, execute_query, get_schema  # type: ignore

def verify():
    tables = get_all_tables()
    print("Tables found:", tables)
    
    schema = get_schema()
    for t in tables:
        print(f"\n{'='*40}")
        print(f"Table: {t}")
        print(f"Schema columns: {len(schema[t])} columns")
        
        row_count = execute_query(f'SELECT COUNT(*) as count FROM "{t}"')[0]['count']
        print(f"Row count: {row_count}")
        
        print(f"Sample 3 rows:")
        rows = execute_query(f'SELECT * FROM "{t}" LIMIT 3')
        for i, row in enumerate(rows):
            # Print only first 3 key-value pairs per row just to not overload terminal
            keys = list(row.keys())
            subset = {keys[j]: row[keys[j]] for j in range(min(3, len(keys)))}
            print(f"  Row {i+1}: {subset} ...")

if __name__ == "__main__":
    verify()
