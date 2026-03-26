import os
import json
import re
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_DIR, "data", "sap-o2c-data")
DB_NAME = os.path.join(BASE_DIR, "o2c.db")

def camel_to_snake(name):
    # Converts camelCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def flatten_dict(d, parent_key='', sep='_'):
    """Flatten a nested dictionary and convert keys to snake_case."""
    items = []
    for k, v in d.items():
        if v is None:
            continue
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        new_key = camel_to_snake(new_key)
        # Remove repeated underscores if any
        new_key = re.sub(r'_+', '_', new_key)
        
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def process_dataset():
    if not os.path.exists(DATASET_PATH):
        print(f"Dataset path {DATASET_PATH} does not exist.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for folder_name in os.listdir(DATASET_PATH):
        folder_path = os.path.join(DATASET_PATH, folder_name)
        if not os.path.isdir(folder_path):
            continue
            
        table_name = folder_name
        print(f"Processing table: {table_name}")
        
        all_records = []
        unique_keys = set()
        
        # Pass 1: Parse JSONL and collect unique keys
        for file_name in os.listdir(folder_path):
            if not file_name.endswith('.jsonl'):
                continue
                
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        flat_record = flatten_dict(record)
                        all_records.append(flat_record)
                        unique_keys.update(flat_record.keys())
                    except json.JSONDecodeError:
                        print(f"Skipping malformed JSON line in {file_name}")
                        
        if not all_records:
            print(f"No valid records found for {table_name}.")
            print("-" * 30)
            continue
            
        # Pass 2: Create Table and Batch Insert
        columns = list(unique_keys)
        columns.sort()  # Consistent column order
        
        # Build CREATE TABLE
        columns_def = ", ".join([f'"{col}" TEXT' for col in columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def});'
        cursor.execute(create_sql)
        
        # Build INSERT
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f'INSERT INTO "{table_name}" ({", ".join([f"{col}" for col in columns])}) VALUES ({placeholders});'
        
        insert_data = []
        for record in all_records:
            # Missing fields default to None which becomes NULL in SQLite
            row_data = [str(record.get(col)) if record.get(col) is not None else None for col in columns]
            insert_data.append(row_data)
            
        cursor.executemany(insert_sql, insert_data)
        conn.commit()  # Batch commit per table
        
        print(f"Inserted {len(insert_data)} rows into {table_name}")
        print("-" * 30)
        
    conn.close()
    
    # Print final summary
    print("Ingestion complete. Database tables:")
    from db import get_all_tables
    tables = get_all_tables()
    for t in tables:
        print(f" - {t}")

if __name__ == "__main__":
    process_dataset()
