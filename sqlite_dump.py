import sqlite3
import os

def run():
    db_path = 'backend/o2c.db'
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [r[0] for r in cursor.fetchall()]
    
    print("Table Row Counts:")
    for table in sorted(tables):
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f" - {table}: {count} rows")
        
    print("\nDetailed Schema:")
    schema_output = []
    for table in sorted(tables):
        cursor.execute(f"PRAGMA table_info({table});")
        columns = [row[1] for row in cursor.fetchall()]
        schema_output.append(f"  - {table} ({', '.join(columns)})")
    
    with open('schema_dump.txt', 'w') as f:
        f.write('\n'.join(schema_output))
    
    for line in schema_output:
        print(line)

if __name__ == '__main__':
    run()
