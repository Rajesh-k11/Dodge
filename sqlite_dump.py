import sqlite3

def run():
    conn = sqlite3.connect('backend/o2c.db')
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for t in tables:
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info({t})")]
        print(f"          - {t} ({', '.join(cols)})")

if __name__ == '__main__':
    run()
