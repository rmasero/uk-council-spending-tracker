# db_schema.py
import sqlite3

def create_schema(db_path="spend.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sources (
        id INTEGER PRIMARY KEY,
        council TEXT,
        source_url TEXT,
        file_name TEXT,
        file_hash TEXT,
        retrieved_at TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY,
        source_id INTEGER,
        council TEXT,
        payment_date TEXT,
        supplier TEXT,
        supplier_normalized TEXT,
        description TEXT,
        amount_gbp REAL,
        invoice_ref TEXT,
        address TEXT,
        raw_json TEXT,
        UNIQUE(source_id, invoice_ref, amount_gbp, payment_date)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY,
        payment_id INTEGER,
        anomaly_type TEXT,
        score REAL,
        note TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        payment_id INTEGER,
        user_name TEXT,
        rating INTEGER,
        comment TEXT,
        photo_base64 TEXT,
        created_at TEXT
    );
    """)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_schema()
    print("Schema created.")
