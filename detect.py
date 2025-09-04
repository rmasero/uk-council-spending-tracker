# detect.py
import sqlite3
import statistics
from collections import Counter

DB = "spend.db"

def compute_anomalies(db_path=DB):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, amount_gbp, supplier FROM payments WHERE amount_gbp IS NOT NULL")
    rows = cur.fetchall()
    amounts = [r[1] for r in rows if isinstance(r[1], (int,float))]
    if not amounts:
        conn.close()
        return
    median = statistics.median(amounts)
    # large payment threshold: median * 5 or fixed floor
    large_threshold = max(median * 5, 50000)
    suppliers = [r[2] for r in rows]
    supplier_counts = Counter(suppliers)
    frequent_threshold = 30  # arbitrary; tune later

    # clear old anomalies
    cur.execute("DELETE FROM anomalies")
    conn.commit()

    for pid, amount, supplier in rows:
        if amount and amount > large_threshold:
            cur.execute("INSERT INTO anomalies (payment_id, anomaly_type, score, note) VALUES (?, ?, ?, ?)",
                        (pid, "large_payment", amount / (median if median else 1), f"Payment {amount} > threshold {large_threshold}"))
        if supplier and supplier_counts.get(supplier,0) >= frequent_threshold:
            cur.execute("INSERT INTO anomalies (payment_id, anomaly_type, score, note) VALUES (?, ?, ?, ?)",
                        (pid, "frequent_winner", supplier_counts[supplier], f"Supplier {supplier} appears {supplier_counts[supplier]} times"))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    compute_anomalies()
    print("Anomaly pass complete.")
