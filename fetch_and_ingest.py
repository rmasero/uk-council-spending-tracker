# fetch_and_ingest.py
import os, io, hashlib, datetime, json
import requests
import sqlite3
import pandas as pd
from db_schema import create_schema
from discover import search_data_gov, get_local_authority_domains, crawl_for_payment_csv
from cleaning import to_canonical_row

DB = "spend.db"
RAW_DIR = "raw_files"

def sha256_bytes(b):
    import hashlib
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def save_raw_bytes(council, url, content):
    os.makedirs(RAW_DIR, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    safe = council.replace(" ", "_").replace("/","_")[:60]
    fname = f"{safe}_{ts}.bin"
    path = os.path.join(RAW_DIR, fname)
    with open(path, "wb") as f:
        f.write(content)
    return fname, path

def insert_source(conn, council, source_url, file_name, file_hash):
    cur = conn.cursor()
    cur.execute("INSERT INTO sources (council, source_url, file_name, file_hash, retrieved_at) VALUES (?, ?, ?, ?, ?)",
                (council, source_url, file_name, file_hash, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    return cur.lastrowid

def ingest_dataframe(conn, source_id, council, df):
    cur = conn.cursor()
    inserted = 0
    for idx, row in df.iterrows():
        r = to_canonical_row(row.to_dict() if hasattr(row,'to_dict') else dict(row))
        try:
            cur.execute("""
                INSERT OR IGNORE INTO payments (source_id, council, payment_date, supplier, supplier_normalized, description, amount_gbp, invoice_ref, address, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, council, r['payment_date'], r['supplier'], r['supplier_normalized'], r['description'], r['amount_gbp'], r['invoice_ref'], r['address'], r['raw_json']))
            if cur.rowcount:
                inserted += 1
        except Exception as e:
            print("Insert error", e)
    conn.commit()
    return inserted

def download_and_ingest(url, council, conn):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print("Failed to download", url, e)
        return 0, None
    content = r.content
    h = sha256_bytes(content)
    fname, path = save_raw_bytes(council, url, content)
    source_id = insert_source(conn, council, url, fname, h)
    # try parsing with pandas
    df = None
    for read_fn in (pd.read_csv, pd.read_excel):
        try:
            if read_fn==pd.read_csv:
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
            break
        except Exception:
            continue
    if df is None:
        # try with latin1 for CSV
        try:
            df = pd.read_csv(io.BytesIO(content), encoding='latin1')
        except Exception as e:
            print("Failed to parse", url, e)
            return 0, source_id
    inserted = ingest_dataframe(conn, source_id, council, df)
    return inserted, source_id

def run_full_discovery_and_ingest():
    create_schema(DB)
    conn = sqlite3.connect(DB)
    total_inserted = 0
    # strategy 1: search data.gov.uk
    queries = ["payments suppliers", "payments to suppliers", "payments to suppliers council", "payments to suppliers over"]
    seen_urls = set()
    for q in queries:
        resources = search_data_gov(q, rows=200)
        for res in resources:
            url = res['resource_url']
            council = res.get('council') or "Unknown"
            if url in seen_urls:
                continue
            seen_urls.add(url)
            print("Ingesting from data.gov.uk resource:", council, url)
            inserted, sid = download_and_ingest(url, council, conn)
            print("Inserted rows:", inserted)
            total_inserted += inserted
    # strategy 2: crawl council domains
    domains = get_local_authority_domains()
    for d in domains:
        try:
            csvs = crawl_for_payment_csv(d)
            for url in csvs:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                print("Crawled URL:", d, url)
                inserted, sid = download_and_ingest(url, d, conn)
                print("Inserted rows:", inserted)
                total_inserted += inserted
        except Exception as e:
            print("Error crawling", d, e)
    conn.close()
    print("Total inserted rows this run:", total_inserted)
    return total_inserted

if __name__ == "__main__":
    run_full_discovery_and_ingest()
