# cleaning.py
import re
import json
from rapidfuzz import fuzz

def normalize_supplier(name):
    if not isinstance(name, str):
        return ""
    s = name.lower().strip()
    s = re.sub(r'[^a-z0-9 &\-\.]', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s

def similar(a,b):
    if not a or not b:
        return 0
    return fuzz.ratio(a,b) / 100.0

def to_canonical_row(row):
    supplier = row.get('supplier') or row.get('Supplier') or row.get('Payee') or row.get('beneficiary') or row.get('supplier_name') or ""
    amount = row.get('amount') or row.get('Amount') or row.get('payment') or row.get('value') or row.get('AmountGBP') or row.get('Amount (GBP)') or 0
    date = row.get('date') or row.get('PaymentDate') or row.get('payment_date') or row.get('Date') or ""
    desc = row.get('description') or row.get('Description') or row.get('Details') or ""
    invoice = row.get('invoice_ref') or row.get('DocumentRef') or row.get('Invoice') or row.get('Reference') or ""
    address = row.get('address') or row.get('project_address') or ""
    try:
        amt = float(str(amount).replace('£','').replace(',',''))
    except:
        try:
            amt = float(amount)
        except:
            amt = None
    return {
        "payment_date": date,
        "supplier": supplier,
        "supplier_normalized": normalize_supplier(supplier),
        "description": desc,
        "amount_gbp": amt,
        "invoice_ref": invoice,
        "address": address,
        "raw_json": json.dumps(dict(row))
    }
