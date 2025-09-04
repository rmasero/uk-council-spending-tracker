# streamlit_app.py
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from datetime import datetime
import base64
import io

DB = "spend.db"

st.set_page_config(page_title="Public Spending Tracker", layout="wide")

def conn():
    return sqlite3.connect(DB, check_same_thread=False)

@st.cache_data
def list_councils():
    c = conn()
    df = pd.read_sql_query("SELECT DISTINCT council FROM payments ORDER BY council", c)
    c.close()
    if df.empty:
        return []
    return df['council'].tolist()

def payments_query(council=None, supplier_q=None, date_from=None, date_to=None):
    c = conn()
    q = "SELECT p.id, p.payment_date, p.supplier, p.amount_gbp, p.description, p.invoice_ref, a.anomaly_type FROM payments p LEFT JOIN anomalies a ON p.id=a.payment_id"
    params = []
    if council and council != "All":
        q += " WHERE p.council = ?"
        params.append(council)
    df = pd.read_sql_query(q, c, params=params, parse_dates=['payment_date'])
    c.close()
    if supplier_q:
        df = df[df['supplier'].str.contains(supplier_q, case=False, na=False)]
    if date_from is not None:
        df = df[df['payment_date'] >= pd.to_datetime(date_from)]
    if date_to is not None:
        df = df[df['payment_date'] <= pd.to_datetime(date_to)]
    return df

def sidebar_filters(councils):
    st.sidebar.title("Filters")
    council = st.sidebar.selectbox("Council", ["All"] + councils)
    supplier_q = st.sidebar.text_input("Supplier contains")
    col1, col2 = st.sidebar.columns(2)
    date_from = col1.date_input("From", value=None)
    date_to = col2.date_input("To", value=None)
    return council, supplier_q, date_from, date_to

def show_dashboard(df):
    st.subheader("Summary")
    total = df['amount_gbp'].sum()
    st.metric("Total shown spend", f"£{total:,.2f}")
    # monthly time series
    if not df.empty:
        df['month'] = pd.to_datetime(df['payment_date']).dt.to_period('M').dt.to_timestamp()
        monthly = df.groupby('month')['amount_gbp'].sum().reset_index()
        fig = px.line(monthly, x='month', y='amount_gbp', title="Monthly Spend (shown period)")
        st.plotly_chart(fig, use_container_width=True)
        top = df.groupby('supplier')['amount_gbp'].sum().reset_index().sort_values('amount_gbp', ascending=False).head(10)
        fig2 = px.bar(top, x='supplier', y='amount_gbp', title="Top Suppliers (shown period)")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.write("No data for selected filters")

def show_table_and_download(df):
    st.subheader("Payments")
    st.dataframe(df.sort_values('payment_date', ascending=False).reset_index(drop=True), use_container_width=True)
    csv_data = df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv_data, "payments_export.csv", mime="text/csv")

def show_anomalies(council):
    st.subheader("Anomalies")
    c = conn()
    if council and council != "All":
        q = "SELECT a.id, a.payment_id, a.anomaly_type, a.score, a.note, p.payment_date, p.supplier, p.amount_gbp FROM anomalies a JOIN payments p ON a.payment_id=p.id WHERE p.council=? ORDER BY a.id DESC"
        df = pd.read_sql_query(q, c, params=[council], parse_dates=['payment_date'])
    else:
        df = pd.read_sql_query("SELECT a.id, a.payment_id, a.anomaly_type, a.score, a.note, p.payment_date, p.supplier, p.amount_gbp, p.council FROM anomalies a JOIN payments p ON a.payment_id=p.id ORDER BY a.id DESC", c, parse_dates=['payment_date'])
    c.close()
    if df.empty:
        st.write("No anomalies found.")
    else:
        st.dataframe(df, use_container_width=True)

def review_form(payment_id):
    st.subheader("Leave a review / report")
    with st.form("review_form", clear_on_submit=True):
        name = st.text_input("Your name (optional)")
        rating = st.selectbox("Rating (0-5)", list(range(6)), index=5)
        comment = st.text_area("Comment")
        photo = st.file_uploader("Photo (optional, image)", type=["png","jpg","jpeg"])
        submitted = st.form_submit_button("Submit review")
        if submitted:
            b64 = None
            if photo:
                b = photo.read()
                b64 = base64.b64encode(b).decode('ascii')
            c = conn()
            cur = c.cursor()
            cur.execute("INSERT INTO reviews (payment_id, user_name, rating, comment, photo_base64, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (payment_id, name, rating, comment, b64, datetime.utcnow().isoformat()))
            c.commit()
            c.close()
            st.success("Thanks — your review was submitted.")

def show_reviews(payment_id):
    c = conn()
    df = pd.read_sql_query("SELECT id, user_name, rating, comment, photo_base64, created_at FROM reviews WHERE payment_id=? ORDER BY created_at DESC", c, params=[payment_id])
    c.close()
    if df.empty:
        st.write("No reviews yet.")
    else:
        for _, row in df.iterrows():
            st.markdown(f"**{row['user_name'] or 'Anonymous'}** — {row['rating']}/5 — {row['created_at']}")
            st.write(row['comment'])
            if row['photo_base64']:
                b = base64.b64decode(row['photo_base64'])
                st.image(b, use_column_width=True)

def main():
    st.title("Public Spending Tracker — UK councils")
    councils = list_councils()
    if not councils:
        st.info("No data found. The repo must run the ingestion Action to populate spend.db. See README.")
        return
    council, supplier_q, date_from, date_to = sidebar_filters(councils)
    df = payments_query(council=council, supplier_q=supplier_q, date_from=(None if not date_from else date_from), date_to=(None if not date_to else date_to))
    # layout: left summary and charts, right table and anomalies
    col1, col2 = st.columns([2,3])
    with col1:
        show_dashboard(df)
        # select a payment to review
        st.subheader("Select Payment to review")
        pid = st.selectbox("Payment ID", options=[None] + df['id'].astype(str).tolist())
        if pid:
            pid_int = int(pid)
            review_form(pid_int)
            show_reviews(pid_int)
    with col2:
        show_table_and_download(df)
        show_anomalies(council)
    st.caption("Data discovered via data.gov.uk and council transparency pages. Raw files stored in repo/raw_files with SHA256 hashes for auditability.")

if __name__ == "__main__":
    main()
