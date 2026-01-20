import streamlit as st
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"
BQ_LOCATION = "asia-northeast1"

VIEW_TARGET_MONTH = f"{PROJECT_ID}.{DATASET}.v_target_new_adoption_kpi_month"
VIEW_REALIZED_MONTH = f"{PROJECT_ID}.{DATASET}.v_realized_new_adoption_kpi_month"
VIEW_CONVERSION_MONTH = f"{PROJECT_ID}.{DATASET}.v_new_adoption_conversion_month"

# =========================
# BigQuery Client
# =========================
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    # Secrets 形式：
    # [gcp_service_account]
    # json_key = """{...}"""
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets に [gcp_service_account] がありません")

    sa_block = st.secrets["gcp_service_account"]
    if "json_key" not in sa_block:
        raise RuntimeError("Secrets の [gcp_service_account] に json_key がありません")

    sa_info = json.loads(sa_block["json_key"])  # ← ここで JSON を dict に戻す

    credentials = service_account.Credentials.from_service_account_info(sa_info)

    return bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
        location=BQ_LOCATION,
    )

# =========================
# Query Runner
# =========================
@st.cache_data(ttl=600)
def run_query(sql: str, params=None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = client.query(sql, job_config=job_config)
    return job.result().to_dataframe(create_bqstorage_client=False)

# =========================
# UI
# =========================
st.set_page_config(page_title="New Adoption Dashboard", layout="wide")
st.title("📊 New Adoption Dashboard")
st.caption("Target / Realized / Conversion（月次）")

# =========================
# Month Selector
# =========================
sql_months = f"""
SELECT DISTINCT month_ym
FROM `{VIEW_TARGET_MONTH}`
ORDER BY month_ym DESC
"""
months_df = run_query(sql_months)

if months_df.empty:
    st.error("month_ym が取得できません（VIEW_TARGET_MONTH を確認）")
    st.stop()

month_ym = st.selectbox("month_ym (YYYY-MM)", months_df["month_ym"].tolist())

param_month = [bigquery.ScalarQueryParameter("month_ym", "STRING", month_ym)]

# =========================
# Target KPI
# =========================
sql_target = f"""
SELECT
  month_ym,
  target_rows,
  new_customers_cnt,
  new_items_cnt,
  no_yj_cnt,
  no_yj_ratio
FROM `{VIEW_TARGET_MONTH}`
WHERE month_ym = @month_ym
"""
df_target = run_query(sql_target, param_month)

# =========================
# Realized KPI
# =========================
sql_realized = f"""
SELECT
  month_ym,
  realized_items,
  realized_customers,
  realized_sales_sum,
  realized_gp_sum,
  SAFE_DIVIDE(realized_gp_sum, realized_sales_sum) AS gross_margin
FROM `{VIEW_REALIZED_MONTH}`
WHERE month_ym = @month_ym
"""
df_realized = run_query(sql_realized, param_month)

# =========================
# Conversion KPI
# =========================
sql_conversion = f"""
SELECT
  month_ym,
  target_items,
  target_customers,
  realized_items,
  realized_customers,
  conversion_items,
  conversion_customers
FROM `{VIEW_CONVERSION_MONTH}`
WHERE month_ym = @month_ym
"""
df_conversion = run_query(sql_conversion, param_month)

# =========================
# KPI Cards
# =========================
st.subheader("📌 KPI Summary")
c1, c2, c3, c4 = st.columns(4)

if not df_target.empty:
    c1.metric("Target Items", f"{int(df_target.loc[0,'new_items_cnt']):,}")
    c2.metric("Target Customers", f"{int(df_target.loc[0,'new_customers_cnt']):,}")

if not df_realized.empty:
    c3.metric("Realized Sales", f"¥{int(df_realized.loc[0,'realized_sales_sum']):,}")
    c4.metric("Gross Margin", f"{df_realized.loc[0,'gross_margin']:.1%}")

# =========================
# Tables
# =========================
st.divider()
col1, col2 = st.columns(2)

with col1:
    st.subheader("① Target New（月次）")
    st.dataframe(df_target, use_container_width=True)

with col2:
    st.subheader("② Realized New（月次）")
    st.dataframe(df_realized, use_container_width=True)

st.divider()
st.subheader("③ Conversion（Target → Realized）")
st.dataframe(df_conversion, use_container_width=True)

st.caption("※ BigQuery VIEW を直接参照（OS v1 準拠）")
