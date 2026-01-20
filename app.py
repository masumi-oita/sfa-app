import streamlit as st
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"
BQ_LOCATION = "asia-northeast1"

# ---- Primary (expected) views ----
VIEW_TARGET_MONTH_PRIMARY = f"{PROJECT_ID}.{DATASET}.v_target_new_adoption_kpi_month"
VIEW_REALIZED_MONTH_PRIMARY = f"{PROJECT_ID}.{DATASET}.v_realized_new_adoption_kpi_month"
VIEW_CONVERSION_MONTH_PRIMARY = f"{PROJECT_ID}.{DATASET}.v_new_adoption_conversion_month"

# ---- Fallback views (known to exist in your project history) ----
VIEW_TARGET_FALLBACK = f"{PROJECT_ID}.{DATASET}.v_target_new_adoption"
VIEW_REALIZED_FALLBACK = f"{PROJECT_ID}.{DATASET}.v_realized_new_adoption_kpi"
VIEW_CONVERSION_FALLBACK = f"{PROJECT_ID}.{DATASET}.v_new_adoption_conversion_month"

# =========================
# BigQuery Client
# =========================
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """
    Streamlit Secrets:
    [gcp_service_account]
    json_key = """{...}"""
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets に [gcp_service_account] がありません")
    sa_block = st.secrets["gcp_service_account"]
    if "json_key" not in sa_block:
        raise RuntimeError("Secrets の [gcp_service_account] に json_key がありません")

    sa_info = json.loads(sa_block["json_key"])
    credentials = service_account.Credentials.from_service_account_info(sa_info)

    return bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
        location=BQ_LOCATION,
    )

# =========================
# Helpers
# =========================
@st.cache_data(ttl=600)
def run_query(sql: str, params=None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = client.query(sql, job_config=job_config)
    return job.result().to_dataframe(create_bqstorage_client=False)

@st.cache_data(ttl=600)
def table_exists(full_name: str) -> bool:
    """
    BigQuery VIEW/TABLE existence check via INFORMATION_SCHEMA.
    full_name: "project.dataset.table"
    """
    try:
        project, dataset, table = full_name.split(".")
    except ValueError:
        return False

    sql = f"""
    SELECT 1
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = @t
    LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("t", "STRING", table)]
    df = run_query(sql, params)
    return not df.empty

def safe_query(sql: str, params=None, empty_columns=None, context_name="") -> pd.DataFrame:
    """
    Execute query safely.
    If NotFound / errors occur, return empty DF.
    """
    try:
        return run_query(sql, params)
    except NotFound as e:
        st.warning(f"NotFound: {context_name}（参照先が存在しない可能性）")
        st.caption(str(e))
    except Exception as e:
        st.error(f"Query error: {context_name}")
        st.caption(str(e))
    if empty_columns:
        return pd.DataFrame(columns=empty_columns)
    return pd.DataFrame()

# =========================
# Page
# =========================
st.set_page_config(page_title="New Adoption Dashboard", layout="wide")
st.title("📊 New Adoption Dashboard")
st.caption("Target / Realized / Conversion（月次）")

# =========================
# Resolve views (existence-based)
# =========================
view_target_month = VIEW_TARGET_MONTH_PRIMARY if table_exists(VIEW_TARGET_MONTH_PRIMARY) else None
view_realized_month = VIEW_REALIZED_MONTH_PRIMARY if table_exists(VIEW_REALIZED_MONTH_PRIMARY) else None
view_conversion_month = (
    VIEW_CONVERSION_MONTH_PRIMARY if table_exists(VIEW_CONVERSION_MONTH_PRIMARY)
    else (VIEW_CONVERSION_FALLBACK if table_exists(VIEW_CONVERSION_FALLBACK) else None)
)

# Provide fallbacks for KPI queries if month views are missing
view_target_fallback = VIEW_TARGET_FALLBACK if table_exists(VIEW_TARGET_FALLBACK) else None
view_realized_fallback = VIEW_REALIZED_FALLBACK if table_exists(VIEW_REALIZED_FALLBACK) else None

with st.expander("🔧 Debug: Using Views", expanded=False):
    st.write({
        "TARGET_MONTH": view_target_month,
        "REALIZED_MONTH": view_realized_month,
        "CONVERSION_MONTH": view_conversion_month,
        "TARGET_FALLBACK": view_target_fallback,
        "REALIZED_FALLBACK": view_realized_fallback,
    })

if view_conversion_month is None:
    st.error("conversion（月次）VIEW が見つかりません。`v_new_adoption_conversion_month` の存在を確認してください。")
    st.stop()

# =========================
# Month selector (from conversion view: most stable)
# =========================
sql_months = f"""
SELECT DISTINCT month_ym
FROM `{view_conversion_month}`
ORDER BY month_ym DESC
"""
months_df = safe_query(sql_months, context_name="months (from conversion view)", empty_columns=["month_ym"])

if months_df.empty:
    st.error("month_ym が取得できません。conversion VIEW の列名を確認してください（month_ym）。")
    st.stop()

month_ym = st.selectbox("month_ym (YYYY-MM)", months_df["month_ym"].tolist())
param_month = [bigquery.ScalarQueryParameter("month_ym", "STRING", month_ym)]

# =========================
# Target KPI（月次）
# =========================
if view_target_month:
    sql_target = f"""
    SELECT
      month_ym,
      target_rows,
      new_customers_cnt,
      new_items_cnt,
      no_yj_cnt,
      no_yj_ratio
    FROM `{view_target_month}`
    WHERE month_ym = @month_ym
    """
    df_target = safe_query(
        sql_target, param_month,
        empty_columns=["month_ym","target_rows","new_customers_cnt","new_items_cnt","no_yj_cnt","no_yj_ratio"],
        context_name="target KPI month"
    )
elif view_target_fallback:
    # Build month KPI on the fly from v_target_new_adoption
    sql_target = f"""
    SELECT
      month_ym,
      COUNT(*) AS target_rows,
      COUNT(DISTINCT customer_code) AS new_customers_cnt,
      COUNT(DISTINCT customer_yj_key) AS new_items_cnt,
      COUNTIF(yj_code_norm = 'NO_YJ') AS no_yj_cnt,
      SAFE_DIVIDE(COUNTIF(yj_code_norm = 'NO_YJ'), COUNT(*)) AS no_yj_ratio
    FROM `{view_target_fallback}`
    WHERE month_ym = @month_ym
    GROUP BY month_ym
    """
    df_target = safe_query(
        sql_target, param_month,
        empty_columns=["month_ym","target_rows","new_customers_cnt","new_items_cnt","no_yj_cnt","no_yj_ratio"],
        context_name="target KPI month (fallback calc)"
    )
else:
    df_target = pd.DataFrame(columns=["month_ym","target_rows","new_customers_cnt","new_items_cnt","no_yj_cnt","no_yj_ratio"])

# =========================
# Realized KPI（月次）
# =========================
if view_realized_month:
    sql_realized = f"""
    SELECT
      month_ym,
      realized_items,
      realized_customers,
      realized_sales_sum,
      realized_gp_sum,
      SAFE_DIVIDE(realized_gp_sum, realized_sales_sum) AS gross_margin
    FROM `{view_realized_month}`
    WHERE month_ym = @month_ym
    """
    df_realized = safe_query(
        sql_realized, param_month,
        empty_columns=["month_ym","realized_items","realized_customers","realized_sales_sum","realized_gp_sum","gross_margin"],
        context_name="realized KPI month"
    )
elif view_realized_fallback:
    # Build month KPI on the fly from v_realized_new_adoption_kpi
    sql_realized = f"""
    SELECT
      FORMAT_DATE('%Y-%m', sales_date) AS month_ym,
      COUNT(DISTINCT CONCAT(customer_code, '_', yj_code_norm)) AS realized_items,
      COUNT(DISTINCT customer_code) AS realized_customers,
      SUM(sales_amount) AS realized_sales_sum,
      SUM(gross_profit) AS realized_gp_sum,
      SAFE_DIVIDE(SUM(gross_profit), SUM(sales_amount)) AS gross_margin
    FROM `{view_realized_fallback}`
    WHERE FORMAT_DATE('%Y-%m', sales_date) = @month_ym
    GROUP BY month_ym
    """
    df_realized = safe_query(
        sql_realized, param_month,
        empty_columns=["month_ym","realized_items","realized_customers","realized_sales_sum","realized_gp_sum","gross_margin"],
        context_name="realized KPI month (fallback calc)"
    )
else:
    df_realized = pd.DataFrame(columns=["month_ym","realized_items","realized_customers","realized_sales_sum","realized_gp_sum","gross_margin"])

# =========================
# Conversion（月次）
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
FROM `{view_conversion_month}`
WHERE month_ym = @month_ym
"""
df_conversion = safe_query(
    sql_conversion, param_month,
    empty_columns=["month_ym","target_items","target_customers","realized_items","realized_customers","conversion_items","conversion_customers"],
    context_name="conversion month"
)

# =========================
# KPI cards
# =========================
st.subheader("📌 KPI Summary")
c1, c2, c3, c4 = st.columns(4)

# Target cards
if not df_target.empty:
    c1.metric("Target Items", f"{int(df_target.loc[0,'new_items_cnt']):,}")
    c2.metric("Target Customers", f"{int(df_target.loc[0,'new_customers_cnt']):,}")
else:
    c1.metric("Target Items", "—")
    c2.metric("Target Customers", "—")

# Realized cards
if not df_realized.empty:
    c3.metric("Realized Sales", f"¥{int(df_realized.loc[0,'realized_sales_sum']):,}")
    gm = df_realized.loc[0, "gross_margin"]
    c4.metric("Gross Margin", "—" if pd.isna(gm) else f"{gm:.1%}")
else:
    c3.metric("Realized Sales", "—")
    c4.metric("Gross Margin", "—")

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

# =========================
# Extra: derived conversion rate
# =========================
st.subheader("📈 Conversion Rate（計算）")

if not df_conversion.empty:
    row = df_conversion.iloc[0].to_dict()
    conv_items = row.get("conversion_items")
    conv_customers = row.get("conversion_customers")
    tgt_items = row.get("target_items")
    tgt_customers = row.get("target_customers")

    rate_items = None
    rate_customers = None
    try:
        if tgt_items and tgt_items != 0:
            rate_items = float(conv_items) / float(tgt_items)
        if tgt_customers and tgt_customers != 0:
            rate_customers = float(conv_customers) / float(tgt_customers)
    except Exception:
        pass

    c5, c6 = st.columns(2)
    c5.metric("Items Conversion", "—" if rate_items is None else f"{rate_items:.1%}")
    c6.metric("Customers Conversion", "—" if rate_customers is None else f"{rate_customers:.1%}")
else:
    st.info("conversion のデータがありません。")

st.caption("※ BigQuery VIEW を直接参照（OS v1 準拠） / VIEWが無い場合はフォールバック計算で継続表示します。")
