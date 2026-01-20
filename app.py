# app.py
# =========================================
# New Adoption Dashboard (Target / Realized / Conversion)
# BigQuery + Streamlit
# =========================================

from __future__ import annotations

import json
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, Forbidden, BadRequest


# -----------------------------
# CONFIG (edit if needed)
# -----------------------------
PROJECT_ID = "salesdb-479915"
DATASET_ID = "sales_data"
BQ_LOCATION = "asia-northeast1"

# Views you already created
VIEW_TARGET = f"`{PROJECT_ID}.{DATASET_ID}.v_target_new_adoption`"
VIEW_REALIZED_KPI = f"`{PROJECT_ID}.{DATASET_ID}.v_realized_new_adoption_kpi`"

# (Optional) If you have unified KPI view, you can use it
# VIEW_KPI_UNIFIED = f"`{PROJECT_ID}.{DATASET_ID}.v_new_adoption_kpi_unified`"


# -----------------------------
# Streamlit page
# -----------------------------
st.set_page_config(
    page_title="New Adoption Dashboard",
    layout="wide",
)

st.title("📊 New Adoption Dashboard")
st.caption("Target / Realized / Conversion（月次）")


# -----------------------------
# Helpers
# -----------------------------
def _read_service_account_from_secrets() -> Dict[str, Any]:
    """
    Streamlit Secrets TOML should contain:

    [gcp_service_account]
    type = "service_account"
    project_id = "..."
    private_key_id = "..."
    private_key = """-----BEGIN PRIVATE KEY----- ... -----END PRIVATE KEY-----"""
    client_email = "..."
    token_uri = "https://oauth2.googleapis.com/token"
    ...
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError(
            "st.secrets に [gcp_service_account] がありません。"
            "Streamlit Cloud の Secrets を設定してください。"
        )

    sa = dict(st.secrets["gcp_service_account"])

    # Guard: private_key must be PEM, not JSON blob
    pk = sa.get("private_key", "")
    if "{\n" in pk or "\"type\"" in pk or pk.strip().startswith("{"):
        raise RuntimeError(
            "Secrets の private_key に JSON が混入しています。"
            "private_key は PEM（BEGIN/END PRIVATE KEY）のみを貼ってください。"
        )
    if "BEGIN PRIVATE KEY" not in pk or "END PRIVATE KEY" not in pk:
        raise RuntimeError(
            "Secrets の private_key が PEM 形式ではありません。BEGIN/END が必要です。"
        )

    return sa


@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    sa = _read_service_account_from_secrets()
    creds = service_account.Credentials.from_service_account_info(sa)
    project = sa.get("project_id") or PROJECT_ID
    return bigquery.Client(project=project, credentials=creds, location=BQ_LOCATION)


@st.cache_data(show_spinner=False, ttl=300)
def run_query(sql: str, params: Optional[list[bigquery.ScalarQueryParameter]] = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = client.query(sql, job_config=job_config, location=BQ_LOCATION)
    return job.result().to_dataframe(create_bqstorage_client=False)


def fmt_yen(x: float) -> str:
    try:
        return f"¥{int(round(x)):,}"
    except Exception:
        return ""


def fmt_pct(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return ""


# -----------------------------
# SQL: months list (from target + realized)
# -----------------------------
sql_months = f"""
WITH t AS (
  SELECT DISTINCT
    SAFE_CAST(month_ym AS STRING) AS month_ym
  FROM (
    SELECT
      FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m', month_ym)) AS month_ym
    FROM (
      SELECT
        FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m', month_ym)) AS month_ym
      FROM (
        SELECT
          -- target view has month_ym or month? まず両対応で吸収
          COALESCE(
            SAFE_CAST(month_ym AS STRING),
            SAFE_CAST(month AS STRING)
          ) AS month_ym
        FROM {VIEW_TARGET}
      )
      WHERE month_ym IS NOT NULL AND month_ym <> ""
    )
  )
),
r AS (
  SELECT DISTINCT
    FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) AS month_ym
  FROM {VIEW_REALIZED_KPI}
  WHERE first_sales_date_in_month IS NOT NULL
),
u AS (
  SELECT month_ym FROM t
  UNION DISTINCT
  SELECT month_ym FROM r
)
SELECT month_ym
FROM u
WHERE month_ym IS NOT NULL AND month_ym <> ""
ORDER BY month_ym DESC
"""


# -----------------------------
# UI: sidebar month filter
# -----------------------------
try:
    months_df = run_query(sql_months)
    months = months_df["month_ym"].dropna().astype(str).tolist()
except Exception as e:
    st.error("month_ym の候補取得で失敗しました。")
    st.exception(e)
    st.stop()

if not months:
    st.warning("表示できる month_ym がありません（Target/Realized にデータがない可能性）。")
    st.stop()

default_month = months[0]
month_ym = st.sidebar.selectbox("month_ym (YYYY-MM)", months, index=0)

st.sidebar.markdown("---")
st.sidebar.caption(f"BQ location: `{BQ_LOCATION}`")


# -----------------------------
# SQL: Target monthly KPI
# -----------------------------
# target view: mode=target, month_ym exists in your latest outputs.
# columns you showed:
# month_ym, target_rows, new_customers_cnt, new_items_cnt, no_yj_cnt, no_yj_ratio
sql_target_month = f"""
WITH base AS (
  SELECT
    COALESCE(SAFE_CAST(month_ym AS STRING), SAFE_CAST(month AS STRING)) AS month_ym,
    SAFE_CAST(customer_code AS STRING) AS customer_code,
    SAFE_CAST(yj_code_norm AS STRING) AS yj_code_norm,
    SAFE_CAST(customer_yj_key AS STRING) AS customer_yj_key
  FROM `{PROJECT_ID}.{DATASET_ID}.v_new_adoption_kpi_unified`
  WHERE mode = 'target'
)
SELECT
  month_ym,
  COUNT(1) AS target_rows,
  COUNT(DISTINCT customer_code) AS new_customers_cnt,
  COUNT(DISTINCT customer_yj_key) AS new_items_cnt,
  SUM(CASE WHEN yj_code_norm = 'NO_YJ' THEN 1 ELSE 0 END) AS no_yj_cnt,
  SAFE_DIVIDE(
    SUM(CASE WHEN yj_code_norm = 'NO_YJ' THEN 1 ELSE 0 END),
    COUNT(1)
  ) AS no_yj_ratio
FROM base
WHERE month_ym = @month_ym
GROUP BY month_ym
"""

# -----------------------------
# SQL: Realized monthly KPI (from realized_kpi view)
# realized_kpi columns:
# mode, customer_code, customer_name, yj_code, sales_date, sales_amount, gross_profit, gross_margin,
# quantity, first_sales_date_in_month, yj_code_norm
sql_realized_month = f"""
SELECT
  FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) AS month_ym,
  COUNT(1) AS realized_rows,
  COUNT(DISTINCT customer_code) AS realized_customers,
  COUNT(DISTINCT CONCAT(customer_code, '_', yj_code_norm)) AS realized_items,
  SUM(sales_amount) AS realized_sales_sum,
  SUM(gross_profit) AS realized_gp_sum,
  SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS realized_gross_margin
FROM {VIEW_REALIZED_KPI}
WHERE FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) = @month_ym
GROUP BY month_ym
"""

# -----------------------------
# SQL: Conversion summary (target vs realized)
# conversion_items = realized_items / target_items
# conversion_customers = realized_customers / target_customers
sql_conversion_month = f"""
WITH t AS (
  SELECT
    COALESCE(SAFE_CAST(month_ym AS STRING), SAFE_CAST(month AS STRING)) AS month_ym,
    SAFE_CAST(customer_code AS STRING) AS customer_code,
    SAFE_CAST(yj_code_norm AS STRING) AS yj_code_norm,
    SAFE_CAST(customer_yj_key AS STRING) AS customer_yj_key
  FROM `{PROJECT_ID}.{DATASET_ID}.v_new_adoption_kpi_unified`
  WHERE mode = 'target'
),
t2 AS (
  SELECT
    month_ym,
    COUNT(DISTINCT customer_yj_key) AS target_items,
    COUNT(DISTINCT customer_code) AS target_customers
  FROM t
  WHERE month_ym = @month_ym
    AND yj_code_norm <> 'NO_YJ'
  GROUP BY month_ym
),
r2 AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) AS month_ym,
    COUNT(DISTINCT CONCAT(customer_code, '_', yj_code_norm)) AS realized_items,
    COUNT(DISTINCT customer_code) AS realized_customers,
    SUM(sales_amount) AS realized_sales_sum,
    SUM(gross_profit) AS realized_gp_sum
  FROM {VIEW_REALIZED_KPI}
  WHERE FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) = @month_ym
  GROUP BY month_ym
)
SELECT
  t2.month_ym,
  t2.target_items,
  t2.target_customers,
  r2.realized_items,
  r2.realized_customers,
  r2.realized_sales_sum,
  r2.realized_gp_sum,
  SAFE_DIVIDE(r2.realized_items, NULLIF(t2.target_items, 0)) AS conversion_items,
  SAFE_DIVIDE(r2.realized_customers, NULLIF(t2.target_customers, 0)) AS conversion_customers
FROM t2
LEFT JOIN r2 USING (month_ym)
"""

# -----------------------------
# SQL: Daily drill (Realized)
# -----------------------------
sql_realized_daily = f"""
SELECT
  DATE(sales_date) AS sales_date,
  customer_code,
  customer_name,
  yj_code_norm AS yj_code,
  sales_amount,
  gross_profit,
  gross_margin,
  quantity,
  first_sales_date_in_month
FROM {VIEW_REALIZED_KPI}
WHERE FORMAT_DATE('%Y-%m', DATE_TRUNC(first_sales_date_in_month, MONTH)) = @month_ym
ORDER BY sales_date DESC, sales_amount DESC
LIMIT 2000
"""


# -----------------------------
# Execute queries
# -----------------------------
params = [bigquery.ScalarQueryParameter("month_ym", "STRING", month_ym)]

try:
    df_target = run_query(sql_target_month, params)
    df_realized = run_query(sql_realized_month, params)
    df_conv = run_query(sql_conversion_month, params)
    df_daily = run_query(sql_realized_daily, params)
except (NotFound, Forbidden, BadRequest) as e:
    st.error("BigQueryクエリでエラー（NotFound / Forbidden / BadRequest）")
    st.exception(e)
    st.stop()
except Exception as e:
    st.error("BigQueryクエリ実行中に予期せぬエラー")
    st.exception(e)
    st.stop()


# -----------------------------
# Display: KPI cards
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

# Target summary
if not df_target.empty:
    t_row = df_target.iloc[0].to_dict()
else:
    t_row = {"target_rows": 0, "new_customers_cnt": 0, "new_items_cnt": 0, "no_yj_cnt": 0, "no_yj_ratio": None}

# Realized summary
if not df_realized.empty:
    r_row = df_realized.iloc[0].to_dict()
else:
    r_row = {
        "realized_rows": 0,
        "realized_customers": 0,
        "realized_items": 0,
        "realized_sales_sum": 0.0,
        "realized_gp_sum": 0.0,
        "realized_gross_margin": None,
    }

# Conversion summary
if not df_conv.empty:
    c_row = df_conv.iloc[0].to_dict()
else:
    c_row = {
        "target_items": 0,
        "target_customers": 0,
        "realized_items": 0,
        "realized_customers": 0,
        "realized_sales_sum": 0.0,
        "realized_gp_sum": 0.0,
        "conversion_items": None,
        "conversion_customers": None,
    }

with col1:
    st.metric("Target：新規品目（件）", int(t_row.get("new_items_cnt", 0)))
    st.caption(f"対象行数: {int(t_row.get('target_rows', 0)):,}")

with col2:
    st.metric("Target：新規得意先（件）", int(t_row.get("new_customers_cnt", 0)))
    st.caption(f"NO_YJ: {int(t_row.get('no_yj_cnt', 0)):,} / 比率: {fmt_pct(t_row.get('no_yj_ratio'))}")

with col3:
    st.metric("Realized：売上合計", fmt_yen(float(r_row.get("realized_sales_sum", 0.0))))
    st.caption(f"粗利: {fmt_yen(float(r_row.get('realized_gp_sum', 0.0)))} / 粗利率: {fmt_pct(r_row.get('realized_gross_margin'))}")

with col4:
    st.metric("Conversion（品目）", fmt_pct(c_row.get("conversion_items")))
    st.caption(f"Conversion（得意先）: {fmt_pct(c_row.get('conversion_customers'))}")


st.markdown("---")


# -----------------------------
# Tables (monthly target / realized / conversion)
# -----------------------------
left, right = st.columns([1, 1])

with left:
    st.subheader("① Target（月次）")
    if df_target.empty:
        st.info("Target データがありません。")
    else:
        st.dataframe(df_target, use_container_width=True)

with right:
    st.subheader("② Realized（月次）")
    if df_realized.empty:
        st.info("Realized データがありません。")
    else:
        st.dataframe(df_realized, use_container_width=True)

st.subheader("③ Conversion（月次：Target vs Realized）")
if df_conv.empty:
    st.info("Conversion データがありません。")
else:
    st.dataframe(df_conv, use_container_width=True)


st.markdown("---")


# -----------------------------
# Daily Drilldown table
# -----------------------------
st.subheader("④ Realized（日次ドリル：当月の初回売上を含む明細）")
st.caption("当月内の初出日（first_sales_date_in_month）を含めて確認できます。最大 2000 行。")

if df_daily.empty:
    st.info("日次ドリルデータがありません。")
else:
    # Pretty formats
    df_show = df_daily.copy()
    for col in ["sales_amount", "gross_profit"]:
        if col in df_show.columns:
            df_show[col] = pd.to_numeric(df_show[col], errors="coerce")
    st.dataframe(df_show, use_container_width=True)


# -----------------------------
# Debug panel
# -----------------------------
with st.expander("🔧 Debug（SQL確認 / Secretsチェック）", expanded=False):
    st.write("**Selected month_ym:**", month_ym)
    st.write("**Views:**")
    st.code(f"TARGET: {VIEW_TARGET}\nREALIZED_KPI: {VIEW_REALIZED_KPI}\nLOCATION: {BQ_LOCATION}")
    st.write("**SQL (months):**")
    st.code(sql_months)
    st.write("**SQL (target month):**")
    st.code(sql_target_month)
    st.write("**SQL (realized month):**")
    st.code(sql_realized_month)
    st.write("**SQL (conversion):**")
    st.code(sql_conversion_month)
    st.write("**SQL (daily drill):**")
    st.code(sql_realized_daily)

    try:
        sa = _read_service_account_from_secrets()
        st.success("Secrets: gcp_service_account は読み取りOK（private_key 形式もOK）")
        st.write({k: sa.get(k) for k in ["project_id", "client_email", "private_key_id", "universe_domain"]})
    except Exception as e:
        st.error("Secrets の形式がNGです（private_key JSON混入など）")
        st.exception(e)
