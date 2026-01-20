import os
from datetime import date
import pandas as pd
import streamlit as st
from google.cloud import bigquery

# =========================
# CONFIG
# =========================
PROJECT_ID = os.getenv("BQ_PROJECT", "salesdb-479915")
DATASET = os.getenv("BQ_DATASET", "sales_data")

VIEW_KPI_UNIFIED = os.getenv("VIEW_KPI_UNIFIED", f"{PROJECT_ID}.{DATASET}.v_new_adoption_kpi_unified")
VIEW_CUST_SUMMARY = os.getenv("VIEW_CUST_SUMMARY", f"{PROJECT_ID}.{DATASET}.v_new_adoption_customer_summary")
VIEW_DAILY_DRILL = os.getenv("VIEW_DAILY_DRILL", f"{PROJECT_ID}.{DATASET}.v_new_adoption_daily_drill")

# 認証：
# - ローカル: gcloud auth application-default login でもOK
# - SaaS: GOOGLE_APPLICATION_CREDENTIALS に service account json のパスを設定
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=300)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig()

    if params:
        bq_params = []
        for k, v in params.items():
            # 型推論（必要ならここを厳密に）
            if isinstance(v, int):
                bq_params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            elif isinstance(v, float):
                bq_params.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
            elif isinstance(v, date):
                bq_params.append(bigquery.ScalarQueryParameter(k, "DATE", v))
            else:
                bq_params.append(bigquery.ScalarQueryParameter(k, "STRING", v))
        job_config.query_parameters = bq_params

    return client.query(sql, job_config=job_config).to_dataframe()

# =========================
# UI
# =========================
st.set_page_config(page_title="New Adoption Dashboard", layout="wide")
st.title("New Adoption Dashboard (Target / Realized / Conversion)")

with st.sidebar:
    st.subheader("Filters")
    # month_ym は 'YYYY-MM' 形式の想定
    default_month = "2026-01"
    month_ym = st.text_input("month_ym (YYYY-MM)", value=default_month)
    show_sap = st.checkbox("SAP/処理用を含める（Drillのみ）", value=False)

# =========================
# 1) KPI（月次：target/realized統合）
# =========================
sql_kpi_month = f"""
WITH base AS (
  SELECT
    mode,
    COALESCE(month_ym, FORMAT_DATE('%Y-%m', first_sales_date_in_month)) AS month_ym,
    customer_code,
    yj_code,
    yj_code_norm,
    sales_amount,
    gross_profit
  FROM `{VIEW_KPI_UNIFIED}`
),
agg AS (
  SELECT
    month_ym,

    -- Target
    COUNTIF(mode='target') AS target_rows,
    COUNT(DISTINCT IF(mode='target', customer_code, NULL)) AS target_customers,
    COUNT(DISTINCT IF(mode='target', CONCAT(customer_code, '_', yj_code_norm), NULL)) AS target_items,
    COUNTIF(mode='target' AND yj_code_norm='NO_YJ') AS target_no_yj_cnt,

    -- Realized
    COUNTIF(mode='realized') AS realized_rows,
    COUNT(DISTINCT IF(mode='realized', customer_code, NULL)) AS realized_customers,
    COUNT(DISTINCT IF(mode='realized', CONCAT(customer_code, '_', yj_code_norm), NULL)) AS realized_items,
    SUM(IF(mode='realized', sales_amount, 0)) AS realized_sales_sum,
    SUM(IF(mode='realized', gross_profit, 0)) AS realized_gp_sum
  FROM base
  GROUP BY month_ym
)
SELECT
  month_ym,
  target_rows,
  target_customers,
  target_items,
  target_no_yj_cnt,
  SAFE_DIVIDE(target_no_yj_cnt, NULLIF(target_rows, 0)) AS target_no_yj_ratio,

  realized_rows,
  realized_customers,
  realized_items,
  realized_sales_sum,
  realized_gp_sum,
  SAFE_DIVIDE(realized_gp_sum, NULLIF(realized_sales_sum, 0)) AS realized_gross_margin,

  SAFE_DIVIDE(realized_items, NULLIF(target_items, 0)) AS conversion_items,
  SAFE_DIVIDE(realized_customers, NULLIF(target_customers, 0)) AS conversion_customers
FROM agg
ORDER BY month_ym DESC
"""

df_month = run_query(sql_kpi_month)

# 指定月の行
df_focus = df_month[df_month["month_ym"] == month_ym].copy()

c1, c2 = st.columns([2, 3])
with c1:
    st.subheader("月次サマリー（全期間）")
    st.dataframe(df_month, use_container_width=True)

with c2:
    st.subheader(f"選択月（{month_ym}）KPI")
    if df_focus.empty:
        st.warning("指定月が見つかりません。month_ym を確認してください。")
    else:
        row = df_focus.iloc[0].to_dict()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Target Customers", f"{int(row['target_customers']):,}")
        m1.metric("Target Items", f"{int(row['target_items']):,}")
        m2.metric("Realized Customers", f"{int(row['realized_customers']):,}")
        m2.metric("Realized Items", f"{int(row['realized_items']):,}")
        m3.metric("Realized Sales", f"{row['realized_sales_sum']:,.0f}")
        m3.metric("Realized GP", f"{row['realized_gp_sum']:,.0f}")
        m4.metric("GP Margin", f"{(row['realized_gross_margin'] or 0)*100:.2f}%")
        m4.metric("Conv. Customers", f"{(row['conversion_customers'] or 0)*100:.2f}%")
        st.caption(f"Target NO_YJ ratio: {(row['target_no_yj_ratio'] or 0)*100:.2f}% / Conv. Items: {(row['conversion_items'] or 0)*100:.2f}%")

st.divider()

# =========================
# 2) Realized 得意先サマリー（当月 top）
# =========================
st.subheader(f"Realized 得意先サマリー（{month_ym} / Top）")

sql_cust = f"""
SELECT
  mode,
  month,
  customer_code,
  customer_name,
  new_items_cnt,
  sales_amount,
  gross_profit,
  gross_margin,
  first_sales_date_in_month
FROM `{VIEW_CUST_SUMMARY}`
WHERE mode = 'realized'
  AND month = @month_ym
ORDER BY sales_amount DESC
LIMIT 200
"""

df_cust = run_query(sql_cust, {"month_ym": month_ym})
st.dataframe(df_cust, use_container_width=True)

st.divider()

# =========================
# 3) 日次ドリル（得意先選択）
# =========================
st.subheader("日次ドリル（Realized）")

if df_cust.empty:
    st.info("この月の Realized が無いので日次ドリルは表示できません。")
else:
    cust_options = (
        df_cust[["customer_code", "customer_name"]]
        .drop_duplicates()
        .assign(label=lambda x: x["customer_code"] + "｜" + x["customer_name"])
    )
    selected_label = st.selectbox("得意先を選択", cust_options["label"].tolist())
    selected_code = selected_label.split("｜", 1)[0]

    sql_drill = f"""
    SELECT
      mode,
      sales_date,
      customer_code,
      customer_name,
      yj_code,
      sales_amount,
      gross_profit,
      gross_margin,
      quantity,
      first_sales_date_in_month
    FROM `{VIEW_DAILY_DRILL}`
    WHERE mode = 'daily'
      AND FORMAT_DATE('%Y-%m', first_sales_date_in_month) = @month_ym
      AND customer_code = @customer_code
      {"AND NOT STARTS_WITH(customer_name, 'ＳＡＰ') AND NOT CONTAINS_SUBSTR(customer_name, '処理用')" if not show_sap else ""}
    ORDER BY sales_date DESC, sales_amount DESC
    LIMIT 2000
    """
    df_drill = run_query(sql_drill, {"month_ym": month_ym, "customer_code": selected_code})
    st.dataframe(df_drill, use_container_width=True)

st.caption("※ BigQuery VIEWの列名が違う場合は、上のSQL中の列名をあなたのVIEW定義に合わせて調整してください。")
