import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, Forbidden, BadRequest

# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
DATASET_ID = "sales_data"

# 使うVIEW（あなたの現状に合わせて固定）
VIEW_UNIFIED_KPI = f"{PROJECT_ID}.{DATASET_ID}.v_new_adoption_kpi_unified"
VIEW_CUSTOMER_SUMMARY = f"{PROJECT_ID}.{DATASET_ID}.v_new_adoption_customer_summary"
VIEW_DAILY_DRILL = f"{PROJECT_ID}.{DATASET_ID}.v_new_adoption_daily_drill"


# =========================
# BigQuery Client
# =========================
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """
    Streamlit Cloud の Secrets に以下の形式で入っている前提:
    [gcp_service_account]
    type = "service_account"
    project_id = "..."
    private_key_id = "..."
    private_key = """-----BEGIN PRIVATE KEY-----
    ...
    -----END PRIVATE KEY-----"""
    client_email = "..."
    ...
    """
    sa = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=sa.get("project_id", PROJECT_ID), credentials=creds)


@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    client = get_bq_client()
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=False)


def show_bq_error(e: Exception):
    # Streamlit Cloud は詳細が伏せられるので、ユーザーが見て判断できる最低限を表示
    st.error("BigQuery 実行でエラーが出ました。")
    st.code(str(e))


# =========================
# SQL Builders
# =========================
def sql_months() -> str:
    # month / month_ym の揺れ対策：customer_summary 側から取る（あなたの結果に month がある）
    return f"""
    SELECT DISTINCT
      CAST(month AS STRING) AS month_ym
    FROM `{VIEW_CUSTOMER_SUMMARY}`
    ORDER BY month_ym DESC
    """


def sql_kpi_month(month_ym: str) -> str:
    # v_new_adoption_kpi_unified から Target/Realized/Conversion を集計
    # - yj_code 空白は NO_YJ として扱う（空白は活かす方針は維持しつつ KPI では除外）
    # - conversion は items と customers の2軸
    return f"""
    WITH base AS (
      SELECT
        mode,
        -- month_ym がある前提（あなたの最新結果で month_ym を出している）
        CAST(month_ym AS STRING) AS month_ym,
        CAST(customer_code AS STRING) AS customer_code,
        CAST(yj_code AS STRING) AS yj_code,
        COALESCE(NULLIF(CAST(yj_code AS STRING), ''), 'NO_YJ') AS yj_code_norm,
        CAST(sales_amount AS FLOAT64) AS sales_amount,
        CAST(gross_profit AS FLOAT64) AS gross_profit
      FROM `{VIEW_UNIFIED_KPI}`
      WHERE CAST(month_ym AS STRING) = '{month_ym}'
    ),
    agg AS (
      SELECT
        month_ym,

        -- TARGET
        COUNT(DISTINCT IF(mode='target', customer_code, NULL)) AS target_customers,
        COUNT(DISTINCT IF(mode='target' AND yj_code_norm <> 'NO_YJ', CONCAT(customer_code,'_',yj_code_norm), NULL)) AS target_items,
        COUNTIF(mode='target' AND yj_code_norm='NO_YJ') AS target_no_yj_rows,

        -- REALIZED
        COUNT(DISTINCT IF(mode='realized', customer_code, NULL)) AS realized_customers,
        COUNT(DISTINCT IF(mode='realized', CONCAT(customer_code,'_',yj_code_norm), NULL)) AS realized_items,
        SUM(IF(mode='realized', sales_amount, 0.0)) AS realized_sales_sum,
        SUM(IF(mode='realized', gross_profit, 0.0)) AS realized_gp_sum
      FROM base
      GROUP BY month_ym
    )
    SELECT
      month_ym,

      target_items,
      target_customers,
      target_no_yj_rows,
      SAFE_DIVIDE(target_no_yj_rows, NULLIF(target_items + target_no_yj_rows, 0)) AS target_no_yj_ratio,

      realized_items,
      realized_customers,
      realized_sales_sum,
      realized_gp_sum,
      SAFE_DIVIDE(realized_gp_sum, NULLIF(realized_sales_sum, 0)) AS realized_gross_margin,

      SAFE_DIVIDE(realized_items, NULLIF(target_items, 0)) AS conversion_items,
      SAFE_DIVIDE(realized_customers, NULLIF(target_customers, 0)) AS conversion_customers
    FROM agg
    """


def sql_customer_summary(month_ym: str, mode: str) -> str:
    # month の型は 'YYYY-MM' 想定（あなたの結果が 2026-01）
    return f"""
    SELECT
      mode,
      CAST(month AS STRING) AS month_ym,
      customer_code,
      customer_name,
      new_items_cnt,
      sales_amount,
      gross_profit,
      gross_margin,
      first_sales_date_in_month
    FROM `{VIEW_CUSTOMER_SUMMARY}`
    WHERE CAST(month AS STRING) = '{month_ym}'
      AND mode = '{mode}'
    ORDER BY sales_amount DESC, gross_profit DESC
    """


def sql_daily_drill(month_ym: str, customer_code: str | None) -> str:
    where_customer = ""
    if customer_code and customer_code != "ALL":
        where_customer = f" AND CAST(customer_code AS STRING) = '{customer_code}' "

    return f"""
    SELECT
      mode,
      CAST(month AS STRING) AS month_ym,
      sales_date,
      customer_code,
      customer_name,
      yj_code,
      sales_amount,
      gross_profit,
      gross_margin,
      quantity,
      first_sales_date_in_month,
      ingredient,
      item_name,
      product_name,
      branch_name,
      staff_name
    FROM `{VIEW_DAILY_DRILL}`
    WHERE CAST(month AS STRING) = '{month_ym}'
      AND mode = 'realized'
      {where_customer}
    ORDER BY sales_date DESC, sales_amount DESC, gross_profit DESC
    """


# =========================
# UI
# =========================
st.set_page_config(page_title="New Adoption Dashboard", layout="wide")

st.title("📊 New Adoption Dashboard")
st.caption("Target / Realized / Conversion（月次） — BigQuery VIEW 参照のみ")

# ---- Sidebar
with st.sidebar:
    st.header("Filters")
    try:
        months_df = run_query(sql_months())
        months = months_df["month_ym"].dropna().astype(str).tolist()
    except Exception as e:
        show_bq_error(e)
        st.stop()

    if not months:
        st.warning("month_ym が取得できません。VIEW の month 列/データを確認してください。")
        st.stop()

    default_month = months[0]
    month_ym = st.selectbox("month_ym (YYYY-MM)", options=months, index=0)

# ---- Main
try:
    kpi_df = run_query(sql_kpi_month(month_ym))
except (NotFound, Forbidden, BadRequest) as e:
    show_bq_error(e)
    st.stop()
except Exception as e:
    show_bq_error(e)
    st.stop()

if kpi_df.empty:
    st.warning("KPI が空です。month_ym、または v_new_adoption_kpi_unified の列（month_ym）を確認してください。")
    st.stop()

k = kpi_df.iloc[0].to_dict()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Target（品目）", f"{int(k.get('target_items') or 0):,}")
c2.metric("Target（得意先）", f"{int(k.get('target_customers') or 0):,}")
c3.metric("Realized（品目）", f"{int(k.get('realized_items') or 0):,}")
c4.metric("Realized（得意先）", f"{int(k.get('realized_customers') or 0):,}")

c5, c6, c7, c8 = st.columns(4)
c5.metric("Realized 売上", f"{float(k.get('realized_sales_sum') or 0):,.0f}")
c6.metric("Realized 粗利", f"{float(k.get('realized_gp_sum') or 0):,.0f}")
c7.metric("Realized 粗利率", f"{float(k.get('realized_gross_margin') or 0):.3f}")
c8.metric("転換率（品目 / 得意先）", f"{float(k.get('conversion_items') or 0):.3f} / {float(k.get('conversion_customers') or 0):.3f}")

with st.expander("KPI 生データ（確認用）", expanded=False):
    st.dataframe(kpi_df, use_container_width=True)

tab1, tab2 = st.tabs(["🏢 得意先サマリー", "📅 日次ドリル（Realized）"])

with tab1:
    colA, colB = st.columns([1, 3])
    with colA:
        mode = st.radio("mode", options=["realized", "target"], horizontal=True)

    try:
        df_cs = run_query(sql_customer_summary(month_ym, mode))
    except Exception as e:
        show_bq_error(e)
        st.stop()

    st.subheader(f"得意先サマリー（{mode} / {month_ym}）")
    st.dataframe(df_cs, use_container_width=True)

with tab2:
    # realized の customer_code を候補に
    try:
        df_realized_customers = run_query(f"""
        SELECT DISTINCT CAST(customer_code AS STRING) AS customer_code, customer_name
        FROM `{VIEW_CUSTOMER_SUMMARY}`
        WHERE CAST(month AS STRING) = '{month_ym}'
          AND mode = 'realized'
        ORDER BY customer_code
        """)
    except Exception as e:
        show_bq_error(e)
        st.stop()

    customer_options = ["ALL"]
    if not df_realized_customers.empty:
        customer_options += df_realized_customers["customer_code"].astype(str).tolist()

    customer_code = st.selectbox("customer_code（任意）", options=customer_options, index=0)

    try:
        df_dd = run_query(sql_daily_drill(month_ym, customer_code))
    except Exception as e:
        show_bq_error(e)
        st.stop()

    st.subheader(f"日次ドリル（realized / {month_ym}）")
    st.dataframe(df_dd, use_container_width=True)

st.caption("※ 参照先：v_new_adoption_kpi_unified / v_new_adoption_customer_summary / v_new_adoption_daily_drill")
