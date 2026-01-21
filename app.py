import json
from datetime import date
import pandas as pd
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery

# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

# BigQuery objects (EDIT HERE if needed)
VIEW_SALES_FACT = "salesdb-479915.sales_data.v_sales_fact_fy_norm"
VIEW_NEW_DELIVERIES_STAFF_SUMMARY = "salesdb-479915.sales_data.jp_new_deliveries_realized_staff_period_summary"

APP_TITLE = "SFA（Streamlit × BigQuery）"

# =========================
# UI
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

st.caption(
    "✅ Standard SQL 強制 / Secrets の Service Account 読み込み / Streamlit cache 設計（credsをhashしない）"
)

# =========================
# Helpers: Secrets -> Credentials -> BigQuery Client
# =========================
def _load_sa_info_from_secrets() -> dict:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Streamlit Secrets に 'gcp_service_account' がありません。")

    sa = st.secrets["gcp_service_account"]

    # st.secrets は dict-like だが、念のため普通の dict に落とす
    sa_info = {k: sa[k] for k in sa.keys()}

    # private_key の改行を保証（たまに \n が崩れる対策）
    if "private_key" in sa_info and isinstance(sa_info["private_key"], str):
        sa_info["private_key"] = sa_info["private_key"].replace("\\n", "\n")

    return sa_info


@st.cache_resource(show_spinner=False)
def get_credentials_and_client():
    sa_info = _load_sa_info_from_secrets()

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)
    return creds, client


def _ensure_standard_sql(sql: str) -> str:
    if not sql.lstrip().startswith("#standardSQL"):
        return "#standardSQL\n" + sql
    return sql


@st.cache_data(show_spinner=False, ttl=300)
def run_query_cached(sql: str, params_json: str = "") -> pd.DataFrame:
    """
    cache_dataに乗せるため、引数は hashable（str）だけにする
    """
    _, client = get_credentials_and_client()

    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,       # ★最重要：Standard SQL 強制
    )

    params = {}
    if params_json:
        params = json.loads(params_json)

    # Query Parameters（必要な時だけ）
    if params:
        qps = []
        for k, v in params.items():
            # 型推定：必要に応じて拡張OK
            if isinstance(v, bool):
                qps.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
            elif isinstance(v, int):
                qps.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            elif isinstance(v, float):
                qps.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
            else:
                # date を文字列で渡す場合もあるので STRING に寄せる
                qps.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
        job_config.query_parameters = qps

    sql2 = _ensure_standard_sql(sql)
    df = client.query(sql2, job_config=job_config).to_dataframe()
    return df


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    params_json = json.dumps(params or {}, ensure_ascii=False)
    return run_query_cached(sql, params_json=params_json)


# =========================
# Sidebar: Filters
# =========================
with st.sidebar:
    st.header("フィルタ")

    screen_mode = st.radio(
        "画面モード",
        ["実データ表示", "診断用"],
        index=0,
    )

    st.divider()

    # 粒度（あなたの新規納品ビューに period がある前提）
    period_grain = st.selectbox("期間粒度", ["day", "week", "month"], index=2)

    periods = st.selectbox(
        "表示する期間数（最新から）",
        [5, 10, 20, 30, 60],
        index=0,
    )

    st.caption("※ month=月初 / week=週初 / day=日付")

# =========================
# Diagnostics
# =========================
if screen_mode == "診断用":
    st.subheader("BigQuery 接続 診断（Streamlit Secrets / Service Account）")

    # STEP0
    st.write("### STEP0: Secrets 読み取り確認")
    st.write("secrets keys:", list(st.secrets.keys()))
    has_gcp = "gcp_service_account" in st.secrets
    st.write("has gcp_service_account:", has_gcp)

    if not has_gcp:
        st.stop()

    # STEP1
    st.write("### STEP1: Credentials 生成")
    try:
        creds, client = get_credentials_and_client()
        st.success("STEP1 OK: Credentials を生成できました。")
        st.write("client_email:", getattr(creds, "service_account_email", "(unknown)"))
        st.write("project_id:", PROJECT_ID)
    except Exception as e:
        st.error("STEP1 FAILED")
        st.exception(e)
        st.stop()

    # STEP2
    st.write("### STEP2: BigQuery Client 生成")
    try:
        st.success("STEP2 OK: bigquery.Client を生成できました。")
        st.write("client.project:", client.project)
        st.write("location:", BQ_LOCATION)
    except Exception as e:
        st.error("STEP2 FAILED")
        st.exception(e)
        st.stop()

    # STEP3
    st.write("### STEP3: 最小クエリ（SELECT 1）")
    try:
        df_min = run_query("SELECT 1 AS ok")
        st.success("STEP3 OK: クエリ成功")
        st.dataframe(df_min, use_container_width=True)
    except Exception as e:
        st.error("STEP3 FAILED: クエリ実行で失敗しました")
        st.exception(e)

    st.stop()

# =========================
# Main: Sales Summary
# =========================
st.subheader("売上サマリー")

# period_grain → DATE_TRUNC の式（SQLに直接入れるので whitelist で安全に）
TRUNC_EXPR = {
    "day": "DATE(sales_date)",
    "week": "DATE_TRUNC(sales_date, WEEK(MONDAY))",
    "month": "DATE_TRUNC(sales_date, MONTH)",
}

period_expr = TRUNC_EXPR[period_grain]

# 最新期間 N ぶんの売上サマリー（全体）
sql_sales_summary = f"""
WITH base AS (
  SELECT
    {period_expr} AS period_date,
    sales_amount,
    gross_profit,
    quantity,
    customer_code
  FROM `{VIEW_SALES_FACT}`
),
periods AS (
  SELECT DISTINCT period_date
  FROM base
  ORDER BY period_date DESC
  LIMIT {int(periods)}
)
SELECT
  period_date,
  COUNT(DISTINCT customer_code) AS customers,
  SUM(quantity) AS qty_sum,
  SUM(sales_amount) AS sales_sum,
  SUM(gross_profit) AS gp_sum,
  SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gross_margin
FROM base
WHERE period_date IN (SELECT period_date FROM periods)
GROUP BY period_date
ORDER BY period_date DESC
"""

try:
    df_sales = run_query(sql_sales_summary)
    if df_sales.empty:
        st.warning("売上サマリー：データがありません（期間や元VIEWを確認してください）")
    else:
        col1, col2, col3, col4 = st.columns(4)
        latest = df_sales.iloc[0]
        col1.metric("最新期間 売上", f"{latest['sales_sum']:,.0f}")
        col2.metric("最新期間 粗利", f"{latest['gp_sum']:,.0f}")
        col3.metric("最新期間 粗利率", f"{latest['gross_margin']*100:,.2f}%")
        col4.metric("最新期間 得意先数", f"{int(latest['customers']):,}")

        st.dataframe(df_sales, use_container_width=True)

        st.download_button(
            "売上サマリーCSVをダウンロード",
            df_sales.to_csv(index=False).encode("utf-8-sig"),
            file_name="sales_summary.csv",
            mime="text/csv",
        )
except Exception as e:
    st.error("売上サマリー：BigQueryクエリで失敗しました。ログを確認してください。")
    st.exception(e)

st.divider()

# =========================
# Main: New Deliveries Realized (Staff)
# =========================
st.subheader("新規納品（Realized）— 最新担当者別")
st.caption("定義：売上事実 × 直近365日未売上（得意先×YJ） → 新規納品として計上（OS②）")

# ここが「period が無い」と落ちてたポイント：
# → このVIEW（jp_new_deliveries...）は period 列がある想定で where に period を使う
sql_new_deliveries_staff = f"""
WITH base AS (
  SELECT
    period,
    period_date,
    branch_name,
    staff_code,
    staff_name,
    realized_customers,
    realized_customer_items,
    realized_items,
    realized_rows,
    sales_sum,
    gp_sum,
    gross_margin,
    qty_sum
  FROM `{VIEW_NEW_DELIVERIES_STAFF_SUMMARY}`
  WHERE period = @period
),
periods AS (
  SELECT DISTINCT period_date
  FROM base
  ORDER BY period_date DESC
  LIMIT {int(periods)}
)
SELECT
  period,
  period_date,
  branch_name,
  staff_code,
  staff_name,
  realized_customers,
  realized_customer_items,
  realized_items,
  realized_rows,
  sales_sum,
  gp_sum,
  gross_margin,
  qty_sum
FROM base
WHERE period_date IN (SELECT period_date FROM periods)
ORDER BY period_date DESC, branch_name, staff_code
"""

try:
    df_nd = run_query(sql_new_deliveries_staff, params={"period": period_grain})

    if df_nd.empty:
        st.warning("新規納品（担当者別）：データがありません（view名/period値を確認）")
    else:
        col1, col2, col3, col4 = st.columns(4)
        latest_date = df_nd["period_date"].max()
        df_latest = df_nd[df_nd["period_date"] == latest_date]

        col1.metric("最新期間（period_date）", str(latest_date))
        col2.metric("最新期間 新規得意先数合計", f"{df_latest['realized_customers'].sum():,}")
        col3.metric("最新期間 新規行数合計", f"{df_latest['realized_rows'].sum():,}")
        col4.metric("最新期間 売上合計", f"{df_latest['sales_sum'].sum():,.0f}")

        st.dataframe(df_nd, use_container_width=True)

        st.download_button(
            "新規納品（担当者別）CSVをダウンロード",
            df_nd.to_csv(index=False).encode("utf-8-sig"),
            file_name="new_deliveries_realized_staff_summary.csv",
            mime="text/csv",
        )

except Exception as e:
    st.error("集計（最新担当者別）：BigQueryクエリで失敗しました。ログを確認してください。")
    st.exception(e)

st.divider()

# =========================
# Notes
# =========================
with st.expander("メモ（運用）"):
    st.write("- `ROWS` エラーは Legacy SQL 実行が原因。ここでは `use_legacy_sql=False` で強制しています。")
    st.write("- 画面表示は日本語に寄せています（OS追記の要件に対応）。")
    st.write("- `period` 列が無い VIEW を叩くと `Unrecognized name: period` になります。VIEW名を確認してください。")
