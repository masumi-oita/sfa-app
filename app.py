# app.py
# ======================================================================
# SFA Dashboard (Streamlit) - BigQuery Connected
# - 売上サマリー（OS①）
# - 新規納品（Realized）最新担当者別（OS②）
# - BigQuery 接続診断（Secrets / Service Account）
# ======================================================================

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account


# =========================
# CONFIG
# =========================
TZ = "Asia/Tokyo"

# あなたのプロジェクト/データセット
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"

# 参照ビュー（存在しているものに合わせて変更）
# - 売上サマリー用（あなたの棚卸結果で存在：v_sales_fact_fy_norm）
VIEW_SALES_FACT = f"{PROJECT_ID}.{DATASET}.v_sales_fact_fy_norm"

# - 新規納品 Realized 日次 Fact（あなたが作成済み）
VIEW_REALIZED_DAILY_FACT = f"{PROJECT_ID}.{DATASET}.v_new_deliveries_realized_daily_fact_all_months"

APP_TITLE = "SFA（Streamlit）— 売上サマリー / 新規納品（Realized）"
CACHE_TTL_SEC = 300  # 5分


# =========================
# Utilities
# =========================
def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_df(rows: Iterable[bigquery.table.Row]) -> pd.DataFrame:
    """pyarrow依存を避けるため、Row iterator を pandas に変換する安全ルート"""
    rows_list = list(rows)
    if not rows_list:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows_list])


def bq_params(params: Dict[str, Any]) -> List[bigquery.ScalarQueryParameter]:
    out: List[bigquery.ScalarQueryParameter] = []
    for k, v in params.items():
        # BigQuery の型推論を安定させたいので最低限だけ明示
        if isinstance(v, int):
            out.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        elif isinstance(v, float):
            out.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
        else:
            out.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return out


# =========================
# Credentials / Client
# =========================
def get_sa_info_from_secrets() -> Dict[str, Any]:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Streamlit Secrets に gcp_service_account が存在しません。")
    # st.secrets は TOML -> dict になっている
    sa = dict(st.secrets["gcp_service_account"])
    return sa


def create_credentials() -> service_account.Credentials:
    sa = get_sa_info_from_secrets()

    # 最低限チェック
    required = ["type", "project_id", "private_key", "client_email", "token_uri"]
    missing = [k for k in required if k not in sa or not sa[k]]
    if missing:
        raise RuntimeError(f"Service Account Secrets に不足キーがあります: {missing}")

    creds = service_account.Credentials.from_service_account_info(
        sa,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return creds


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    creds = create_credentials()
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    return client


# =========================
# Query Runner (cached)
# =========================
@st.cache_data(ttl=CACHE_TTL_SEC)
def run_query(_sql: str, _params_json: str) -> pd.DataFrame:
    """
    st.cache_data で BigQuery結果をキャッシュ
    - creds/client は cache_resource で保持
    - cache_data に Credentials を渡さない（Unhashable 回避）
    """
    client = get_bq_client()
    params = json.loads(_params_json) if _params_json else {}
    job_config = bigquery.QueryJobConfig(query_parameters=bq_params(params))

    job = client.query(_sql, job_config=job_config)
    rows = job.result()
    return safe_df(rows)


# =========================
# SQL Builders
# =========================
def period_date_expr(grain: str) -> str:
    """
    BigQuery 用 period_date 式
    grain: day|week|month|fy
    """
    grain = grain.lower()
    if grain == "day":
        return "DATE_TRUNC(sales_date, DAY)"
    if grain == "week":
        return "DATE_TRUNC(sales_date, WEEK(MONDAY))"
    if grain == "month":
        return "DATE_TRUNC(sales_date, MONTH)"
    if grain == "fy":
        # FY = 4/1 始まり
        return """
        DATE(
          EXTRACT(YEAR FROM sales_date) - IF(EXTRACT(MONTH FROM sales_date) < 4, 1, 0),
          4, 1
        )
        """
    raise ValueError("grain must be one of: day, week, month, fy")


def build_sales_summary_sql(grain: str) -> str:
    """
    売上サマリー（OS①）
    期待カラム（最低限）:
      - sales_date (DATE)
      - branch_name (STRING)  ※無い場合は空文字でもOK
      - staff_code (INT/STRING) / staff_name (STRING)
      - customer_code / customer_name
      - sales_amount (NUMERIC/FLOAT)
      - gross_profit (NUMERIC/FLOAT)
      - quantity (NUMERIC/FLOAT)
    """
    pexpr = period_date_expr(grain)

    return f"""
    WITH base AS (
      SELECT
        CAST(sales_date AS DATE) AS sales_date,
        CAST(branch_name AS STRING) AS branch_name,
        CAST(staff_code AS STRING) AS staff_code,
        CAST(staff_name AS STRING) AS staff_name,
        CAST(customer_code AS STRING) AS customer_code,
        CAST(customer_name AS STRING) AS customer_name,
        CAST(sales_amount AS FLOAT64) AS sales_amount,
        CAST(gross_profit AS FLOAT64) AS gross_profit,
        CAST(quantity AS FLOAT64) AS quantity
      FROM `{VIEW_SALES_FACT}`
      WHERE sales_date IS NOT NULL
    ),
    agg AS (
      SELECT
        {pexpr} AS period_date,
        branch_name,
        staff_code,
        staff_name,
        SUM(sales_amount) AS sales_sum,
        SUM(gross_profit) AS gp_sum,
        SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gross_margin,
        SUM(quantity) AS qty_sum,
        COUNT(DISTINCT customer_code) AS customers
      FROM base
      GROUP BY period_date, branch_name, staff_code, staff_name
    ),
    ranked AS (
      SELECT
        *,
        DENSE_RANK() OVER (ORDER BY period_date DESC) AS period_rank
      FROM agg
    )
    SELECT
      period_date,
      branch_name,
      staff_code,
      staff_name,
      customers,
      sales_sum,
      gp_sum,
      gross_margin,
      qty_sum
    FROM ranked
    WHERE period_rank <= @n_periods
    ORDER BY period_date DESC, branch_name, staff_code
    """


def build_realized_staff_period_sql(grain: str) -> str:
    """
    新規納品（Realized）— 最新担当者別（OS②）
    期待カラム（最低限）:
      - sales_date (DATE)
      - branch_name
      - staff_code / staff_name
      - customer_code / customer_name
      - yj_code
      - sales_amount / gross_profit / quantity
    """
    pexpr = period_date_expr(grain)

    return f"""
    WITH base AS (
      SELECT
        CAST(sales_date AS DATE) AS sales_date,
        CAST(branch_name AS STRING) AS branch_name,
        CAST(staff_code AS STRING) AS staff_code,
        CAST(staff_name AS STRING) AS staff_name,
        CAST(customer_code AS STRING) AS customer_code,
        CAST(customer_name AS STRING) AS customer_name,
        CAST(yj_code AS STRING) AS yj_code,
        CAST(sales_amount AS FLOAT64) AS sales_amount,
        CAST(gross_profit AS FLOAT64) AS gross_profit,
        CAST(quantity AS FLOAT64) AS quantity
      FROM `{VIEW_REALIZED_DAILY_FACT}`
      WHERE sales_date IS NOT NULL
    ),
    agg AS (
      SELECT
        {pexpr} AS period_date,
        branch_name,
        staff_code,
        staff_name,

        COUNT(DISTINCT customer_code) AS realized_customers,
        COUNT(1) AS realized_rows,
        SUM(sales_amount) AS sales_sum,
        SUM(gross_profit) AS gp_sum,
        SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gross_margin,
        SUM(quantity) AS qty_sum
      FROM base
      GROUP BY period_date, branch_name, staff_code, staff_name
    ),
    ranked AS (
      SELECT
        *,
        DENSE_RANK() OVER (ORDER BY period_date DESC) AS period_rank
      FROM agg
    )
    SELECT
      period_date,
      branch_name,
      staff_code,
      staff_name,
      realized_customers,
      realized_rows,
      sales_sum,
      gp_sum,
      gross_margin,
      qty_sum
    FROM ranked
    WHERE period_rank <= @n_periods
    ORDER BY period_date DESC, branch_name, staff_code
    """


# =========================
# UI
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.subheader("フィルタ")

    screen_mode = st.radio(
        "画面モード",
        ["実データ表示", "診断用"],
        index=0,
    )

    st.divider()

    grain = st.selectbox("期間粒度", ["day", "week", "month", "fy"], index=2)
    n_periods = st.number_input("表示する期間数（最新から）", min_value=1, max_value=120, value=5, step=1)

    st.divider()
    st.caption("定義")
    st.caption("新規納品（Realized）＝ 売上事実 × 直近365日未売上（得意先×YJ） → 新規納品として計上（OS②）")


tabs = st.tabs(["売上サマリー（OS①）", "新規納品 Realized（OS②）", "接続診断"])

# -------------------------
# 売上サマリー
# -------------------------
with tabs[0]:
    st.subheader("売上サマリー（担当者別）")

    if screen_mode == "診断用":
        st.info("診断用モード：クエリと件数確認に重点。")

    sql = build_sales_summary_sql(grain)
    params = {"n_periods": int(n_periods)}

    with st.expander("（開発用）SQLを表示", expanded=False):
        st.code(sql, language="sql")
        st.json(params)

    try:
        df = run_query(sql, json.dumps(params, ensure_ascii=False))
        if df.empty:
            st.warning("表示するデータはありません。")
        else:
            # 日本語表示（OS追記：日本語表記）
            df = df.rename(
                columns={
                    "period_date": "期間開始日",
                    "branch_name": "支店名",
                    "staff_code": "担当者コード",
                    "staff_name": "担当者名",
                    "customers": "得意先数",
                    "sales_sum": "売上合計",
                    "gp_sum": "粗利合計",
                    "gross_margin": "粗利率",
                    "qty_sum": "数量合計",
                }
            )
            st.dataframe(df, width="stretch", hide_index=True)

    except Exception as e:
        st.error("BigQueryクエリで失敗しました。ログを確認してください。")
        st.exception(e)


# -------------------------
# 新規納品 Realized
# -------------------------
with tabs[1]:
    st.subheader("新規納品（Realized）— 最新担当者別")
    st.caption("定義：売上事実 × 直近365日未売上（得意先×YJ） → 新規納品として計上（OS②）")

    sql = build_realized_staff_period_sql(grain)
    params = {"n_periods": int(n_periods)}

    with st.expander("（開発用）SQLを表示", expanded=False):
        st.code(sql, language="sql")
        st.json(params)

    try:
        df = run_query(sql, json.dumps(params, ensure_ascii=False))
        if df.empty:
            st.warning("表示するデータはありません。")
        else:
            df = df.rename(
                columns={
                    "period_date": "期間開始日",
                    "branch_name": "支店名",
                    "staff_code": "担当者コード",
                    "staff_name": "担当者名",
                    "realized_customers": "新規得意先数",
                    "realized_rows": "新規行数",
                    "sales_sum": "売上合計",
                    "gp_sum": "粗利合計",
                    "gross_margin": "粗利率",
                    "qty_sum": "数量合計",
                }
            )
            st.dataframe(df, width="stretch", hide_index=True)

    except Exception as e:
        st.error("BigQueryクエリで失敗しました。ログを確認してください。")
        st.exception(e)


# -------------------------
# 接続診断
# -------------------------
with tabs[2]:
    st.subheader("BigQuery 接続 診断（Streamlit Secrets / Service Account）")

    st.write("STEP0: Secrets 読み取り確認")
    try:
        keys = list(st.secrets.keys())
        st.write("secrets keys:")
        st.code(json.dumps(keys, ensure_ascii=False, indent=2))
        st.write(f"has gcp_service_account: {'gcp_service_account' in st.secrets}")
        st.success("STEP0 OK: Secrets の読み取りはOK（キー一覧のみ表示）")
    except Exception as e:
        st.error("STEP0 FAILED")
        st.exception(e)
        st.stop()

    st.write("STEP1: Credentials 生成")
    try:
        creds = create_credentials()
        # 安全のため、表示は最低限（メール/プロジェクトのみ）
        st.success("STEP1 OK: Credentials を生成できました。")
        st.write(f"client_email: {get_sa_info_from_secrets().get('client_email', '')}")
        st.write(f"project_id: {get_sa_info_from_secrets().get('project_id', '')}")
    except Exception as e:
        st.error("STEP1 FAILED")
        st.exception(e)
        st.stop()

    st.write("STEP2: BigQuery Client 生成")
    try:
        client = get_bq_client()
        st.success("STEP2 OK: bigquery.Client を生成できました。")
        st.write(f"client.project: {client.project}")
    except Exception as e:
        st.error("STEP2 FAILED")
        st.exception(e)
        st.stop()

    st.write("STEP3: 最小クエリ（SELECT 1）")
    try:
        sql_min = "#standardSQL\nSELECT 1 AS ok"
        df_min = run_query(sql_min, json.dumps({}, ensure_ascii=False))
        st.success("STEP3 OK: クエリ成功")
        st.dataframe(df_min, width="content", hide_index=True)
    except Exception as e:
        st.error("STEP3 FAILED: クエリ実行で失敗しました。")
        st.write("よくある原因：① IAM権限不足(403) ② ネットワーク/TransportError ③ token/SAの不整合")
        st.exception(e)

    st.caption(f"診断実行: {now_utc_str()}")
