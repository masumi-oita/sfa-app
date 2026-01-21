import os
import json
import datetime as dt
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account


# =========================
# Config
# =========================
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"
VIEW_REALIZED = "jp_new_deliveries_realized_staff_period_summary"  # 作成済みVIEW

DEFAULT_TZ = "Asia/Tokyo"


# =========================
# UI
# =========================
st.set_page_config(page_title="SFA | 新規納品（Realized）", layout="wide")
st.title("新規納品（Realized）— 最新担当者別")
st.caption("定義：売上事実 × 直近365日未売上（得意先×YJ） → 新規納品として計上（OS②）")


# =========================
# Helpers: Secrets / Credentials / Client
# =========================
def _get_sa_info_from_secrets() -> Dict[str, Any]:
    """
    Streamlit Secrets の [gcp_service_account] を dict で返す
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Streamlit Secrets に [gcp_service_account] が見つかりません。")

    sa = dict(st.secrets["gcp_service_account"])
    required = [
        "type", "project_id", "private_key_id", "private_key",
        "client_email", "client_id", "token_uri"
    ]
    missing = [k for k in required if k not in sa or not sa[k]]
    if missing:
        raise RuntimeError(f"Secrets の gcp_service_account に不足キーがあります: {missing}")

    # private_key の整形（念のため）
    # - TOML """ """ の中は改行が実改行である必要あり
    # - もし \n を含む形で入ってきた場合は復元
    pk = sa["private_key"]
    if "\\n" in pk and "\n" not in pk:
        sa["private_key"] = pk.replace("\\n", "\n")

    return sa


@st.cache_resource
def get_credentials() -> service_account.Credentials:
    """
    Credentials は resource キャッシュ（hash対象にしない）で保持
    """
    sa_info = _get_sa_info_from_secrets()
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return creds


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """
    BigQuery Client も resource キャッシュで保持
    """
    creds = get_credentials()
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    return client


# =========================
# Helpers: Query (NO to_dataframe)
# =========================
@st.cache_data(ttl=300)
def run_query(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    pyarrow不要ルート：
      client.query -> job.result() -> rows -> DataFrame
    """
    client = get_bq_client()

    job_config = None
    if params:
        bq_params = []
        for k, v in params.items():
            # 型は必要最低限で判定（DATE / INT64 / STRING）
            if isinstance(v, dt.date):
                bq_params.append(bigquery.ScalarQueryParameter(k, "DATE", v))
            elif isinstance(v, int):
                bq_params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            else:
                bq_params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
        job_config = bigquery.QueryJobConfig(query_parameters=bq_params)

    job = client.query(sql, job_config=job_config)
    rows = list(job.result())
    if not rows:
        return pd.DataFrame()

    # rows: Row objects
    cols = rows[0].keys()
    data = [list(r.values()) for r in rows]
    return pd.DataFrame(data, columns=cols)


def diag_block():
    st.subheader("BigQuery 接続 診断（Streamlit Secrets / Service Account）")

    # STEP0
    st.write("### STEP0: Secrets 読み取り確認")
    keys = list(st.secrets.keys())
    st.write("secrets keys:", keys)
    st.write("has gcp_service_account:", "gcp_service_account" in st.secrets)

    # STEP1
    st.write("### STEP1: Credentials 生成")
    try:
        creds = get_credentials()
        st.success("STEP1 OK: Credentials を生成できました。")
        st.write("client_email:", getattr(creds, "service_account_email", "N/A"))
        st.write("project_id:", PROJECT_ID)
    except Exception as e:
        st.error("STEP1 FAILED")
        st.exception(e)
        return

    # STEP2
    st.write("### STEP2: BigQuery Client 生成")
    try:
        client = get_bq_client()
        st.success("STEP2 OK: bigquery.Client を生成できました。")
        st.write("client.project:", client.project)
    except Exception as e:
        st.error("STEP2 FAILED")
        st.exception(e)
        return

    # STEP3
    st.write("### STEP3: 最小クエリ（SELECT 1）")
    try:
        df = run_query("SELECT 1 AS ok")
        st.success("STEP3 OK: クエリ成功")
        st.dataframe(df, width="stretch")
    except Exception as e:
        st.error("STEP3 FAILED")
        st.exception(e)


# =========================
# Sidebar Controls
# =========================
st.sidebar.header("フィルタ")

mode = st.sidebar.radio(
    "画面モード",
    ["実データ表示", "診断用"],
    index=0
)

granularity = st.sidebar.selectbox(
    "期間粒度",
    ["day", "week", "month", "fy"],
    index=2
)

limit_n = st.sidebar.selectbox(
    "表示する期間数（最新から）",
    [5, 10, 20, 60],
    index=0
)

# FY開始（月日固定）
FY_START_MONTH = 4
FY_START_DAY = 1


# =========================
# Main
# =========================
if mode == "診断用":
    diag_block()
    st.stop()


st.subheader("集計（最新担当者別）")

sql = f"""
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
FROM `{PROJECT_ID}.{DATASET}.{VIEW_REALIZED}`
WHERE period = @period
QUALIFY ROW_NUMBER() OVER (PARTITION BY period ORDER BY period_date DESC) <= @limit_n
ORDER BY period_date DESC, sales_sum DESC
"""

# ↑ QUALIFY は「期間ごとに最新N期間」を切るためのもの。
# もし BigQuery の仕様/データ形状で期待通りにならない場合は、後で period_date だけ抽出してINに切り替え可能。

params = {"period": granularity, "limit_n": int(limit_n)}

try:
    df = run_query(sql, params=params)
except Exception as e:
    st.error("BigQueryクエリで失敗しました。ログを確認してください。")
    st.exception(e)
    st.stop()

if df.empty:
    st.info("データがありません（期間粒度・期間数を変えてください）。")
    st.stop()

# 表示用（日本語カラムに寄せる）
rename_map = {
    "period": "粒度",
    "period_date": "期間開始日",
    "branch_name": "支店名",
    "staff_code": "担当者コード",
    "staff_name": "担当者名",
    "realized_customers": "新規納品_得意先数",
    "realized_customer_items": "新規納品_得意先×品目数",
    "realized_items": "新規納品_品目数",
    "realized_rows": "新規納品_行数",
    "sales_sum": "売上合計",
    "gp_sum": "粗利合計",
    "gross_margin": "粗利率",
    "qty_sum": "数量合計",
}
df_disp = df.rename(columns=rename_map)

# 粗利率を%表示用
if "粗利率" in df_disp.columns:
    df_disp["粗利率"] = (df_disp["粗利率"].astype(float) * 100).round(2)

# 主要KPI
col1, col2, col3, col4 = st.columns(4)
col1.metric("行数", f"{df_disp['新規納品_行数'].sum():,.0f}")
col2.metric("売上合計", f"{df_disp['売上合計'].sum():,.0f}")
col3.metric("粗利合計", f"{df_disp['粗利合計'].sum():,.0f}")
avg_margin = (df["gp_sum"].sum() / df["sales_sum"].sum()) if df["sales_sum"].sum() else 0
col4.metric("粗利率（加重）", f"{avg_margin*100:.2f}%")

st.dataframe(df_disp, use_container_width=True)

# 期間別のざっくり集計（同一period_date単位）
st.subheader("期間別サマリー（期間開始日）")
sql2 = f"""
SELECT
  period_date,
  SUM(realized_rows) AS realized_rows,
  SUM(sales_sum) AS sales_sum,
  SUM(gp_sum) AS gp_sum,
  SAFE_DIVIDE(SUM(gp_sum), SUM(sales_sum)) AS gross_margin
FROM `{PROJECT_ID}.{DATASET}.{VIEW_REALIZED}`
WHERE period = @period
GROUP BY period_date
ORDER BY period_date DESC
LIMIT @limit_n
"""
df2 = run_query(sql2, params=params)
if not df2.empty:
    df2_disp = df2.rename(columns={
        "period_date": "期間開始日",
        "realized_rows": "新規納品_行数",
        "sales_sum": "売上合計",
        "gp_sum": "粗利合計",
        "gross_margin": "粗利率",
    })
    df2_disp["粗利率"] = (df2_disp["粗利率"].astype(float) * 100).round(2)
    st.dataframe(df2_disp, use_container_width=True)

st.caption("※表示は担当者コードをキーにし、画面上は担当者名を表示。日本語カラム表記に統一。")
