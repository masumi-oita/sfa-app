# =========================================================
# SFA Sales OS / Streamlit app.py
# 管理者・担当 共通入口（FYTD → 当月 → 新規納品）
# =========================================================

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import date
import difflib

# =========================================================
# 基本設定
# =========================================================
st.set_page_config(
    page_title="SFA Sales OS（入口）",
    layout="wide",
    initial_sidebar_state="expanded",
)

PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"

# =========================================================
# BigQuery Client（シングルトン）
# =========================================================
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

bq = get_bq_client()

# =========================================================
# 共通クエリ実行（DataFrameのみ返す）
# =========================================================
@st.cache_data(ttl=300)
def query_df(sql: str) -> pd.DataFrame:
    job = bq.query(sql)
    return job.result().to_dataframe()

# =========================================================
# ログイン情報（email）
# =========================================================
def get_login_email():
    qp = st.experimental_get_query_params()
    return qp.get("user_email", [None])[0]

login_email = get_login_email()

# =========================================================
# 権限取得
# =========================================================
@st.cache_data(ttl=600)
def get_user_role(login_email: str):
    if not login_email:
        return None
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.dim_staff_role`
    WHERE login_email = @email
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", login_email)
            ]
        )
    )
    df = job.result().to_dataframe()
    return df.iloc[0] if len(df) > 0 else None

role = get_user_role(login_email)

if role is None:
    st.error("権限が登録されていません。管理者に連絡してください。")
    st.stop()

is_admin = bool(role["role_admin_view"])

# =========================================================
# current_month
# =========================================================
@st.cache_data(ttl=600)
def get_current_month():
    sql = f"""
    SELECT current_month
    FROM `{PROJECT_ID}.{DATASET}.v_sys_current_month`
    """
    df = query_df(sql)
    return df.loc[0, "current_month"]

current_month = get_current_month()

st.caption(f"対象月：{current_month.strftime('%Y年%m月')}")

# =========================================================
# 得意先マスタ（検索用）
# =========================================================
@st.cache_data(ttl=600)
def load_customers():
    sql = f"""
    SELECT DISTINCT
        customer_code,
        customer_name
    FROM `{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily`
    """
    return query_df(sql)

df_customers = load_customers()

# =========================================================
# 得意先検索（名称入力 → 類似候補）
# =========================================================
def customer_search_ui():
    st.subheader("得意先検索")
    keyword = st.text_input("得意先名を入力（例：熊谷 / 循環器）")

    if not keyword:
        return None, None

    names = df_customers["customer_name"].tolist()
    matches = difflib.get_close_matches(keyword, names, n=10, cutoff=0.2)

    if not matches:
        st.info("候補が見つかりません")
        return None, None

    selected_name = st.selectbox("候補から選択", matches)
    row = df_customers[df_customers["customer_name"] == selected_name].iloc[0]

    return row["customer_code"], row["customer_name"]

# =========================================================
# FYTD サマリー（全社 or 担当）
# =========================================================
@st.cache_data(ttl=300)
def load_fytd_summary(scope: str, login_email: str | None):
    where = ""
    if scope == "ME":
        where = "WHERE login_email = @email"

    sql = f"""
    SELECT
        SUM(sales_amount) AS 売上,
        SUM(gross_profit) AS 粗利,
        SAFE_DIVIDE(SUM(gross_profit), SUM(sales_amount)) AS 粗利率
    FROM `{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily`
    {where}
    """
    if scope == "ME":
        job = bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", login_email)
                ]
            )
        )
        return job.result().to_dataframe()
    else:
        return query_df(sql)

# =========================================================
# 当月 下落・伸長
# =========================================================
@st.cache_data(ttl=300)
def load_monthly_rank(view_name: str, scope: str, login_email: str | None):
    where = ""
    if scope == "ME":
        where = "WHERE login_email = @email"

    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{view_name}`
    {where}
    ORDER BY abs(diff_sales) DESC
    LIMIT 20
    """
    if scope == "ME":
        job = bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", login_email)
                ]
            )
        )
        return job.result().to_dataframe()
    else:
        return query_df(sql)

# =========================================================
# 新規納品サマリー
# =========================================================
@st.cache_data(ttl=300)
def load_new_delivery(period: str):
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.v_new_deliveries_realized_{period}`
    """
    return query_df(sql)

# =========================================================
# UI 描画
# =========================================================
st.title("SFA Sales OS（入口）")

# -----------------------------
# FYTD
# -----------------------------
st.header("① 年度累計（FYTD）")

scope = "ALL" if is_admin else "ME"
df_fytd = load_fytd_summary(scope, login_email)

c1, c2, c3 = st.columns(3)
c1.metric("売上", f"{df_fytd.loc[0,'売上']:,.0f} 円")
c2.metric("粗利", f"{df_fytd.loc[0,'粗利']:,.0f} 円")
c3.metric("粗利率", f"{df_fytd.loc[0,'粗利率']*100:.1f} %")

# -----------------------------
# 当月
# -----------------------------
st.header("② 当月（前年同月比）")

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("下がっている得意先")
    df_down = load_monthly_rank(
        "v_sales_customer_yoy_bottom_current_month",
        scope,
        login_email,
    )
    st.dataframe(df_down, use_container_width=True)

with col_r:
    st.subheader("伸びている得意先")
    df_up = load_monthly_rank(
        "v_sales_customer_yoy_top_current_month",
        scope,
        login_email,
    )
    st.dataframe(df_up, use_container_width=True)

# -----------------------------
# 得意先検索 → ドリル
# -----------------------------
st.header("③ 得意先ドリル")

cust_code, cust_name = customer_search_ui()

if cust_code:
    st.success(f"選択中：{cust_name}")
    sql = f"""
    SELECT
        month,
        SUM(sales_amount) AS 売上,
        SUM(gross_profit) AS 粗利
    FROM `{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily`
    WHERE customer_code = @code
    GROUP BY month
    ORDER BY month
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("code", "STRING", cust_code)
            ]
        )
    )
    df_trend = job.result().to_dataframe()
    st.line_chart(df_trend.set_index("month")[["売上", "粗利"]])

# -----------------------------
# 新規納品
# -----------------------------
st.header("④ 新規納品サマリー")

tabs = st.tabs(["昨日", "週間", "月間", "年間"])
periods = ["daily", "weekly", "monthly", "yearly"]

for tab, period in zip(tabs, periods):
    with tab:
        df_new = load_new_delivery(period)
        st.dataframe(df_new, use_container_width=True)

# =========================================================
# フッター
# =========================================================
st.caption("SFA Sales OS / BigQuery Single Source of Truth")
