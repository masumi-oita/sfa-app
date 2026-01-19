# ============================================================
# SFA Sales Intelligence App
# ============================================================

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import date

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
PROJECT_ID = "salesdb-479915"
TABLE_SALES = "sales_data.v_sales_merged_2y_plus_month"

LOOKBACK_DAYS_NEW = 365   # 新規納品判定（YJ×得意先）

st.set_page_config(
    page_title="SFA Sales Intelligence",
    layout="wide"
)

# ------------------------------------------------------------
# BigQuery Client
# ------------------------------------------------------------
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

# ------------------------------------------------------------
# Load Sales Data
# ------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_sales_data():
    client = get_bq_client()

    query = f"""
    SELECT
      customer_code,
      customer_name,
      sales_date,
      yj_code,
      unique_code_yj,
      ingredient,
      product_name,
      efficacy_category,
      quantity,
      sales_amount,
      gross_profit
    FROM `{PROJECT_ID}.{TABLE_SALES}`
    """

    df = client.query(query).to_dataframe()
    return df

# ------------------------------------------------------------
# New Delivery Flag (YJ × 得意先)
# ------------------------------------------------------------
def add_new_delivery_flag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sales_date"] = pd.to_datetime(df["sales_date"])

    last_sale = (
        df.groupby(["customer_code", "yj_code"])["sales_date"]
        .max()
        .reset_index()
        .rename(columns={"sales_date": "last_sales_date"})
    )

    df = df.merge(last_sale, on=["customer_code", "yj_code"], how="left")

    today = pd.Timestamp(date.today())
    df["is_new_delivery"] = (
        (today - df["last_sales_date"]).dt.days > LOOKBACK_DAYS_NEW
    )

    return df

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("📊 SFA 営業支援ダッシュボード")

with st.spinner("売上データ読込中..."):
    df_sales = load_sales_data()

if df_sales.empty:
    st.warning("データがありません")
    st.stop()

df_sales = add_new_delivery_flag(df_sales)

# ------------------------------------------------------------
# Sidebar Filters
# ------------------------------------------------------------
st.sidebar.header("🔍 フィルタ")

customers = sorted(df_sales["customer_name"].unique())
selected_customer = st.sidebar.selectbox(
    "得意先を選択",
    customers
)

df_cust = df_sales[df_sales["customer_name"] == selected_customer]

# ------------------------------------------------------------
# KPI Summary
# ------------------------------------------------------------
st.subheader(f"🏥 {selected_customer} サマリー")

col1, col2, col3 = st.columns(3)

col1.metric(
    "売上合計",
    f"¥{df_cust['sales_amount'].sum():,.0f}"
)

col2.metric(
    "粗利合計",
    f"¥{df_cust['gross_profit'].sum():,.0f}"
)

margin = (
    df_cust["gross_profit"].sum() / df_cust["sales_amount"].sum()
    if df_cust["sales_amount"].sum() > 0 else 0
)

col3.metric(
    "粗利率",
    f"{margin:.1%}"
)

# ------------------------------------------------------------
# Efficacy Trend (得意先の薬効傾向)
# ------------------------------------------------------------
st.subheader("💊 薬効分類別 売上構成")

eff_summary = (
    df_cust.groupby("efficacy_category", dropna=False)
    .agg(
        売上金額=("sales_amount", "sum"),
        粗利=("gross_profit", "sum")
    )
    .sort_values("売上金額", ascending=False)
    .reset_index()
)

st.dataframe(eff_summary, use_container_width=True)

# ------------------------------------------------------------
# Recommendation Logic
# ------------------------------------------------------------
st.subheader("🚀 おすすめ未採用品目")

# 得意先が採用している薬効
adopted_eff = set(df_cust["efficacy_category"].dropna().unique())

# 全社売上（基準）
df_all = df_sales.copy()

top_products_by_eff = (
    df_all.groupby(
        ["efficacy_category", "yj_code", "ingredient", "product_name"]
    )
    .agg(
        全社売上=("sales_amount", "sum"),
        全社粗利=("gross_profit", "sum")
    )
    .reset_index()
)

# 得意先未採用 × 同薬効
cust_yj = set(df_cust["yj_code"].unique())

recommend = top_products_by_eff[
    (top_products_by_eff["efficacy_category"].isin(adopted_eff)) &
    (~top_products_by_eff["yj_code"].isin(cust_yj))
].sort_values("全社売上", ascending=False)

# 表示用
recommend_display = recommend.head(20).rename(columns={
    "efficacy_category": "薬効分類",
    "yj_code": "YJコード",
    "ingredient": "成分",
    "product_name": "商品名",
    "全社売上": "全社売上金額",
    "全社粗利": "全社粗利"
})

st.dataframe(recommend_display, use_container_width=True)

# ------------------------------------------------------------
# New Delivery Check
# ------------------------------------------------------------
st.subheader("🆕 新規納品候補（1年以上実績なし）")

new_delivery = df_cust[df_cust["is_new_delivery"]]

if new_delivery.empty:
    st.info("新規納品候補はありません")
else:
    new_display = new_delivery[[
        "product_name",
        "ingredient",
        "yj_code",
        "sales_date"
    ]].rename(columns={
        "product_name": "商品名",
        "ingredient": "成分",
        "yj_code": "YJコード",
        "sales_date": "最終販売日"
    })

    st.dataframe(new_display, use_container_width=True)
