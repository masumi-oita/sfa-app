import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

# 1. BigQueryからのデータ読み込み
@st.cache_data(ttl=600)
def load_sales_performance():
    client = bigquery.Client()
    # 岡崎様が作成した「整地済みビュー」をそのまま叩く
    query = """
    SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    ORDER BY 売上日 DESC
    """
    return client.query(query).to_dataframe()

st.title("📊 営業実績・利益分析ダッシュボード")
df = load_sales_performance()

# --- サイドバー：踏襲したカラム（支店・担当）でのフィルタリング ---
st.sidebar.header("表示フィルタ")

# 支店・担当者での絞り込み（過去実績側はNULLなので「未割当」として処理）
branches = st.sidebar.multiselect("支店名", options=df["支店名"].fillna("本部/過去分").unique())
staffs = st.sidebar.multiselect("担当社員名", options=df["担当社員名"].fillna("未割当/過去分").unique())

# データのフィルタリング
filtered_df = df.copy()
if branches:
    filtered_df = filtered_df[filtered_df["支店名"].fillna("本部/過去分").isin(branches)]
if staffs:
    filtered_df = filtered_df[filtered_df["担当社員名"].fillna("未割当/過去分").isin(staffs)]

# --- メイン指標：逆算した利益を合算 ---
# 実績と採用分を統合した「分析用」カラムを使用
total_sales = (filtered_df["数量"] * filtered_df["分析用単価"]).sum()
total_profit = (filtered_df["数量"] * (filtered_df["分析用単価"] - filtered_df["分析用原価"])).sum()

col1, col2, col3 = st.columns(3)
col1.metric("総販売金額", f"¥{total_sales:,.0f}")
col2.metric("総粗利額", f"¥{total_profit:,.0f}")
col3.metric("粗利率", f"{(total_profit / total_sales * 100):.1f}%" if total_sales != 0 else "0%")

# --- グラフ：2年分の推移（データ区分別） ---
st.subheader("📈 売上・粗利の時系列推移（24ヶ月）")
monthly_summary = filtered_df.groupby(["売上月", "データ区分"]).agg({
    "数量": "sum",
    "分析用単価": "mean"
}).reset_index()

# 数量 × 単価で月別売上を算出
monthly_summary["月別売上"] = monthly_summary["数量"] * monthly_summary["分析用単価"]

fig = px.bar(
    monthly_summary, 
    x="売上月", 
    y="月別売上", 
    color="データ区分",
    barmode="group",
    title="過去実績 vs 採用実績 の月別推移",
    labels={"月別売上": "販売金額(¥)", "売上月": "年月"}
)
st.plotly_chart(fig, use_container_width=True)

# --- 詳細データテーブル ---
st.subheader("📑 詳細データ一覧")
st.dataframe(filtered_df[[
    "売上日", "得意先名", "商品名", "数量", 
    "分析用単価", "分析用原価", "データ区分", "戦略品フラグ"
]], use_container_width=True)
