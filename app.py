import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. ページ設定
st.set_page_config(page_title="利益管理ダッシュボード | 九州東和薬品", layout="wide")

# 2. 認証情報とBigQueryクライアントの設定
@st.cache_resource # 接続自体をキャッシュして効率化
def get_bq_client():
    try:
        # 岡崎様の secrets.toml からJSONを取得
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(credentials=credentials, project=info['project_id'])
        return client
    except Exception as e:
        st.error(f"認証情報の読み込みに失敗しました: {e}")
        st.stop()

client = get_bq_client()

# 3. データの読み込み
@st.cache_data(ttl=600)
def load_data():
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    # クライアントを直接使ってクエリ実行
    query_job = client.query(query)
    return query_job.to_dataframe()

# データのロード
df = load_data()

# --- 以降、サイドバーやグラフのコードは前回と同じです ---
st.sidebar.header("📊 分析フィルタ")

# 期間選択
month_list = sorted(df['売上月'].unique(), reverse=True)
selected_months = st.sidebar.multiselect("対象月", month_list, default=month_list[:3])

# 担当者選択
rep_list = sorted(df['正規担当者名'].unique())
selected_reps = st.sidebar.multiselect("担当者", rep_list, default=rep_list)

# フィルタ適用
mask = df['売上月'].isin(selected_months) & df['正規担当者名'].isin(selected_reps)
filtered_df = df[mask].copy()

# --- メイン表示 ---
st.title("🏥 九州東和薬品 利益管理 SFA")

# KPI
st.subheader("📌 主要KPI指標")
total_sales = filtered_df['販売金額'].sum()
total_profit = filtered_df['粗利額'].sum()
avg_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric("総売上", f"¥{total_sales:,.0f}")
with kpi2:
    st.metric("総粗利額", f"¥{total_profit:,.0f}")
with kpi3:
    st.metric("全体粗利率", f"{avg_margin:.1f} %")

st.divider()

# 可視化
col_left, col_right = st.columns(2)
with col_left:
    st.subheader("📈 月別 推移")
    monthly_data = filtered_df.groupby('売上月')[['販売金額', '粗利額']].sum().reset_index()
    fig_monthly = px.bar(monthly_data, x='売上月', y=['販売金額', '粗利額'], barmode='group')
    st.plotly_chart(fig_monthly, use_container_width=True)

with col_right:
    st.subheader("🏆 担当者別 粗利貢献度")
    rep_data = filtered_df.groupby('正規担当者名')['粗利額'].sum().sort_values(ascending=True).reset_index()
    fig_rep = px.bar(rep_data, y='正規担当者名', x='粗利額', orientation='h', color='粗利額')
    st.plotly_chart(fig_rep, use_container_width=True)

# 詳細テーブル
st.subheader("🔍 詳細明細")
st.dataframe(filtered_df, use_container_width=True, hide_index=True)
