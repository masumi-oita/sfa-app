import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. ページ設定
st.set_page_config(page_title="九州東和薬品 | 利益管理SFA", layout="wide")

# 2. 認証と接続設定（Googleドライブへのアクセス許可を追加）
@st.cache_resource
def get_bq_client():
    try:
        # 岡崎様の secrets.toml からJSONを取得
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        
        # ★最重要：BigQueryだけでなく、Googleドライブへのアクセススコープを明示します
        # これがないと、オーナー権限でもスプレッドシート参照でForbiddenが出ます
        scopes = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
        
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=scopes
        )
        client = bigquery.Client(credentials=credentials, project=info['project_id'])
        return client
    except Exception as e:
        st.error(f"認証エラー: {e}")
        st.stop()

client = get_bq_client()

# 3. データの読み込み
@st.cache_data(ttl=600)
def load_data():
    # 作成した最強のビューを読み込み
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    # Storage APIのエラーを避けるため、安全な読み込みモードを指定
    return client.query(query).to_dataframe(create_bqstorage_client=False)

try:
    df = load_data()
except Exception as e:
    st.error(f"データ取得エラー。Google Drive APIが有効か確認してください: {e}")
    st.stop()

# --- サイドバー：フィルタ ---
st.sidebar.header("🔍 分析フィルタ")

# 月選択
month_list = sorted(df['売上月'].unique(), reverse=True)
selected_months = st.sidebar.multiselect("対象月", month_list, default=month_list[:3])

# 担当者選択
rep_list = sorted(df['正規担当者名'].unique())
selected_reps = st.sidebar.multiselect("担当者", rep_list, default=rep_list)

# フィルタ適用
mask = df['売上月'].isin(selected_months) & df['正規担当者名'].isin(selected_reps)
filtered_df = df[mask].copy()

# --- メイン画面 ---
st.title("🏥 利益管理ダッシュボード")
st.caption("Kyushu Towa Pharmaceutical - DX Project")

# 4. KPIメトリクス
total_sales = filtered_df['販売金額'].sum()
total_profit = filtered_df['粗利額'].sum()
avg_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("総売上", f"¥{total_sales:,.0f}")
with col2:
    st.metric("総粗利額", f"¥{total_profit:,.0f}")
with col3:
    st.metric("平均粗利率", f"{avg_margin:.1f} %")

st.divider()

# 5. 可視化
c_left, c_right = st.columns(2)

with c_left:
    st.subheader("🗓 月別推移")
    m_summary = filtered_df.groupby('売上月')[['販売金額', '粗利額']].sum().reset_index()
    fig = px.bar(m_summary, x='売上月', y=['販売金額', '粗利額'], barmode='group',
                 color_discrete_sequence=['#3366CC', '#109618'])
    st.plotly_chart(fig, use_container_width=True)

with c_right:
    st.subheader("👤 担当者別 粗利貢献")
    r_summary = filtered_df.groupby('正規担当者名')['粗利額'].sum().sort_values().reset_index()
    fig_r = px.bar(r_summary, y='正規担当者名', x='粗利額', orientation='h', color='粗利額')
    st.plotly_chart(fig_r, use_container_width=True)

# 6. 明細
st.subheader("📋 詳細明細")
st.dataframe(filtered_df.sort_values('粗利額', ascending=False), use_container_width=True, hide_index=True)
