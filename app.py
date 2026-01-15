import streamlit as st
import pandas as pd
import plotly.express as px
import json

# 1. ページ基本設定
st.set_page_config(page_title="利益管理ダッシュボード | 九州東和薬品", layout="wide")

# 2. 認証情報と接続設定（岡崎様の secrets.toml 設定に準拠）
try:
    # secrets から JSON 文字列を取得して辞書に変換
    service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    
    # BigQueryへの接続
    conn = st.connection(
        "bigquery", 
        type="gcp.bigquery", 
        service_account_info=service_account_info
    )
except Exception as e:
    st.error(f"認証情報の読み込みに失敗しました。secrets.toml を確認してください: {e}")
    st.stop()

# 3. データの読み込み（キャッシュを利用して高速化）
@st.cache_data(ttl=600)
def load_data():
    # 先ほど作成した「最強のビュー」を全件取得
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    return conn.query(query)

# データのロード実行
df = load_data()

# --- サイドバー：管理者用フィルタ ---
st.sidebar.header("📊 分析フィルタ")

# 期間選択（売上月）
month_list = sorted(df['売上月'].unique(), reverse=True)
selected_months = st.sidebar.multiselect("対象月", month_list, default=month_list[:3])

# 担当者選択
rep_list = sorted(df['正規担当者名'].unique())
selected_reps = st.sidebar.multiselect("担当者", rep_list, default=rep_list)

# フィルタ適用
mask = df['売上月'].isin(selected_months) & df['正規担当者名'].isin(selected_reps)
filtered_df = df[mask].copy()

# --- メイン画面 ---
st.title("🏥 九州東和薬品 利益管理 SFA")
st.caption("BigQuery + Streamlit DXプロジェクト")

# 4. KPIメトリクス表示
st.subheader("📌 主要KPI指標")
total_sales = filtered_df['販売金額'].sum()
total_profit = filtered_df['粗利額'].sum()
avg_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric("総売上（適用単価ベース）", f"¥{total_sales:,.0f}")
with kpi2:
    st.metric("総粗利額（事務原価連動）", f"¥{total_profit:,.0f}")
with kpi3:
    st.metric("全体粗利率", f"{avg_margin:.1f} %")

st.divider()

# 5. 可視化チャート
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 月別 売上・粗利推移")
    monthly_data = filtered_df.groupby('売上月')[['販売金額', '粗利額']].sum().reset_index()
    fig_monthly = px.bar(
        monthly_data, x='売上月', y=['販売金額', '粗利額'],
        barmode='group',
        labels={'value': '金額（円）', 'variable': '項目'},
        color_discrete_sequence=['#1f77b4', '#2ca02c'] # 青と緑
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

with col_right:
    st.subheader("🏆 担当者別 粗利貢献度")
    rep_data = filtered_df.groupby('正規担当者名')['粗利額'].sum().sort_values(ascending=True).reset_index()
    fig_rep = px.bar(
        rep_data, y='正規担当者名', x='粗利額',
        orientation='h',
        color='粗利額',
        color_continuous_scale='YlGn' # 利益が高いほど濃い緑
    )
    st.plotly_chart(fig_rep, use_container_width=True)

# 6. 詳細データテーブル
st.subheader("🔍 詳細明細（施設・商品別）")
st.write("粗利額の高い順に表示しています。")
st.dataframe(
    filtered_df[[
        '売上月', '正規担当者名', '得意先名', '商品名', 
        '数量', '適用単価', '適用原価', '粗利額', 'ユニークコード_JAN'
    ]].sort_values('粗利額', ascending=False),
    use_container_width=True,
    hide_index=True
)

# 7. エクスポート機能
st.sidebar.divider()
csv = filtered_df.to_csv(index=False).encode('utf_8_sig')
st.sidebar.download_button(
    label="📥 表示中のデータをダウンロード",
    data=csv,
    file_name=f"towa_profit_report.csv",
    mime="text/csv",
)
