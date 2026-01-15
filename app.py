import streamlit as st
import pandas as pd
import plotly.express as px

# ページ設定
st.set_page_config(page_title="九州東和薬品 - 利益管理ダッシュボード", layout="wide")

# 1. BigQuery接続設定
# (注: .streamlit/secrets.toml に認証情報がある前提です)
conn = st.connection("bigquery", type="gcp.bigquery")

@st.cache_data(ttl=600)
def load_data():
    # 先ほど作成した最強のビューから全データを取得
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    return conn.query(query)

# データの読み込み
try:
    df = load_data()
except Exception as e:
    st.error(f"データの読み込みに失敗しました。BigQueryのビューを確認してください: {e}")
    st.stop()

# --- サイドバー：フィルター機能 ---
st.sidebar.header("🔍 フィルタ")

# 月選択
month_list = sorted(df['売上月'].unique(), reverse=True)
selected_months = st.sidebar.multiselect("売上月を選択", month_list, default=month_list)

# 担当者選択
rep_list = sorted(df['正規担当者名'].unique())
selected_reps = st.sidebar.multiselect("担当者を選択", rep_list, default=rep_list)

# フィルタリング実行
mask = df['売上月'].isin(selected_months) & df['正規担当者名'].isin(selected_reps)
filtered_df = df[mask]

# --- メイン画面：ダッシュボード ---
st.title("📈 利益管理ダッシュボード (SFA v2.0)")
st.write(f"現在の表示データ: {len(filtered_df)} 件")

# 2. KPI メトリクス
total_sales = filtered_df['販売金額'].sum()
total_profit = filtered_df['粗利額'].sum()
profit_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("総売上金額", f"¥{total_sales:,.0f}")
with col2:
    st.metric("総粗利額", f"¥{total_profit:,.0f}", delta=f"{profit_margin:.1f}% (粗利率)")
with col3:
    st.metric("粗利率平均", f"{profit_margin:.1f}%")

st.divider()

# 3. グラフ分析
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🗓 月別 売上・粗利推移")
    # 月別集計
    monthly_summary = filtered_df.groupby('売上月')[['販売金額', '粗利額']].sum().reset_index()
    fig_monthly = px.bar(
        monthly_summary, x='売上月', y=['販売金額', '粗利額'],
        barmode='group', labels={'value': '金額', 'variable': '項目'},
        color_discrete_sequence=['#3366CC', '#109618']
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

with col_right:
    st.subheader("👤 担当者別 粗利貢献度")
    # 担当者別集計
    rep_summary = filtered_df.groupby('正規担当者名')['粗利額'].sum().sort_values(ascending=True).reset_index()
    fig_rep = px.bar(
        rep_summary, y='正規担当者名', x='粗利額',
        orientation='h', color='粗利額', color_continuous_scale='Greens'
    )
    st.plotly_chart(fig_rep, use_container_width=True)

# 4. 詳細データ一覧
st.subheader("📋 詳細明細（フィルタ連動）")
st.dataframe(
    filtered_df[[
        '売上月', '正規担当者名', '得意先名', '商品名', 
        '数量', '適用単価', '適用原価', '販売金額', '粗利額'
    ]].sort_values('粗利額', ascending=False),
    use_container_width=True,
    hide_index=True
)

# 5. CSVダウンロード機能
csv = filtered_df.to_csv(index=False).encode('utf_8_sig')
st.download_button(
    label="📩 フィルタ結果をCSVでダウンロード",
    data=csv,
    file_name=f"sales_report_{selected_months[0] if selected_months else 'all'}.csv",
    mime="text/csv",
)
