import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. ページ設定 ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")

# --- 2. デザイン ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1rem; color: white; text-align: center; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# --- 3. 認証・データ読み込み ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=300)
def load_data():
    client = get_client()
    # SQLで指定した「売上月」で並べる
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    # 万が一の重複削除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # 日付整形
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    return df

st.markdown('<div class="main-header"><h1>九州東和薬品 販売実績分析</h1></div>', unsafe_allow_html=True)

# --- 4. 実行部（エラー回避の徹底） ---
try:
    df = load_data()
    
    with st.sidebar:
        st.markdown("### ⚙️ 設定")
        if st.button("🔄 データを強制更新（キャッシュ消去）"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        # フィルタ作成
        tantosha = st.selectbox("担当者", ['全 担当者'] + sorted(df['正規担当者名'].unique().tolist()))
        f_df = df if tantosha == '全 担当者' else df[df['正規担当者名'] == tantosha]
        
        search = st.text_input("商品名・ユニークコード検索")
        if search:
            f_df = f_df[f_df['商品名'].str.contains(search, na=False) | f_df['ユニークコード'].astype(str).str.contains(search, na=False)]

    # --- 5. 画面表示 ---
    c1, c2, c3 = st.columns(3)
    c1.metric("売上金額", f"¥{f_df['販売金額'].sum():,.0f}")
    c2.metric("数量", f"{f_df['数量'].sum():,.0f}")
    c3.metric("得意先数", f"{f_df['得意先名'].nunique():,} 軒")

    st.markdown("### 📈 月別トレンド")
    trend = f_df.groupby('売上月')['販売金額'].sum().reset_index()
    st.plotly_chart(px.line(trend, x='売上月', y='販売金額', markers=True), use_container_width=True)

    st.markdown("### 📋 販売詳細（2026/01対応）")
    pivot = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values='販売金額', aggfunc='sum', fill_value=0)
    pivot['合計'] = pivot.sum(axis=1)
    st.dataframe(pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"), use_container_width=True, height=500)

except Exception as e:
    st.error(f"エラーが発生しました。現在、修復を試みています。")
    st.info(f"技術詳細: {e}")
    if st.button("もう一度キャッシュを消去して再開"):
        st.cache_data.clear()
        st.rerun()
