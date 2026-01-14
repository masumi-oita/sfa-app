import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. キャッシュクリア機能 ---
def clear_all_cache():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("キャッシュを完全に消去しました。再読み込みします...")

# --- 3. データ取得 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    # 2026/01を表示するため「売上月」でソート
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    # 重複列の強制排除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # 日付整形
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    return df

# --- 4. メイン画面 ---
st.markdown('<h1 style="text-align:center; color:#003366;">九州東和薬品 販売実績分析システム</h1>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"Oh No! データ取得エラー。SQLを更新して下のボタンを押してください。: {e}")
    if st.button("🔄 キャッシュを強制リセット"):
        clear_all_cache()
    st.stop()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 分析フィルタ")
        t_list = ['全 担当者'] + sorted(df['正規担当者名'].unique().tolist())
        sel_t = st.selectbox("担当者選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['正規担当者名'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先選択", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索")
        
        st.markdown("---")
        if st.button("🧹 キャッシュを初期化する"):
            clear_all_cache()
            st.rerun()

    # フィルタリング
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].astype(str).str.contains(kw, na=False)]

    # --- 5. サマリー ---
    col1, col2, col3 = st.columns(3)
    col1.metric("販売金額 累計", f"¥{f_df['販売金額'].sum():,.0f}")
    col2.metric("販売数量 合計", f"{f_df['数量'].sum():,.0f}")
    col3.metric("対象得意先数", f"{f_df['得意先名'].nunique():,} 軒")

    # --- 6. 2026/01対応トレンド ---
    st.markdown("### 📈 月別トレンド")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    st.plotly_chart(px.line(monthly, x='売上月', y='販売金額', markers=True), use_container_width=True)

    # --- 7. 詳細ピボット ---
    st.markdown("### 📋 販売詳細（2026/01対応）")
    pivot = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values='販売金額', aggfunc='sum', fill_value=0)
    pivot['期間合計'] = pivot.sum(axis=1)
    st.dataframe(pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"), use_container_width=True, height=500)

else:
    st.info("データがありません。")
