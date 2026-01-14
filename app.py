import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定（ここが重要！） ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")

# 72万セルの壁を突破するために、制限を200万セルまで引き上げます
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. デザイン ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem;}
</style>
""", unsafe_allow_html=True)

# --- 3. データ取得 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=300)
def load_data():
    client = get_client()
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    
    # 重複列の強制排除（1-dimensionalエラー対策）
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 表示月の整形
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品 販売実績分析ダッシュボード</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
    
    with st.sidebar:
        st.markdown("### ⚙️ 管理・フィルタ")
        # Rebootが面倒な時のためのキャッシュ消去ボタン
        if st.button("🔄 データを再読み込み"):
            st.cache_data.clear()
            st.rerun()
            
        st.markdown("---")
        tantosha = st.selectbox("担当者", ['全 担当者'] + sorted(df['正規担当者名'].unique().tolist()))
        f_df = df if tantosha == '全 担当者' else df[df['正規担当者名'] == tantosha]
        
        search = st.text_input("商品名・ユニークコード検索")
        if search:
            f_df = f_df[f_df['商品名'].str.contains(search, na=False) | f_df['ユニークコード'].astype(str).str.contains(search, na=False)]

    # --- 5. 実績表示 ---
    c1, c2, c3 = st.columns(3)
    c1.metric("販売金額 累計", f"¥{f_df['販売金額'].sum():,.0f}")
    c2.metric("販売数量 合計", f"{f_df['数量'].sum():,.0f}")
    c3.metric("対象得意先数", f"{f_df['得意先名'].nunique():,} 軒")

    st.markdown("### 📈 月別トレンド (2026/01対応)")
    trend = f_df.groupby('売上月')['販売金額'].sum().reset_index()
    st.plotly_chart(px.line(trend, x='売上月', y='販売金額', markers=True), use_container_width=True)

    st.markdown("### 📋 販売詳細明細")
    # ここで制限解除したStylerが活躍します
    pivot = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values='販売金額', aggfunc='sum', fill_value=0)
    pivot['合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"), 
        use_container_width=True, height=600
    )

except Exception as e:
    st.error(f"Oh no. 予期せぬエラーが発生しました。")
    st.info(f"技術詳細: {e}")
    if st.button("キャッシュを消去して再起動"):
        st.cache_data.clear()
        st.rerun()
