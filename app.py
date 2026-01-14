import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. 基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネスデザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
    .stMetric { background-color: white; border: 1px solid #ddd; padding: 15px; border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

# --- 3. データ取得 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    # 2026年1月を表示させるため、年月でソート
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `年月` ASC"
    df = client.query(query).to_dataframe()
    
    # 重複列の削除（念のため）
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2026/01を正しく並べるための処理
    df['売上月'] = df['年月'].astype(str).str.replace('-', '/')
    
    # 数値変換
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析システム</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"Oh No! エラーが発生しました。SQLを実行してビューを更新してください。: {e}")
    st.stop()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 分析フィルタ")
        # ★ここが重要：SQLで作った「正規担当者名」だけをリストに使う
        tantosha_list = ['全 担当者'] + sorted(df['正規担当者名'].unique().tolist())
        sel_t = st.selectbox("担当者名", tantosha_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['正規担当者名'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先名", c_list)
        
        kw = st.text_input("商品名・ユニークコードで検索")

    # フィルタリング
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].astype(str).str.contains(kw, na=False)]

    # --- 5. サマリー ---
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("販売金額 累計", f"¥{f_df['販売金額'].sum():,.0f}")
    with c2: st.metric("販売数量 合計", f"{f_df['数量'].sum():,.0f}")
    with c3: st.metric("対象得意先数", f"{f_df['得意先名'].nunique():,} 軒")

    # --- 6. トレンド（2026/01対応） ---
    st.markdown("### 📈 月別トレンド分析")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    st.plotly_chart(px.area(monthly, x='売上月', y='販売金額', color_discrete_sequence=['#003366']), use_container_width=True)

    # --- 7. 詳細ピボット ---
    st.markdown("### 📋 詳細明細一覧")
    mode = st.radio("表示モード:", ["販売金額", "数量"], horizontal=True)
    
    # 2026/01を最後にするための並び順固定
    month_order = sorted(f_df['売上月'].unique().tolist())
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装単位'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    pivot = pivot.reindex(columns=month_order)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )

else:
    st.info("データがありません。")
