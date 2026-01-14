import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. キャッシュ強制消去関数 ---
def reset_system():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("キャッシュを消去しました。再読み込みします。")
    st.rerun()

# --- 3. プロ仕様デザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 25px; }
    .card { background: white; border: 1px solid #e2e8f0; padding: 1.5rem; border-radius: 10px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); text-align: center; }
    .metric-val { font-size: 1.7rem; color: #003366; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

# --- 4. データ取得 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    
    # 【最重要】重複列を物理的に排除してエラーを防ぐ
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 表示月の整形（2026/01対応）
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値のクリーンアップ
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    return df

# --- 5. メイン画面レイアウト ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析システム</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"Oh no. データの読み込み中にエラーが発生しました。: {e}")
    if st.button("🔄 システムをリセットして再試行"):
        reset_system()
    st.stop()

if not df.empty:
    with st.sidebar:
        st.image("https://www.towa-yakuhin.co.jp/common/images/logo_head.png", width=150)
        st.markdown("### 🔎 フィルタ")
        t_list = ['全 担当者'] + sorted(df['正規担当者名'].unique().tolist())
        sel_t = st.selectbox("担当者選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['正規担当者名'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先選択", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索")
        
        st.markdown("---")
        if st.button("🧹 キャッシュを強制リセット"):
            reset_system()

    # フィルタリング
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].astype(str).str.contains(kw, na=False)]

    # --- 6. サマリー ---
    col1, col2, col3 = st.columns(3)
    with col1: st.markdown(f'<div class="card">販売金額合計<br><span class="metric-val">¥{f_df["販売金額"].sum():,.0f}</span></div>', unsafe_allow_html=True)
    with col2: st.markdown(f'<div class="card">販売数量合計<br><span class="metric-val">{f_df["数量"].sum():,.0f}</span></div>', unsafe_allow_html=True)
    with col3: st.markdown(f'<div class="card">稼働得意先数<br><span class="metric-val">{f_df["得意先名"].nunique():,} 軒</span></div>', unsafe_allow_html=True)

    # --- 7. トレンド分析（2026/01対応） ---
    st.markdown("### 📈 月別トレンド分析")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    fig = px.area(monthly, x='売上月', y='販売金額', color_discrete_sequence=['#003366'])
    fig.update_layout(xaxis_type='category', plot_bgcolor='white')
    st.plotly_chart(fig, theme="streamlit")

    # --- 8. 詳細ピボットテーブル ---
    st.markdown("### 📋 販売実績詳細")
    mode = st.radio("表示項目:", ["販売金額", "数量"], horizontal=True)
    
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
        height=600
    )

else:
    st.info("データが読み込めませんでした。サイドバーからリセットを試してください。")
