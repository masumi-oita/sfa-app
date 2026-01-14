import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Analysis", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. プロフェッショナル・デザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 10px; margin-bottom: 2rem; }
    .metric-card { background-color: #ffffff; border: 1px solid #e2e8f0; padding: 1.5rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; }
    .metric-label { font-size: 0.95rem; color: #64748b; font-weight: bold; margin-bottom: 8px; }
    .metric-value { font-size: 1.8rem; color: #003366; font-weight: 800; }
</style>
""", unsafe_allow_html=True)

# --- 3. データ処理 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `年月` ASC"
    df = client.query(query).to_dataframe()
    
    # 品名をメインの商品名として定義（重複回避）
    if '品名' in df.columns:
        if '商品名' in df.columns: df = df.drop(columns=['商品名'])
        df = df.rename(columns={'品名': '商品名'})
    
    df = df.rename(columns={'正規担当者名': '担当者', '年月': '売上月', '包装単位': '包装'})
    
    # 2026/01までの並び順を固定
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値のクリーンアップ
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    # 重複列の最終排除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析システム</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 分析フィルタ")
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者名", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先名", c_list)
        
        kw = st.text_input("商品名・ユニークコードで検索", "")

    # フィルタリング
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリー (洗練されたメトリクス) ---
    s_val = f_df['販売金額'].sum()
    q_val = f_df['数量'].sum()
    cust_val = f_df['得意先名'].nunique()
    
    col1, col2, col3 = st.columns(3)
    with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">販売金額累計</div><div class="metric-value">¥{s_val:,.0f}</div></div>', unsafe_allow_html=True)
    with col2: st.markdown(f'<div class="metric-card"><div class="metric-label">販売数量合計</div><div class="metric-value">{q_val:,.0f}</div></div>', unsafe_allow_html=True)
    with col3: st.markdown(f'<div class="metric-card"><div class="metric-label">稼働得意先数</div><div class="metric-value">{cust_val:,} 軒</div></div>', unsafe_allow_html=True)

    # --- 6. トレンド分析 (2026/01を確実に表示) ---
    st.markdown("### 📈 月次販売トレンド")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    fig = px.area(monthly, x='売上月', y='販売金額', color_discrete_sequence=['#003366'])
    fig.update_layout(xaxis_type='category', plot_bgcolor='white', margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

    # --- 7. 詳細ピボットテーブル ---
    st.markdown("### 📋 得意先別・製品別詳細実績")
    mode = st.radio("表示切り替え:", ["販売金額", "数量"], horizontal=True)
    
    # 時系列を昇順で固定（2026/01が最後に来るように）
    all_months = sorted(f_df['売上月'].unique().tolist())
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    
    # 列を最新月まで確実に並べる
    pivot = pivot.reindex(columns=all_months)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 データをCSVで保存", pivot.to_csv().encode('utf_8_sig'), "KyushuTowa_Report.csv")

else:
    st.info("データがありません。条件をリセットしてください。")
