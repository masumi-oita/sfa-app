import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネス・デザインCSS（高級感のあるビジネス紺を採用） ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
    .metric-card { background-color: #ffffff; border: 1px solid #e2e8f0; padding: 1.2rem; border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); text-align: center; }
    .metric-title { font-size: 0.9rem; color: #64748b; font-weight: bold; margin-bottom: 5px; }
    .metric-value { font-size: 1.6rem; color: #003366; font-weight: 800; }
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
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    df = client.query(query).to_dataframe()
    
    # 重複列の解消とマッピング（品名を商品名として扱う）
    if '品名' in df.columns:
        if '商品名' in df.columns: df = df.drop(columns=['商品名'])
        df = df.rename(columns={'品名': '商品名'})
    
    df = df.rename(columns={'正規担当者名': '担当者', '年月': '売上月', '包装単位': '包装'})
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 数値変換
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析ダッシュボード</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 検索・フィルタ")
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者を選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先を選択", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索", "")

    # フィルタ適用
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリーメトリクス ---
    sales = f_df['販売金額'].sum()
    qty = f_df['数量'].sum()
    cust_count = f_df['得意先名'].nunique()
    
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="metric-card"><div class="metric-title">販売金額合計</div><div class="metric-value">¥{sales:,.0f}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><div class="metric-title">販売数量合計</div><div class="metric-value">{qty:,.0f}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><div class="metric-title">稼働得意先数</div><div class="metric-value">{cust_count:,} 軒</div></div>', unsafe_allow_html=True)

    # --- 6. チャート分析 ---
    st.markdown("### 📊 トレンド分析")
    g1, g2 = st.columns([2, 1])
    with g1:
        monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index()
        fig_line = px.line(monthly, x='売上月', y='販売金額', title="月別販売トレンド", markers=True, color_discrete_sequence=['#003366'])
        st.plotly_chart(fig_line, use_container_width=True)
    with g2:
        top10 = f_df.groupby('商品名')['販売金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_bar = px.bar(top10, x='販売金額', y='商品名', orientation='h', title="製品別売上TOP10", color_discrete_sequence=['#10b981'])
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- 7. 詳細明細テーブル ---
    st.markdown("### 📋 販売詳細明細（得意先別）")
    mode = st.radio("表示モード:", ["販売金額", "数量"], horizontal=True)
    
    pivot = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装'], columns='売上月', values=mode, aggfunc='sum', fill_value=0)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 分析結果(CSV)を保存", pivot.to_csv().encode('utf_8_sig'), "sales_report.csv")

else:
    st.info("データがありません。")
