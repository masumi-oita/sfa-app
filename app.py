import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネス・デザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
    .card { background-color: white; border: 1px solid #e0e0e0; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; }
    .card-label { font-size: 0.9rem; color: #666; font-weight: bold; }
    .card-value { font-size: 1.8rem; color: #003366; font-weight: 800; }
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
    
    # --- エラー回避：重複した列名を強制的に削除 ---
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 列名の微調整
    df = df.rename(columns={'年月': '売上月', '包装単位': '包装'})
    
    # 2026/01を正しく表示させるための日付処理
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値変換
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析ダッシュボード</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"データ取得エラー。BigQueryのビューを更新してください: {e}")
    st.stop()

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
    if kw: f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].astype(str).str.contains(kw, na=False)]

    # --- 5. サマリー ---
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="card"><div class="card-label">販売金額累計</div><div class="card-value">¥{f_df["販売金額"].sum():,.0f}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="card"><div class="card-label">販売数量合計</div><div class="card-value">{f_df["数量"].sum():,.0f}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="card"><div class="card-label">対象得意先数</div><div class="card-value">{f_df["得意先名"].nunique():,} 軒</div></div>', unsafe_allow_html=True)

    # --- 6. トレンド分析 ---
    st.markdown("### 📈 月別トレンド")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    fig = px.area(monthly, x='売上月', y='販売金額', color_discrete_sequence=['#003366'])
    fig.update_layout(xaxis_type='category', plot_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True)

    # --- 7. 詳細明細 ---
    st.markdown("### 📋 詳細明細（2026/01対応）")
    mode = st.radio("表示項目:", ["販売金額", "数量"], horizontal=True)
    
    # 2026/01が必ず右に来るようにソート
    month_cols = sorted(f_df['売上月'].unique().tolist())
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    pivot = pivot.reindex(columns=month_cols)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 CSVダウンロード", pivot.to_csv().encode('utf_8_sig'), "report.csv")

else:
    st.info("データがありません。")
