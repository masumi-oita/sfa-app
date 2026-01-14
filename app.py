import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定（表示制限を最大化） ---
st.set_page_config(page_title="Kyushu Towa SFA 2026", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. 経営ダッシュボードCSS（清潔感と重厚感） ---
st.markdown("""
<style>
    .main-header { background-color: #002D62; padding: 20px; color: white; text-align: center; border-radius: 10px; margin-bottom: 25px; }
    .card { background-color: white; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; }
    .card-label { font-size: 0.9rem; color: #64748b; font-weight: bold; margin-bottom: 5px; }
    .card-value { font-size: 1.8rem; color: #002D62; font-weight: 800; }
    .stDataFrame { border: 1px solid #e2e8f0; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# --- 3. データ取得ロジック ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `年月` ASC"
    df = client.query(query).to_dataframe()
    
    # 列名の整理（ユニークコード等はそのまま保持される）
    if '品名' in df.columns:
        if '商品名' in df.columns: df = df.drop(columns=['商品名'])
        df = df.rename(columns={'品名': '商品名'})
    
    df = df.rename(columns={'正規担当者名': '担当者', '年月': '売上月', '包装単位': '包装'})
    
    # 2026/01を正しく並べるための処理
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値の確定
    for c in ['販売金額', '売上利益', '数量']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    
    # 重複列の最終排除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

# --- 4. メイン画面 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売・利益分析ダッシュボード 2026</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    with st.sidebar:
        st.image("https://www.towa-yakuhin.co.jp/common/images/logo_head.png", width=150)
        st.markdown("### 🔍 高度な検索")
        # 担当者（古賀優一郎に統一）
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先", c_list)
        
        kw = st.text_input("商品名・ユニークコードで検索", "")

    # フィルタ
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリー（洗練された表示） ---
    sales = f_df['販売金額'].sum()
    qty = f_df['数量'].sum()
    profit = f_df['売上利益'].sum() # 原価がない場合は0になるが項目は維持
    
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.markdown(f'<div class="card"><div class="card-label">販売金額累計</div><div class="card-value">¥{sales:,.0f}</div></div>', unsafe_allow_html=True)
    with col2: st.markdown(f'<div class="card"><div class="card-label">販売数量合計</div><div class="card-value">{qty:,.0f}</div></div>', unsafe_allow_html=True)
    with col3: st.markdown(f'<div class="card"><div class="card-label">対象得意先数</div><div class="card-value">{f_df["得意先名"].nunique():,} 軒</div></div>', unsafe_allow_html=True)
    with col4: st.markdown(f'<div class="card"><div class="card-label">取引データ数</div><div class="card-value">{len(f_df):,} 件</div></div>', unsafe_allow_html=True)

    # --- 6. 推移分析（2026年まで表示） ---
    st.markdown("### 📈 業績トレンド（月次）")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    fig = px.area(monthly, x='売上月', y='販売金額', title="月別販売推移", color_discrete_sequence=['#002D62'])
    fig.update_layout(xaxis_type='category', plot_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True)

    # --- 7. 詳細ピボット（2026/01を確実に表示） ---
    st.markdown("### 📋 販売詳細明細（得意先別・商品別）")
    mode = st.segmented_control("表示切り替え", ["販売金額", "数量"], default="販売金額")
    
    # 売上月のリストを明示的に取得してソート（2026/01を最後にするため）
    month_cols = sorted(f_df['売上月'].unique().tolist())
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    # カラムの並びを2026/01が最後になるように固定
    pivot = pivot.reindex(columns=month_cols)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 データをCSVで出力", pivot.to_csv().encode('utf_8_sig'), "KyushuTowa_Report.csv")

else:
    st.warning("対象データがありません。条件をリセットしてください。")
