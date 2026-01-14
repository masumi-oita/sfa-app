import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. 表示設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Analysis", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネス・デザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #002664; padding: 20px; color: white; text-align: center; border-radius: 10px; margin-bottom: 25px; }
    .card { background-color: white; border: 1px solid #e0e0e0; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); text-align: center; }
    .card-label { font-size: 0.9rem; color: #666; font-weight: bold; }
    .card-value { font-size: 1.8rem; color: #002664; font-weight: 800; }
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
    # SQL側でリネームした「売上月」でソート
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    
    # 既存のカラム名を整理
    df = df.rename(columns={'正規担当者名': '担当者', '包装単位': '包装'})
    
    # 日付フォーマットの統一 (2026/01等に対応)
    df['売上月'] = df['売上月'].astype(str).str.replace('-', '/')
    
    # 数値のクリーンアップ
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    # 重複列の最終排除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

# --- 4. メイン画面レイアウト ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析ダッシュボード</h1></div>', unsafe_allow_html=True)

try:
    df = load_data()
except Exception as e:
    st.error(f"データの読み込み中にエラーが発生しました。BigQueryのビューを最新版に更新してください。 詳細: {e}")
    st.stop()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 分析フィルタ")
        # 担当者（古賀優一郎に統一済み）
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先選択", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索", "")

    # フィルタ
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリーメトリクス ---
    sales = f_df['販売金額'].sum()
    qty = f_df['数量'].sum()
    cust = f_df['得意先名'].nunique()
    
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="card"><div class="card-label">販売金額累計</div><div class="card-value">¥{sales:,.0f}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="card"><div class="card-label">販売数量合計</div><div class="card-value">{qty:,.0f}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="card"><div class="card-label">稼働得意先数</div><div class="card-value">{cust:,} 軒</div></div>', unsafe_allow_html=True)

    # --- 6. 業績トレンド分析 (2026/01対応) ---
    st.markdown("### 📈 月別販売トレンド")
    monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index().sort_values('売上月')
    fig = px.area(monthly, x='売上月', y='販売金額', color_discrete_sequence=['#002664'])
    fig.update_layout(xaxis_type='category', plot_bgcolor='white', hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

    # --- 7. 詳細明細テーブル (最新月まで表示) ---
    st.markdown("### 📋 販売詳細明細（得意先別・製品別）")
    mode = st.radio("表示モード:", ["販売金額", "数量"], horizontal=True)
    
    # 月列を確実にソート (2026/01を最後にする)
    month_order = sorted(f_df['売上月'].unique().tolist())
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    
    # 最新月まで確実に並べる
    pivot = pivot.reindex(columns=month_order)
    pivot['期間合計'] = pivot.sum(axis=1)
    
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 データをCSV出力", pivot.to_csv().encode('utf_8_sig'), "sales_report.csv")

else:
    st.warning("表示条件に一致するデータがありません。")
