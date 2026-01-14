import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. ビジネス・デザインCSS ---
st.markdown("""
<style>
    .main-header { background-color: #004098; padding: 1.5rem; color: white; text-align: center; border-radius: 8px; margin-bottom: 2rem; }
    .stMetric { background-color: #ffffff; border: 1px solid #e2e8f0; padding: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
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
    # カラム名のマッピング（基本項目を活かす）
    df = df.rename(columns={
        '正規担当者名': '担当者',
        '年月': '売上月',
        '品名': '商品名',
        '包装単位': '包装'
    })
    return df

# --- 4. メイン表示 ---
st.markdown('<div class="main-header"><h1>九州東和薬品　売上・利益分析ダッシュボード</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    with st.sidebar:
        st.markdown("### 🔍 検索・フィルタ")
        # 担当者リスト（古賀優一郎に統一済み）
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先選択", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索", "")

    # フィルタ適用
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. エグゼクティブ・サマリー ---
    c1, c2, c3, c4 = st.columns(4)
    sales = f_df['販売金額'].sum()
    profit = f_df['売上利益'].sum()
    margin = (profit / sales * 100) if sales != 0 else 0
    
    with c1: st.metric("販売金額合計", f"¥{sales:,.0f}")
    with c2: st.metric("売上利益合計", f"¥{profit:,.0f}")
    with c3: st.metric("平均利益率", f"{margin:.1f}%")
    with c4: st.metric("対象得意先", f"{f_df['得意先名'].nunique():,} 軒")

    # --- 6. チャート分析 ---
    st.markdown("### 📊 トレンドと構成")
    g1, g2 = st.columns([2, 1])
    with g1:
        monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index()
        fig_line = px.line(monthly, x='売上月', y='販売金額', title="月別販売推移", markers=True)
        st.plotly_chart(fig_line, use_container_width=True)
    with g2:
        top10 = f_df.groupby('商品名')['販売金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_bar = px.bar(top10, x='販売金額', y='商品名', orientation='h', title="売上TOP10製品")
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- 7. 詳細明細テーブル ---
    st.markdown("### 📋 詳細データ（得意先・商品別）")
    mode = st.radio("表示項目を選択:", ["販売金額", "数量", "売上利益"], horizontal=True)
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    pivot['期間合計'] = pivot.sum(axis=1)
    
    # 視認性の高いスタイル
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=500
    )
    
    st.download_button("📥 分析結果をエクスポート(CSV)", pivot.to_csv().encode('utf_8_sig'), "sales_profit_report.csv")

else:
    st.info("データが読み込めませんでした。")
