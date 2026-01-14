import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. 究極の設定（表示制限解除 & ページ構成） ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. スタイリッシュCSS（だささを徹底排除） ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans JP', sans-serif; }
    
    .main-header { background-color: #004098; padding: 20px; border-radius: 10px; color: white; text-align: center; margin-bottom: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
    .metric-card { background-color: white; border: 1px solid #e0e6ed; padding: 20px; border-radius: 8px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .metric-label { font-size: 0.9rem; color: #64748b; margin-bottom: 5px; font-weight: bold; }
    .metric-value { font-size: 1.8rem; color: #0f172a; font-weight: 700; }
    .stTable { border: none !important; }
</style>
""", unsafe_allow_html=True)

# --- 3. BigQuery接続 ---
@st.cache_resource
def get_client():
    info = json.loads(st.secrets["gcp_service_account"]["json_key"])
    return bigquery.Client.from_service_account_info(info)

client = get_client()

@st.cache_data(ttl=600)
def load_data():
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    df = client.query(query).to_dataframe()
    # 型の安全確保
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['売上利益'] = pd.to_numeric(df['売上利益'], errors='coerce').fillna(0)
    return df

# --- 4. メイン画面レイアウト ---
st.markdown('<div class="main-header"><h1>九州東和薬品 売上・利益分析ダッシュボード</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    # 5. サイドバー（プロ仕様）
    with st.sidebar:
        st.image("https://www.towa-yakuhin.co.jp/common/images/logo_head.png", width=150) # 例としてロゴ（任意）
        st.markdown("### 🔍 分析フィルタ")
        t_list = ['全 担当者'] + sorted(df['担当者名'].unique().tolist())
        sel_t = st.selectbox("担当者を選択", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者名'] == sel_t]
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先を選択", c_list)
        
        search = st.text_input("商品名・キーワード検索", "")

    # フィルタ適用
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if search: f_df = f_df[f_df['商品名'].str.contains(search, na=False)]

    # --- 6. エグゼクティブ・サマリー ---
    col1, col2, col3, col4 = st.columns(4)
    sales_total = f_df['販売金額'].sum()
    profit_total = f_df['売上利益'].sum()
    margin = (profit_total / sales_total * 100) if sales_total != 0 else 0
    
    with col1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">販売金額合計</div><div class="metric-value">¥{sales_total:,.0f}</div></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">総利益額</div><div class="metric-value">¥{profit_total:,.0f}</div></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="metric-card"><div class="metric-label">平均利益率</div><div class="metric-value">{margin:.1f}%</div></div>', unsafe_allow_html=True)
    with col4:
        st.markdown(f'<div class="metric-card"><div class="metric-label">対象得意先数</div><div class="metric-value">{f_df["得意先名"].nunique():,} 軒</div></div>', unsafe_allow_html=True)

    # --- 7. グラフィカル分析 ---
    st.markdown("### 📊 業績トレンド")
    g_col1, g_col2 = st.columns([2, 1])
    
    with g_col1:
        monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index()
        fig = px.area(monthly, x='売上月', y='販売金額', title="月別売上推移", color_discrete_sequence=['#004098'])
        fig.update_layout(plot_bgcolor='white', hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)
    
    with g_col2:
        top10 = f_df.groupby('商品名')['販売金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig2 = px.bar(top10, x='販売金額', y='商品名', orientation='h', title="商品別売上TOP10", color_discrete_sequence=['#22c55e'])
        fig2.update_layout(yaxis={'categoryorder':'total ascending'}, plot_bgcolor='white')
        st.plotly_chart(fig2, use_container_width=True)

    # --- 8. 精密ピボットテーブル ---
    st.markdown("### 📋 販売明細（得意先別・商品別）")
    mode = st.segmented_control("表示項目切替", ["販売金額", "数量", "利益率"], default="販売金額")
    
    try:
        if mode == "利益率":
            s_piv = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values='販売金額', aggfunc='sum', fill_value=0)
            p_piv = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values='売上利益', aggfunc='sum', fill_value=0)
            pivot = (p_piv / s_piv).fillna(0)
            styled = pivot.style.background_gradient(cmap='RdYlGn', axis=None).format("{:.1%}")
        else:
            val = '販売金額' if mode == "販売金額" else '数量'
            pivot = pd.pivot_table(f_df, index=['得意先名', '商品名', '包装単位'], columns='売上月', values=val, aggfunc='sum', fill_value=0)
            pivot['期間合計'] = pivot.sum(axis=1)
            styled = pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}")
        
        st.dataframe(styled, use_container_width=True, height=600)
        st.download_button("📥 データをエクスポート (CSV)", pivot.to_csv().encode('utf_8_sig'), "report.csv")
    
    except Exception as e:
        st.warning(f"詳細表示を生成中... (データが巨大なため、検索条件で絞り込んでください)")

else:
    st.info("データがありません。条件をリセットしてください。")
