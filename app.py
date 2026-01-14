import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム・表示設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. プロフェッショナルCSS（だささを排除） ---
st.markdown("""
<style>
    .main-header { background-color: #003366; padding: 1.5rem; color: white; text-align: center; border-radius: 10px; margin-bottom: 2rem; }
    .metric-card { background-color: #ffffff; border: 1px solid #e2e8f0; padding: 1.2rem; border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); text-align: center; }
    .metric-title { font-size: 0.9rem; color: #64748b; font-weight: bold; margin-bottom: 5px; }
    .metric-value { font-size: 1.6rem; color: #0f172a; font-weight: 800; }
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
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    df = client.query(query).to_dataframe()
    
    # --- 重複列の完全解消ロジック ---
    # 「品名」があればそれを「商品名」とし、既存の「商品名」系は削除
    if '品名' in df.columns:
        if '商品名' in df.columns: df = df.drop(columns=['商品名'])
        df = df.rename(columns={'品名': '商品名'})
    
    # その他のマッピング
    df = df.rename(columns={
        '担当者名_正規化': '担当者',
        '年月': '売上月',
        '包装単位': '包装'
    })
    
    # 最終チェック：列名が重複していたら強制削除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 数値変換の徹底
    for col in ['販売金額', '売上利益', '数量']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

# --- 4. メイン画面レイアウト ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売・利益分析ダッシュボード</h1></div>', unsafe_allow_html=True)

df = load_data()

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

    # フィルタリング適用
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリー表示（洗練されたカード型） ---
    sales = f_df['販売金額'].sum()
    profit = f_df['売上利益'].sum()
    margin = (profit / sales * 100) if sales != 0 else 0
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(f'<div class="metric-card"><div class="metric-title">販売金額合計</div><div class="metric-value">¥{sales:,.0f}</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><div class="metric-title">売上利益額</div><div class="metric-value">¥{profit:,.0f}</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><div class="metric-title">利益率</div><div class="metric-value">{margin:.1f}%</div></div>', unsafe_allow_html=True)
    with c4: st.markdown(f'<div class="metric-card"><div class="metric-title">稼働得意先数</div><div class="metric-value">{f_df["得意先名"].nunique():,} 軒</div></div>', unsafe_allow_html=True)

    # --- 6. 視覚化セクション ---
    st.markdown("### 📈 実績推移とランキング")
    g1, g2 = st.columns([2, 1])
    with g1:
        monthly = f_df.groupby('売上月')['販売金額'].sum().reset_index()
        fig_line = px.line(monthly, x='売上月', y='販売金額', title="月別販売トレンド", markers=True, color_discrete_sequence=['#003366'])
        st.plotly_chart(fig_line, use_container_width=True)
    with g2:
        top10 = f_df.groupby('商品名')['販売金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_bar = px.bar(top10, x='販売金額', y='商品名', orientation='h', title="製品別TOP10", color_discrete_sequence=['#10b981'])
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- 7. 詳細ピボットテーブル ---
    st.markdown("### 📋 販売・利益詳細明細")
    mode = st.radio("表示切り替え:", ["販売金額", "数量", "売上利益"], horizontal=True)
    
    pivot = pd.pivot_table(
        f_df, 
        index=['得意先名', '商品名', '包装'], 
        columns='売上月', 
        values=mode, 
        aggfunc='sum', 
        fill_value=0
    )
    pivot['期間合計'] = pivot.sum(axis=1)
    
    # 視認性の高いヒートマップ
    st.dataframe(
        pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
        use_container_width=True, height=600
    )
    
    st.download_button("📥 集計結果をCSVで出力", pivot.to_csv().encode('utf_8_sig'), "sales_profit_report.csv")

else:
    st.info("表示するデータがありません。条件をリセットしてください。")
