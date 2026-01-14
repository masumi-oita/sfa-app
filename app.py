import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. システム・パフォーマンス設定 ---
st.set_page_config(
    page_title="Kyushu Towa SFA Analysis",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 詳細テーブルの描画制限を200万セルに拡張（エラー回避）
pd.set_option("styler.render.max_elements", 2000000)

# --- 2. エグゼクティブ・デザインCSS ---
st.markdown("""
<style>
    /* 全体のフォントと背景 */
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans JP', sans-serif; }

    /* メインヘッダー */
    .main-header { 
        background-color: #003366; 
        padding: 1.5rem; 
        color: white; 
        text-align: center; 
        border-radius: 10px; 
        margin-bottom: 2rem; 
        box-shadow: 0 4px 10px rgba(0,0,0,0.1);
    }
    
    /* メトリクスカード */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
        text-align: center;
    }
    [data-testid="stMetricLabel"] { font-size: 1rem !important; font-weight: bold !important; color: #64748b !important; }
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 800 !important; color: #003366 !important; }

    /* サブヘッダー */
    .sub-header { 
        font-size: 1.4em; 
        color: #333; 
        margin-top: 2rem; 
        margin-bottom: 1rem; 
        padding-left: 10px; 
        border-left: 6px solid #003366; 
        background: #f8fafc;
        line-height: 2;
    }
</style>
""", unsafe_allow_html=True)

# --- 3. データ取得・加工ロジック ---
@st.cache_resource
def get_client():
    try:
        info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(info)
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        return None

@st.cache_data(ttl=600)
def load_data():
    client = get_client()
    if not client: return pd.DataFrame()
    
    # 全項目（a.*）を取得。最新順でソート。
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上月` ASC"
    df = client.query(query).to_dataframe()
    
    # 重複列のクリーニング（品名を商品名として統合）
    if '品名' in df.columns:
        if '商品名' in df.columns: df = df.drop(columns=['商品名'])
        df = df.rename(columns={'品名': '商品名'})
    
    # カラム名のマッピング（基本項目はそのまま残る）
    df = df.rename(columns={
        '正規担当者名': '担当者',
        '売上月': '月次',
        '包装単位': '包装'
    })
    
    # 2026/01を正しく表示するための日付整形
    df['月次'] = df['月次'].astype(str).str.replace('-', '/')
    
    # 数値型の確定
    df['販売金額'] = pd.to_numeric(df['販売金額'], errors='coerce').fillna(0)
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
    
    # 列重複を物理的に完全排除
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

# --- 4. メイン画面レイアウト ---
st.markdown('<div class="main-header"><h1>九州東和薬品　販売実績分析ダッシュボード</h1></div>', unsafe_allow_html=True)

df = load_data()

if not df.empty:
    # --- サイドバー・フィルタ ---
    with st.sidebar:
        st.markdown("### 🔍 分析フィルタ")
        # 担当者（SQL側で古賀優一郎に統合済み）
        t_list = ['全 担当者'] + sorted(df['担当者'].unique().tolist())
        sel_t = st.selectbox("担当者名", t_list)
        
        target_df = df if sel_t == '全 担当者' else df[df['担当者'] == sel_t]
        
        c_list = ['全 得意先'] + sorted(target_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先名", c_list)
        
        kw = st.text_input("商品名・ユニークコード検索", "")

    # フィルタ適用
    f_df = target_df.copy()
    if sel_c != '全 得意先': f_df = f_df[f_df['得意先名'] == sel_c]
    if kw: 
        f_df = f_df[f_df['商品名'].str.contains(kw, na=False) | f_df['ユニークコード'].str.contains(kw, na=False)]

    # --- 5. サマリーメトリクス ---
    m_sales = f_df['販売金額'].sum()
    m_qty = f_df['数量'].sum()
    m_cust = f_df['得意先名'].nunique()
    
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("販売金額 累計", f"¥{m_sales:,.0f}")
    with c2: st.metric("販売数量 合計", f"{m_qty:,.0f}")
    with c3: st.metric("稼働得意先数", f"{m_cust:,} 軒")

    # --- 6. チャート分析 ---
    st.markdown('<div class="sub-header">📊 業績トレンド分析</div>', unsafe_allow_html=True)
    g1, g2 = st.columns([2, 1])
    
    with g1:
        # 月別推移（2026/01まで表示）
        monthly = f_df.groupby('月次')['販売金額'].sum().reset_index().sort_values('月次')
        fig_line = px.area(monthly, x='月次', y='販売金額', title="月次販売トレンド", color_discrete_sequence=['#003366'])
        fig_line.update_layout(xaxis_type='category', plot_bgcolor='white', hovermode='x unified')
        st.plotly_chart(fig_line, use_container_width=True)
    
    with g2:
        # 商品別ランキング
        top10 = f_df.groupby('商品名')['販売金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_bar = px.bar(top10, x='販売金額', y='商品名', orientation='h', title="製品別TOP10", color_discrete_sequence=['#10b981'])
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, plot_bgcolor='white')
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- 7. 詳細明細テーブル ---
    st.markdown('<div class="sub-header">📋 詳細実績明細（得意先別）</div>', unsafe_allow_html=True)
    mode = st.radio("表示項目の切替:", ["販売金額", "数量"], horizontal=True)
    
    try:
        # 時系列順を維持してピボット作成
        all_months = sorted(f_df['月次'].unique().tolist())
        
        pivot = pd.pivot_table(
            f_df, 
            index=['得意先名', '商品名', '包装'], 
            columns='月次', 
            values=mode, 
            aggfunc='sum', 
            fill_value=0
        )
        
        # 2026/01を確実に右端にするための並び替え
        pivot = pivot.reindex(columns=all_months)
        pivot['期間合計'] = pivot.sum(axis=1)
        
        # ヒートマップ表示
        st.dataframe(
            pivot.style.background_gradient(cmap='Blues', axis=None).format("{:,.0f}"),
            use_container_width=True, height=600
        )
        
        # CSV出力
        st.download_button(
            label="📥 分析結果をCSVで保存",
            data=pivot.to_csv().encode('utf_8_sig'),
            file_name=f"KyushuTowa_Sales_{mode}.csv",
            mime='text/csv'
        )
        
    except Exception as e:
        st.warning(f"詳細テーブルの作成中... 条件を絞り込むとスムーズに表示されます。")

else:
    st.info("データが読み込めませんでした。条件をリセットしてください。")
