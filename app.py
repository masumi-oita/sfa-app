import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. ページ設定と表示制限解除 ---
st.set_page_config(
    page_title="九州東和薬品　最強売上検索",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 詳細テーブルのセル表示制限を100万セルに拡張
pd.set_option("styler.render.max_elements", 1000000)

# --- 2. カスタムCSS（サマリー2行表示 & 全文表示用） ---
st.markdown("""
<style>
    .main-title { font-size: 2.2em; color: #0056b3; font-weight: bold; text-align: center; border-bottom: 3px solid #0056b3; padding-bottom: 10px; margin-bottom: 20px;}
    
    /* メトリクスカード全体の調整 */
    [data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #dee2e6;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        min-height: 140px;
    }
    /* ラベル（タイトル）を2行表示し、全文見せる */
    [data-testid="stMetricLabel"] > div {
        font-size: 1.05em !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        line-height: 1.3 !important;
        min-height: 2.6em !important;
    }
    /* 数値を全文表示 */
    [data-testid="stMetricValue"] > div {
        font-size: 1.6em !important;
        white-space: nowrap !important;
    }
    .sub-header { font-size: 1.5em; color: #333; margin-top: 30px; margin-bottom: 15px; padding-left: 10px; border-left: 5px solid #0056b3; }
</style>
""", unsafe_allow_html=True)

# --- 3. BigQuery接続 ---
@st.cache_resource
def get_bigquery_client():
    try:
        service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(service_account_info)
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        return None

client = get_bigquery_client()

# --- 4. データ読み込み ---
@st.cache_data(ttl=600)
def load_data():
    if not client: return pd.DataFrame()
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    try:
        df = client.query(query).to_dataframe()
        
        # 重複列のクリーニング
        if '商品名' in df.columns and '品名' in df.columns:
            df = df.drop(columns=['商品名'])

        rename_map = {
            '年月': '売上日', '品名': '商品名', '包装単位': '包装',
            '担当社員名': '担当者名', '実績金額': '金額'
        }
        df = df.rename(columns=rename_map)
        df = df.loc[:, ~df.columns.duplicated()].copy()

        # 型の変換
        df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0)
        df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
        df['売上日'] = df['売上日'].astype(str)
        return df
    except Exception as e:
        st.error(f"データ処理エラー: {e}")
        return pd.DataFrame()

st.markdown('<div class="main-title">💊 九州東和薬品　最強売上検索 (SFA完全版)</div>', unsafe_allow_html=True)
df_raw = load_data()

if not df_raw.empty:
    # --- 5. サイドバー ---
    with st.sidebar:
        st.header("🔎 絞り込み条件")
        tantosha_list = ['全て'] + sorted(df_raw['担当者名'].unique().tolist())
        sel_t = st.selectbox("担当者選択", tantosha_list)
        
        filtered_df = df_raw if sel_t == '全て' else df_raw[df_raw['担当者名'] == sel_t]
        c_list = ['全て'] + sorted(filtered_df['得意先名'].unique().tolist())
        sel_c = st.selectbox("得意先選択", c_list)
        search_kw = st.text_input("商品名キーワード検索", "")

    # フィルタリング
    display_df = filtered_df.copy()
    if sel_c != '母親': # '全て'のタイポ修正
        if sel_c != '全て': display_df = display_df[display_df['得意先名'] == sel_c]
    if search_kw: display_df = display_df[display_df['商品名'].str.contains(search_kw, na=False)]

    # --- 6. 実績サマリー（2行表示対応） ---
    st.markdown('<div class="sub-header">📈 実績サマリー</div>', unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    
    m1.metric("総販売数量\n(バラ換算合計)", f"{display_df['数量'].sum():,.0f}")
    m2.metric("売上金額累計\n(最新納入単価基準)", f"¥{display_df['金額'].sum():,.0f}")
    m3.metric("稼働得意先数\n(表示条件内)", f"{display_df['得意先名'].nunique():,} 軒")
    m4.metric("取引データ件数\n(明細行数合計)", f"{len(display_df):,} 件")

    # --- 7. グラフ分析 ---
    st.markdown('<div class="sub-header">📊 視覚的分析</div>', unsafe_allow_html=True)
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.write("**▼ 月別売上推移**")
        monthly_data = display_df.groupby('売上日')['金額'].sum().reset_index()
        fig_line = px.bar(monthly_data, x='売上日', y='金額', color_discrete_sequence=['#0056b3'])
        st.plotly_chart(fig_line, use_container_width=True)

    with chart_col2:
        st.write("**▼ 商品別売上TOP10**")
        top_products = display_df.groupby('商品名')['金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_rank = px.bar(top_products, x='金額', y='商品名', orientation='h', color_discrete_sequence=['#28a745'])
        fig_rank.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_rank, use_container_width=True)

    # --- 8. 詳細テーブル ---
    st.markdown('<div class="sub-header">📋 月別詳細明細 (得意先×商品×包装)</div>', unsafe_allow_html=True)
    view_mode = st.radio("表示する値を選択:", ["金額", "数量"], horizontal=True)
    val_col = '金額' if view_mode == "金額" else '数量'
    
    try:
        pivot_table = pd.pivot_table(
            display_df,
            index=['得意先名', '商品名', '包装'],
            columns='売上日',
            values=val_col,
            aggfunc='sum',
            fill_value=0
        )
        pivot_table['合計'] = pivot_table.sum(axis=1)
        
        # 色付け処理 (matplotlibがない場合も考慮)
        try:
            styled_df = pivot_table.style.background_gradient(cmap='Greens' if view_mode == "金額" else 'Blues', axis=None).format("{:,.0f}")
        except:
            styled_df = pivot_table.style.format("{:,.0f}")
            st.info("※ライブラリ読み込み中のため、背景色なしで表示しています。")
        
        st.dataframe(styled_df, use_container_width=True, height=600)
        
        csv = pivot_table.to_csv().encode('utf_8_sig')
        st.download_button(label="集計結果をCSVでダウンロード", data=csv, file_name="sales_export.csv", mime='text/csv')
        
    except Exception as e:
        st.error(f"テーブル作成失敗: {e}")

else:
    st.warning("表示するデータがありません。条件を変更してください。")
