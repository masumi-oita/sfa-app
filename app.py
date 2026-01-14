import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json
import plotly.express as px

# --- 1. ページ設定 ---
st.set_page_config(
    page_title="九州東和薬品　最強売上検索",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. カスタムCSS（デザイン調整） ---
st.markdown("""
<style>
    .main-title { font-size: 2.2em; color: #0056b3; font-weight: bold; text-align: center; border-bottom: 3px solid #0056b3; padding-bottom: 10px; margin-bottom: 20px;}
    .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #dee2e6; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .sub-header { font-size: 1.5em; color: #333; margin-top: 30px; margin-bottom: 15px; padding-left: 10px; border-left: 5px solid #0056b3; }
</style>
""", unsafe_allow_html=True)

# --- 3. BigQuery接続設定 ---
@st.cache_resource
def get_bigquery_client():
    try:
        service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        return bigquery.Client.from_service_account_info(service_account_info)
    except Exception as e:
        st.error(f"BigQuery接続エラー: {e}")
        return None

client = get_bigquery_client()

# --- 4. データ取得と加工 ---
@st.cache_data(ttl=600)
def load_data():
    if not client:
        return pd.DataFrame()
    
    query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`"
    
    try:
        df = client.query(query).to_dataframe()
        
        # --- 列名の名寄せと重複排除 ---
        # 1. 元々あるかもしれない「商品名」を削除（品名から変換する方を優先するため）
        if '商品名' in df.columns and '品名' in df.columns:
            df = df.drop(columns=['商品名'])

        # 2. ビューの日本語名をPython変量名にマッピング
        rename_map = {
            '年月': '売上日',
            '品名': '商品名',
            '包装単位': '包装',
            '担当社員名': '担当者名',
            '実績金額': '金額'
        }
        df = df.rename(columns=rename_map)

        # 3. それでも重複する列があれば最初の1つを残して削除（エラー回避の要）
        df = df.loc[:, ~df.columns.duplicated()].copy()

        # 4. データ型の最適化
        df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0)
        df['数量'] = pd.to_numeric(df['数量'], errors='coerce').fillna(0)
        df['売上日'] = df['売上日'].astype(str)
        
        return df
    except Exception as e:
        st.error(f"データ取得・加工エラー: {e}")
        return pd.DataFrame()

# タイトル表示
st.markdown('<div class="main-title">💊 九州東和薬品　最強売上検索 (SFA詳細版)</div>', unsafe_allow_html=True)

df_raw = load_data()

if not df_raw.empty:
    # --- 5. サイドバー（絞り込み） ---
    with st.sidebar:
        st.header("🔎 絞り込み条件")
        
        # 担当者（名寄せ済みの名前）
        tantosha_list = ['全て'] + sorted(df_raw['担当者名'].unique().tolist())
        selected_tantosha = st.selectbox("担当者選択", tantosha_list)
        
        # 担当者に連動した得意先
        filtered_df = df_raw if selected_tantosha == '全て' else df_raw[df_raw['担当者名'] == selected_tantosha]
        customer_list = ['全て'] + sorted(filtered_df['得意先名'].unique().tolist())
        selected_customer = st.selectbox("得意先選択", customer_list)
        
        # キーワード検索
        search_keyword = st.text_input("商品名キーワード検索", "")

    # フィルタ適用
    display_df = filtered_df.copy()
    if selected_customer != '全て':
        display_df = display_df[display_df['得意先名'] == selected_customer]
    if search_keyword:
        display_df = display_df[display_df['商品名'].str.contains(search_keyword, na=False)]

    # --- 6. 実績サマリー（メトリクス） ---
    st.markdown('<div class="sub-header">📈 実績サマリー</div>', unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("総販売数量", f"{display_df['数量'].sum():,.0f}")
    m2.metric("売上金額累計", f"¥{display_df['金額'].sum():,.0f}")
    m3.metric("稼働得意先数", f"{display_df['得意先名'].nunique():,} 軒")
    m4.metric("データ件数", f"{len(display_df):,} 件")

    # --- 7. ビジュアル分析（推移とランキング） ---
    st.markdown('<div class="sub-header">📊 視覚的分析</div>', unsafe_allow_html=True)
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.write("**▼ 月別売上推移**")
        monthly_data = display_df.groupby('売上日')['金額'].sum().reset_index()
        fig_line = px.bar(monthly_data, x='売上日', y='金額', color_discrete_sequence=['#0056b3'])
        fig_line.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig_line, use_container_width=True)

    with chart_col2:
        st.write("**▼ 商品別売上TOP10**")
        # ここで groupby('商品名') を行う際、列名重複がないためエラーになりません
        top_products = display_df.groupby('商品名')['金額'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_rank = px.bar(top_products, x='金額', y='商品名', orientation='h', color_discrete_sequence=['#28a745'])
        fig_rank.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=0), yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_rank, use_container_width=True)

    # --- 8. 詳細ピボットテーブル ---
    st.markdown('<div class="sub-header">📋 月別詳細明細</div>', unsafe_allow_html=True)
    
    view_mode = st.radio("表示する値:", ["金額", "数量"], horizontal=True)
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
        
        # 合計列を追加
        pivot_table['合計'] = pivot_table.sum(axis=1)
        
        # スタイリング
        styled_pivot = pivot_table.style.background_gradient(
            cmap='Greens' if view_mode == "金額" else 'Blues', axis=None
        ).format("{:,.0f}")
        
        st.dataframe(styled_pivot, use_container_width=True, height=600)
        
        # CSVダウンロード
        csv = pivot_table.to_csv().encode('utf_8_sig')
        st.download_button(
            label="この集計結果をCSVでダウンロード",
            data=csv,
            file_name=f"sales_data_{view_mode}.csv",
            mime='text/csv',
        )
        
    except Exception as e:
        st.error(f"詳細テーブルの作成に失敗しました。データ構成を確認してください: {e}")

else:
    st.warning("データが読み込めませんでした。BigQueryのビュー 'v_sales_performance_for_python' を確認してください。")
