import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json

# --- 1. ページ設定 ---
st.set_page_config(
    page_title="九州東和薬品　採用マスタ",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- カスタムCSS（デザイン調整） ---
st.markdown("""
<style>
    /* メインタイトルのスタイル */
    .main-title {
        font-size: 2.5em;
        color: #0056b3;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
        border-bottom: 3px solid #0056b3;
        padding-bottom: 15px;
    }
    /* サブヘッダーのスタイル */
    .sub-header {
        font-size: 1.4em;
        color: #444;
        background-color: #f8f9fa;
        border-left: 5px solid #0056b3;
        padding: 10px 15px;
        margin-top: 30px;
        margin-bottom: 15px;
        border-radius: 0 5px 5px 0;
    }
    /* 指標（メトリクス）カードのデザイン */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
</style>
""", unsafe_allow_html=True)

# --- 2. BigQueryへの接続設定 ---
@st.cache_resource
def get_bigquery_client():
    try:
        # st.secretsから認証情報を取得
        service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        return client
    except Exception as e:
        st.error(f"BigQueryへの接続に失敗しました: {e}")
        return None

client = get_bigquery_client()

# --- 3. メイン画面タイトル ---
st.markdown('<div class="main-title">💊 九州東和薬品　採用マスタ</div>', unsafe_allow_html=True)

if client:
    # --- 4. データ取得クエリ ---
    # ※テーブル名が変更になる場合はここを修正してください
    query = """
        SELECT *
        FROM `salesdb-479915.DATA.sales_data`
        LIMIT 2000
    """

    @st.cache_data(ttl=600)
    def load_data(query):
        try:
            df = client.query(query).to_dataframe()
            # 日付列があれば「年月」形式（YYYY-MM）に変換して見やすくする
            if '売上日' in df.columns:
                df['売上日'] = pd.to_datetime(df['売上日']).dt.strftime('%Y-%m')
            return df
        except Exception as e:
            st.error(f"データ取得エラー: {e}")
            return pd.DataFrame()

    df = load_data(query)

    if not df.empty:
        # --- 5. サイドバー（検索条件） ---
        with st.sidebar:
            st.header("🔎 絞り込み検索")
            
            # 担当者名での絞り込み
            if '担当者名' in df.columns:
                tantosha_list = ['全て'] + sorted(df['担当者名'].dropna().unique().tolist())
                selected_tantosha = st.selectbox("担当者名", tantosha_list)
            else:
                selected_tantosha = '全て'

            # 得意先名での絞り込み（オプション）
            if '得意先名' in df.columns:
                # 担当者が選ばれていれば、その担当者の得意先だけを表示
                if selected_tantosha != '全て':
                    filtered_customers = df[df['担当者名'] == selected_tantosha]['得意先名'].unique()
                    customer_list = ['全て'] + sorted(filtered_customers.tolist())
                else:
                    customer_list = ['全て'] + sorted(df['得意先名'].dropna().unique().tolist())
                
                selected_customer = st.selectbox("得意先名", customer_list)
            else:
                selected_customer = '全て'

        # --- データのフィルタリング処理 ---
        filtered_df = df.copy()
        
        if selected_tantosha != '全て':
            filtered_df = filtered_df[filtered_df['担当者名'] == selected_tantosha]
        
        if selected_customer != '全て':
            filtered_df = filtered_df[filtered_df['得意先名'] == selected_customer]

        # --- 6. 重要指標（メトリクス）の表示 ---
        st.markdown('<div class="sub-header">📈 全体サマリー</div>', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)

        # 各種集計（列名が存在する場合のみ計算）
        total_qty = filtered_df['数量'].sum() if '数量' in filtered_df.columns else 0
        total_amount = filtered_df['金額'].sum() if '金額' in filtered_df.columns else 0
        unique_customers = filtered_df['得意先名'].nunique() if '得意先名' in filtered_df.columns else 0
        unique_products = filtered_df['商品名'].nunique() if '商品名' in filtered_df.columns else 0

        col1.metric("総数量", f"{total_qty:,.0f}")
        col2.metric("総金額", f"¥{total_amount:,.0f}")
        col3.metric("稼働得意先数", f"{unique_customers:,} 軒")
        col4.metric("採用品目数", f"{unique_products:,} 品目")

        # --- 7. ピボットテーブル（採用状況一覧） ---
        st.markdown('<div class="sub-header">📊 採用状況・推移（得意先 × 商品）</div>', unsafe_allow_html=True)

        # ピボットに必要な列があるか確認
        required_cols = ['得意先名', '商品名', '売上日', '数量']
        
        if all(col in filtered_df.columns for col in required_cols):
            try:
                # ピボットテーブル作成
                pivot_df = pd.pivot_table(
                    filtered_df,
                    index=['得意先名', '商品名'],  # 縦軸
                    columns='売上日',             # 横軸（年月）
                    values='数量',               # 集計値
                    aggfunc='sum',
                    fill_value=0
                )

                # スタイリング（ヒートマップ：数字が大きいほど濃い青）
                styled_pivot = (
                    pivot_df.style
                    .background_gradient(cmap='Blues', axis=None)
                    .format("{:,.0f}")  # 3桁区切り
                )

                # 表示
                st.dataframe(styled_pivot, use_container_width=True, height=600)
            
            except Exception as e:
                st.error(f"ピボットテーブル作成中にエラーが発生しました: {e}")
        else:
            st.warning("集計に必要な列（得意先名, 商品名, 売上日, 数量）の一部が見つかりません。")
            with st.expander("現在のデータを確認"):
                st.dataframe(filtered_df)

    else:
        st.info("データが見つかりませんでした。BigQueryのテーブルを確認してください。")

else:
    st.stop()
