import streamlit as st
import pandas as pd
from google.cloud import bigquery
import json

# --- 1. ページ設定 ---
st.set_page_config(
    page_title="九州東和薬品　最強売上検索",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- カスタムCSS（省略：変更なし） ---
st.markdown("""
<style>
    .main-title { font-size: 2.2em; color: #0056b3; font-weight: bold; text-align: center; margin-bottom: 20px; border-bottom: 3px solid #0056b3; padding-bottom: 15px; }
    .sub-header { font-size: 1.3em; color: #333; background-color: #e3f2fd; border-left: 5px solid #0056b3; padding: 8px 15px; margin-top: 25px; margin-bottom: 15px; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# --- 2. BigQuery接続 ---
@st.cache_resource
def get_bigquery_client():
    try:
        service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        return client
    except Exception as e:
        st.error(f"BigQueryへの接続に失敗しました: {e}")
        return None

client = get_bigquery_client()

# --- 3. タイトル ---
st.markdown('<div class="main-title">💊 九州東和薬品　最強売上検索</div>', unsafe_allow_html=True)

if client:
    # --- 4. データ取得クエリ (作成したビューを参照) ---
    # ビュー側で計算済みの「最新単価」「実績金額」を取得します
    query = """
        SELECT 
            *,
            -- 包装単位がない場合に備え、存在しない場合は空文字を入れる
            IFNULL(CAST(包装単位 AS STRING), '-') AS 包装
        FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    """

    @st.cache_data(ttl=600)
    def load_data(query):
        try:
            df = client.query(query).to_dataframe()
            
            # --- カラム名のマッピング（ビュー名 -> Pythonで使っている名前） ---
            # ビュー作成時のカラム名に合わせて調整します
            rename_map = {
                '月': '売上日',
                '担当社員名': '担当者名',
                '品名': '商品名',
                '実績金額': '金額'
            }
            df = df.rename(columns=rename_map)
            
            # 日付列の整形（ビューが既に YYYY-MM 形式の文字列ならそのままでもOK）
            if '売上日' in df.columns:
                df['売上日'] = pd.to_datetime(df['売上日']).dt.strftime('%Y-%m')
            
            return df
        except Exception as e:
            st.error(f"データ取得エラー: {e}")
            return pd.DataFrame()

    df = load_data(query)

    if not df.empty:
        # --- 5. サイドバー（検索条件：変更なし） ---
        with st.sidebar:
            st.header("🔎 絞り込み条件")
            
            # 担当者
            tantosha_list = ['全て'] + sorted(df['担当者名'].dropna().unique().tolist()) if '担当者名' in df.columns else ['全て']
            selected_tantosha = st.selectbox("担当者名", tantosha_list)

            # 得意先
            if selected_tantosha != '全て':
                filtered_customers = df[df['担当者名'] == selected_tantosha]['得意先名'].unique()
                customer_list = ['全て'] + sorted(filtered_customers.tolist())
            else:
                customer_list = ['全て'] + sorted(df['得意先名'].dropna().unique().tolist())
            selected_customer = st.selectbox("得意先名", customer_list)

            # 商品名検索
            search_product = st.text_input("商品名検索（部分一致）", "")

        # --- データのフィルタリング ---
        filtered_df = df.copy()
        if selected_tantosha != '全て':
            filtered_df = filtered_df[filtered_df['担当者名'] == selected_tantosha]
        if selected_customer != '全て':
            filtered_df = filtered_df[filtered_df['得意先名'] == selected_customer]
        if search_product:
            filtered_df = filtered_df[filtered_df['商品名'].str.contains(search_product, na=False)]

        # --- 6. 全体サマリー ---
        st.markdown('<div class="sub-header">📈 実績サマリー</div>', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)

        total_qty = filtered_df['数量'].sum() if '数量' in filtered_df.columns else 0
        total_amount = filtered_df['金額'].sum() if '金額' in filtered_df.columns else 0
        unique_customers = filtered_df['得意先名'].nunique() if '得意先名' in filtered_df.columns else 0
        
        col1.metric("総数量", f"{total_qty:,.0f}")
        col2.metric("総金額", f"¥{total_amount:,.0f}")
        col3.metric("稼働得意先", f"{unique_customers:,} 軒")
        col4.metric("データ件数", f"{len(filtered_df):,} 件")

        # --- 7. ピボットテーブル ---
        st.markdown('<div class="sub-header">📊 詳細分析（得意先 × 商品 × 包装）</div>', unsafe_allow_html=True)

        view_mode = st.radio("表示項目を選択:", ["数量", "金額"], horizontal=True)
        value_col = '数量' if view_mode == "数量" else '金額'

        # インデックス列の設定（包装が含まれるようにする）
        index_cols = ['得意先名', '商品名', '包装'] if '包装' in filtered_df.columns else ['得意先名', '商品名']

        if all(col in filtered_df.columns for col in [value_col, '売上日']):
            try:
                pivot_df = pd.pivot_table(
                    filtered_df,
                    index=index_cols,
                    columns='売上日',
                    values=value_col,
                    aggfunc='sum',
                    fill_value=0
                )

                # ヒートマップ表示
                cmap_color = 'Blues' if view_mode == "数量" else 'Greens'
                styled_pivot = pivot_df.style.background_gradient(cmap=cmap_color, axis=None).format("{:,.0f}")

                st.dataframe(styled_pivot, use_container_width=True, height=700)
            
            except Exception as e:
                st.error(f"ピボットテーブル作成エラー: {e}")
        else:
            st.error(f"集計に必要な列（{value_col}, 売上日）が見つかりません。")

    else:
        st.info("条件に一致するデータが見つかりませんでした。")
else:
    st.stop()
