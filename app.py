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

# --- カスタムCSS（可読性向上） ---
st.markdown("""
<style>
    .main-title {
        font-size: 2.2em;
        color: #0056b3;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
        border-bottom: 3px solid #0056b3;
        padding-bottom: 15px;
    }
    .sub-header {
        font-size: 1.3em;
        color: #333;
        background-color: #e3f2fd;
        border-left: 5px solid #0056b3;
        padding: 8px 15px;
        margin-top: 25px;
        margin-bottom: 15px;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. BigQuery接続設定 ---
@st.cache_resource
def get_bigquery_client():
    try:
        # StreamlitのSecretsから認証情報を取得
        service_account_info = json.loads(st.secrets["gcp_service_account"]["json_key"])
        client = bigquery.Client.from_service_account_info(service_account_info)
        return client
    except Exception as e:
        st.error(f"BigQueryへの接続に失敗しました。Secretsの設定を確認してください: {e}")
        return None

client = get_bigquery_client()

# --- 3. メインタイトル ---
st.markdown('<div class="main-title">💊 九州東和薬品　最強売上検索 (SFA連携版)</div>', unsafe_allow_html=True)

if client:
    # --- 4. データ取得クエリ (ビューを参照) ---
    # 日本語のカラム名は必ず ` ` (バッククォート) で囲む
    query = """
        SELECT 
            *,
            IFNULL(CAST(`包装単位` AS STRING), '-') AS `包装`
        FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    """

    @st.cache_data(ttl=600)
    def load_data(query):
        try:
            # データをDataFrameとして取得
            df = client.query(query).to_dataframe()
            
            # --- カラム名のマッピング（ビューの日本語名 -> アプリ内での英数/日本語名） ---
            # ビューの定義に合わせて置換します
            rename_map = {
                '月': '売上日',
                '担当社員名': '担当者名',
                '品名': '商品名',
                '実績金額': '金額'
            }
            df = df.rename(columns=rename_map)
            
            # 日付型への変換（ソート用）
            if '売上日' in df.columns:
                df['売上日'] = pd.to_datetime(df['売上日']).dt.strftime('%Y-%m')
            
            return df
        except Exception as e:
            st.error(f"データ取得エラー: {e}")
            return pd.DataFrame()

    df = load_data(query)

    if not df.empty:
        # --- 5. サイドバー：検索・絞り込み ---
        with st.sidebar:
            st.header("🔎 絞り込み条件")
            
            # 担当者フィルタ
            tantosha_list = ['全て'] + sorted(df['担当者名'].dropna().unique().tolist()) if '担当者名' in df.columns else ['全て']
            selected_tantosha = st.selectbox("担当者名で絞り込み", tantosha_list)

            # 得意先フィルタ（担当者に連動）
            if selected_tantosha != '全て':
                filtered_customers = df[df['担当者名'] == selected_tantosha]['得意先名'].unique()
                customer_list = ['全て'] + sorted(filtered_customers.tolist())
            else:
                customer_list = ['全て'] + sorted(df['得意先名'].dropna().unique().tolist())
            selected_customer = st.selectbox("得意先名で絞り込み", customer_list)

            # 商品名検索（部分一致）
            search_product = st.text_input("商品名検索 (キーワード入力)", "")

        # --- フィルタリングの実行 ---
        filtered_df = df.copy()
        if selected_tantosha != '全て':
            filtered_df = filtered_df[filtered_df['担当者名'] == selected_tantosha]
        if selected_customer != '全て':
            filtered_df = filtered_df[filtered_df['得意先名'] == selected_customer]
        if search_product:
            filtered_df = filtered_df[filtered_df['商品名'].str.contains(search_product, na=False, case=False)]

        # --- 6. 実績サマリー表示 ---
        st.markdown('<div class="sub-header">📈 実績サマリー</div>', unsafe_allow_html=True)
        m1, m2, m3, m4 = st.columns(4)

        # 各指標の計算
        qty_val = filtered_df['数量'].sum() if '数量' in filtered_df.columns else 0
        amt_val = filtered_df['金額'].sum() if '金額' in filtered_df.columns else 0
        cust_count = filtered_df['得意先名'].nunique() if '得意先名' in filtered_df.columns else 0
        data_count = len(filtered_df)

        m1.metric("総販売数量", f"{qty_val:,.0f}")
        m2.metric("売上金額累計", f"¥{amt_val:,.0f}")
        m3.metric("対象得意先数", f"{cust_count:,} 軒")
        m4.metric("取引レコード数", f"{data_count:,} 件")

        # --- 7. 詳細分析ピボットテーブル ---
        st.markdown('<div class="sub-header">📊 得意先別・月別詳細（数量 / 金額 切り替え）</div>', unsafe_allow_html=True)

        # 表示モード選択
        view_mode = st.radio("表示する値を選択してください:", ["数量", "金額"], horizontal=True)
        val_col = '数量' if view_mode == "数量" else '金額'

        # ピボットテーブルのインデックス設定
        idx_cols = ['得意先名', '商品名', '包装']
        
        # 必要な列が存在するか確認
        if all(c in filtered_df.columns for c in idx_cols + ['売上日', val_col]):
            try:
                # 集計の実行
                pivot_data = pd.pivot_table(
                    filtered_df,
                    index=idx_cols,
                    columns='売上日',
                    values=val_col,
                    aggfunc='sum',
                    fill_value=0
                )

                # 合計列の追加（右端）
                pivot_data['合計'] = pivot_data.sum(axis=1)
                
                # スタイリング（ヒートマップ適用）
                color_map = 'Blues' if view_mode == "数量" else 'Greens'
                styled_df = pivot_data.style.background_gradient(cmap=color_map, axis=None).format("{:,.0f}")

                # テーブル表示
                st.dataframe(styled_df, use_container_width=True, height=600)
                
                # CSVダウンロード機能
                csv = pivot_data.to_csv().encode('utf_8_sig')
                st.download_button(
                    label="📊 集計結果をCSVでダウンロード",
                    data=csv,
                    file_name=f"sales_summary_{view_mode}.csv",
                    mime='text/csv',
                )

            except Exception as e:
                st.error(f"ピボットテーブルの生成中にエラーが発生しました: {e}")
        else:
            st.warning("集計に必要なカラムがデータセットに見つかりません。ビューの構成を確認してください。")

    else:
        st.info("指定された条件に一致する実績データがありません。")

else:
    st.error("BigQueryクライアントを初期化できませんでした。設定を再確認してください。")

# フッター
st.markdown("---")
st.caption("© 2026 Kyushu Towa Pharmaceutical Co., Ltd. - Sales Data Analysis System")
