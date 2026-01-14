import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# ページ設定
st.set_page_config(page_title="医薬品卸SFA", layout="wide")

# ★ここがミソ：Streamlitに保存した「鍵」を読み込む
# secretsから情報を取得して認証情報を作成
key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
creds = service_account.Credentials.from_service_account_info(key_dict)

@st.cache_data(ttl=600) # 10分間キャッシュ（サクサク動く秘訣）
def load_data():
    query = """
    SELECT * FROM `salesdb-479915.sales_data.adoption_final_view`
    """
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    df = client.query(query).to_dataframe()
    return df

st.title("💊 医薬品卸 売上検索システム")

# 読み込み中表示
with st.spinner('最新データを取得中...'):
    try:
        df = load_data()
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        st.stop()

# --- サイドバー（絞り込み） ---
st.sidebar.header("検索条件")

# 1. 担当者
rep_list = ["全員"] + list(df['sales_rep'].unique())
rep = st.sidebar.selectbox("担当者", rep_list, index=0)

# 2. 得意先（病院・薬局）
customer_list = ["全て"] + list(df[df['sales_rep'] == rep]['customer_name'].unique()) if rep != "全員" else ["全て"] + list(df['customer_name'].unique())
customer = st.sidebar.selectbox("得意先名", customer_list)

# 3. 商品名（あいまい検索）
product_name = st.sidebar.text_input("商品名（一部でOK）")

# --- データのフィルタリング ---
filtered_df = df.copy()

if rep != "全員":
    filtered_df = filtered_df[filtered_df['sales_rep'] == rep]

if customer != "全て":
    filtered_df = filtered_df[filtered_df['customer_name'] == customer]

if product_name:
    filtered_df = filtered_df[filtered_df['product_name'].str.contains(product_name)]

# --- 結果表示 ---
# KPI表示
total_qty = filtered_df['quantity'].sum()
col1, col2 = st.columns(2)
col1.metric("該当件数", f"{len(filtered_df)} 件")
col2.metric("総数量", f"{total_qty:,}")

# メインの表（卸向けに「包装」と「JAN」を前に配置）
st.dataframe(
    filtered_df[['sales_rep', 'customer_name', 'product_name', 'packaging_unit', 'quantity', 'target_month', 'jan_code']],
    use_container_width=True,
    hide_index=True
)
