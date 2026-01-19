import json
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

st.set_page_config(page_title="SFA", layout="wide")

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    # Secretsにjson_keyがある前提
    if "gcp_service_account" not in st.secrets or "json_key" not in st.secrets["gcp_service_account"]:
        st.error("Streamlit Secrets に [gcp_service_account].json_key が見つかりません。")
        st.stop()

    sa_json_str = st.secrets["gcp_service_account"]["json_key"]

    try:
        sa_info = json.loads(sa_json_str)
    except Exception as e:
        st.error("Secrets の json_key が JSON としてパースできません。TOMLの貼り方（改行/ダブルクォート）を確認してください。")
        st.exception(e)
        st.stop()

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(
        project=sa_info.get("project_id", PROJECT_ID),
        credentials=creds,
        location=BQ_LOCATION,
    )

def bq_health_check():
    client = get_bq_client()
    try:
        df = client.query("SELECT 1 AS ok").to_dataframe(create_bqstorage_client=False)
        return int(df.iloc[0]["ok"]) == 1
    except Exception as e:
        st.error("BigQuery 接続に失敗しました（Secrets/権限/プロジェクト/ロケーションを確認）。")
        st.exception(e)
        st.stop()

@st.cache_data(ttl=3600)
def load_any_table_example():
    # まずは接続確認できる簡単な例
    client = get_bq_client()
    q = f"""
    SELECT
      CURRENT_DATE("Asia/Tokyo") AS today
    """
    return client.query(q).to_dataframe(create_bqstorage_client=False)

st.title("SFA")

with st.spinner("BigQuery 接続確認中..."):
    ok = bq_health_check()
st.success("BigQuery 接続OK")

df = load_any_table_example()
st.dataframe(df, use_container_width=True)
