import json
from datetime import datetime
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

st.set_page_config(page_title="SFA", layout="wide")

# -----------------------------
# BigQuery client (ONLY ONE WAY)
# -----------------------------
import json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

@st.cache_resource
def get_bq_client():
    # Streamlit secrets からサービスアカウント鍵を読む
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)

    return bigquery.Client(
        project=key_dict.get("project_id", PROJECT_ID),
        credentials=credentials,
        location=BQ_LOCATION,
    )



def bq_query_df(sql: str) -> pd.DataFrame:
    """BigQuery実行は必ずここを通す（TransportError封じ）"""
    client = get_bq_client()
    job = client.query(sql)
    return job.to_dataframe(create_bqstorage_client=False)

# -----------------------------
# Health check
# -----------------------------
@st.cache_data(ttl=600)
def health_check() -> bool:
    df = bq_query_df("SELECT 1 AS ok")
    return int(df.iloc[0]["ok"]) == 1

@st.cache_data(ttl=3600)
def load_today() -> pd.DataFrame:
    return bq_query_df('SELECT CURRENT_DATE("Asia/Tokyo") AS today')

# -----------------------------
# UI
# -----------------------------
st.title("SFA")

with st.spinner("BigQuery 接続確認中..."):
    try:
        ok = health_check()
    except Exception as e:
        st.error("BigQuery接続で例外が発生しました（右ログの TransportError は“別経路でClient生成”が原因です）。")
        st.exception(e)
        st.stop()

if ok:
    st.success("BigQuery 接続OK")
else:
    st.error("BigQuery 接続NG")
    st.stop()

df = load_today()
st.dataframe(df, width="stretch")
