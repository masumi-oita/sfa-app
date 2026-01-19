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
def get_bq_client() -> bigquery.Client:
    sa_json_str = st.secrets["gcp_service_account"]["json_key"]
    sa_info = json.loads(sa_json_str)

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(
        project=sa_info.get("project_id", PROJECT_ID),
        credentials=creds,
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
