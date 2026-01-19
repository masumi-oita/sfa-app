import json
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest

PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

st.set_page_config(page_title="SFA Debug", layout="wide")
st.title("SFA Debug（SQLエラー特定用）")

# -----------------------------
# BigQuery Client
# -----------------------------
@st.cache_resource
def get_bq_client():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    creds = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(
        project=PROJECT_ID,
        credentials=creds,
        location=BQ_LOCATION,
    )

client = get_bq_client()

# -----------------------------
# SQL 入力
# -----------------------------
st.subheader("① 実行したいSQLを貼る")
sql = st.text_area(
    "SQL",
    height=200,
    placeholder="SELECT * FROM `salesdb-479915.sales_data.adoption_unpivoted` LIMIT 10"
)

# -----------------------------
# Dry-run
# -----------------------------
st.subheader("② Dry-run（実行せず検証）")
if st.button("Dry-run 実行"):
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=job_config)
        st.success("✅ Dry-run OK（SQL構文・列・型は成立）")
        st.write({
            "bytes_processed": job.total_bytes_processed,
            "bytes_billed": job.total_bytes_billed,
        })
    except BadRequest as e:
        st.error("❌ Dry-run 失敗（ここが原因）")
        st.code(e.message)
        st.stop()

# -----------------------------
# 実行（LIMIT必須）
# -----------------------------
st.subheader("③ 実行（LIMIT必須）")
if st.button("実行"):
    try:
        df = client.query(sql).to_dataframe(create_bqstorage_client=False)
        st.success("✅ 実行成功")
        st.dataframe(df)
    except BadRequest as e:
        st.error("❌ 実行時エラー")
        st.code(e.message)
