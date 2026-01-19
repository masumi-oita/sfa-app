import json
import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

st.set_page_config(page_title="SFA Debug", layout="wide")

PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

st.title("SFA Debug（エラー特定モード）")

# -----------------------------
# 1. 環境変数チェック
# -----------------------------
st.subheader("① 環境変数チェック")

env_keys = [
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
]

env_status = {k: os.getenv(k) for k in env_keys}
st.json(env_status)

# -----------------------------
# 2. secrets 読み取りチェック
# -----------------------------
st.subheader("② Streamlit secrets 読み取り")

try:
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    st.success("secrets 読み取り OK")
    st.json({k: ("***MASKED***" if "key" in k else v) for k, v in key_dict.items()})
except Exception as e:
    st.error("secrets 読み取り失敗")
    st.exception(e)
    st.stop()

# -----------------------------
# 3. BigQuery Client 生成
# -----------------------------
st.subheader("③ BigQuery Client 生成")

try:
    credentials = service_account.Credentials.from_service_account_info(key_dict)

    client = bigquery.Client(
        project=key_dict.get("project_id", PROJECT_ID),
        credentials=credentials,
        location=BQ_LOCATION,
    )

    st.success("BigQuery Client 生成 OK")
    st.write("client.project =", client.project)
    st.write("client.location =", client.location)
    st.write("credentials =", type(credentials).__name__)

except Exception as e:
    st.error("BigQuery Client 生成失敗")
    st.exception(e)
    st.stop()

# -----------------------------
# 4. SELECT 1 テスト
# -----------------------------
st.subheader("④ SELECT 1 テスト")

try:
    df = client.query("SELECT 1 AS ok").to_dataframe(
        create_bqstorage_client=False
    )
    st.success("SELECT 1 成功")
    st.dataframe(df)

except Exception as e:
    st.error("SELECT 1 失敗")
    st.exception(e)
    st.stop()

# -----------------------------
# 5. CURRENT_DATE テスト
# -----------------------------
st.subheader("⑤ CURRENT_DATE テスト")

try:
    df = client.query(
        'SELECT CURRENT_DATE("Asia/Tokyo") AS today'
    ).to_dataframe(create_bqstorage_client=False)

    st.success("日付取得 成功")
    st.dataframe(df)

except Exception as e:
    st.error("日付取得 失敗")
    st.exception(e)
    st.stop()

st.success("🎉 ここまで全て通過 → BigQuery/認証/ネットワークは完全に正常です")
