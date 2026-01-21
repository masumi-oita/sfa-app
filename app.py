import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="BQ 接続診断", layout="wide")

PROJECT_ID = "salesdb-479915"

def load_credentials():
    # Streamlit Secrets から service account dict を読む
    sa_info = st.secrets["gcp_service_account"]

    # 置換漏れ検知（超重要）
    email = sa_info.get("client_email", "")
    if not email or "REPLACE" in email:
        st.error(f"client_email が不正です: {email!r}（Secretsを実値にしてください）")
        st.stop()

    # private_key に改行が入っていることを軽くチェック
    pk = sa_info.get("private_key", "")
    if "BEGIN PRIVATE KEY" not in pk:
        st.error("private_key が不正です（BEGIN PRIVATE KEY が見当たりません）")
        st.stop()

    creds = service_account.Credentials.from_service_account_info(sa_info)
    return creds

@st.cache_data(ttl=300)
def run_query_min(creds):
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    job = client.query("SELECT 1 AS ok")
    rows = list(job.result())
    df = pd.DataFrame([dict(r) for r in rows])
    return df

st.title("BigQuery 接続 診断（Streamlit Secrets / Service Account）")

# STEP0
st.subheader("STEP0: Secrets 読み取り確認")
keys = list(st.secrets.keys())
st.write("secrets keys:", keys)
st.write("has gcp_service_account:", "gcp_service_account" in st.secrets)

# STEP1
st.subheader("STEP1: Credentials 生成")
try:
    creds = load_credentials()
    st.success("STEP1 OK: Credentials を生成できました。")
    st.write("client_email:", st.secrets["gcp_service_account"].get("client_email"))
    st.write("project_id:", st.secrets["gcp_service_account"].get("project_id"))
except Exception as e:
    st.error("STEP1 FAILED")
    st.exception(e)
    st.stop()

# STEP2 + STEP3
st.subheader("STEP2/3: BigQuery Client & 最小クエリ（SELECT 1）")
try:
    df = run_query_min(creds)
    st.success("STEP3 OK: クエリ成功（SELECT 1）")
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.error("STEP3 FAILED")
    st.exception(e)
    st.stop()
