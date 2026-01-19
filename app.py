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
# Secrets -> Credentials
#   - 方式A: [gcp_service_account].json_key = """{...}"""
#   - 方式B: [gcp_service_account] に各キーを分割して持つ（推奨）
# -----------------------------
def _load_service_account_info() -> dict:
    sa = st.secrets.get("gcp_service_account", None)
    if sa is None:
        raise RuntimeError("Streamlit Secrets に [gcp_service_account] が見つかりません。")

    # 方式A: json_key がある
    if "json_key" in sa:
        raw = sa["json_key"]
        # raw がすでに dict っぽい場合もケア
        if isinstance(raw, dict):
            info = raw
        else:
            info = json.loads(raw)

        # private_key が \\n になっている場合に復元
        pk = info.get("private_key")
        if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
            info["private_key"] = pk.replace("\\n", "\n")
        return info

    # 方式B: 分割キー
    info = dict(sa)
    pk = info.get("private_key")
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")
    return info


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    info = _load_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(
        project=info.get("project_id", PROJECT_ID),
        credentials=credentials,
        location=BQ_LOCATION,
    )


def bq_query_df(sql: str) -> pd.DataFrame:
    """BigQuery実行は必ずここを通す（Client生成経路を固定）"""
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
        st.error("BigQuery接続で例外が発生しました。")
        st.exception(e)
        st.stop()

if ok:
    st.success("BigQuery 接続OK")
else:
    st.error("BigQuery 接続NG")
    st.stop()

df = load_today()
st.dataframe(df, width="stretch")
