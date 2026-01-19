# app.py  (DEBUG版：エラー特定用)
import os
import json
import traceback
import platform
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account


# =============================
# Settings
# =============================
PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"

st.set_page_config(page_title="SFA Debug", layout="wide")


# =============================
# Utils
# =============================
def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def mask_sa_key(key_dict: dict) -> dict:
    """サービスアカウント鍵を画面表示するためにマスクする（private_keyは絶対表示しない）"""
    if not isinstance(key_dict, dict):
        return {"_error": "key_dict is not dict"}
    out = dict(key_dict)

    # 完全に消す or マスク
    if "private_key" in out:
        out["private_key"] = "****MASKED****"
    if "private_key_id" in out and isinstance(out["private_key_id"], str):
        out["private_key_id"] = out["private_key_id"][:6] + "..." + out["private_key_id"][-6:]
    if "client_email" in out and isinstance(out["client_email"], str):
        # メールは表示してOK（でも念のため少しマスク）
        parts = out["client_email"].split("@")
        if len(parts) == 2:
            out["client_email"] = (parts[0][:3] + "***@" + parts[1])
    return out


def safe_show_exception(e: Exception, label: str = "Exception"):
    st.error(f"{label}: {type(e).__name__}")
    st.code(str(e))
    st.code(traceback.format_exc())


# =============================
# BigQuery client
# =============================
@st.cache_resource
def get_bq_client():
    """
    ここが唯一のClient生成経路
    Streamlit Cloudでmetadata.google.internal を叩かないように
    明示的に service account credentials を使う
    """
    # secrets読み取り
    raw = st.secrets["gcp_service_account"]["json_key"]
    key_dict = json.loads(raw)

    credentials = service_account.Credentials.from_service_account_info(key_dict)

    client = bigquery.Client(
        project=key_dict.get("project_id", PROJECT_ID),
        credentials=credentials,
        location=BQ_LOCATION,
    )
    return client


def bq_query_df(sql: str, *, dry_run: bool = False, max_bytes_billed: int | None = None) -> pd.DataFrame:
    """
    BigQuery実行は必ずここを通す。
    dry_run=True なら「SQLが通るか/参照テーブルがあるか/型が合うか」を実行せずに検査できる。
    """
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    if dry_run:
        job_config.dry_run = True
        job_config.use_query_cache = False

    if max_bytes_billed is not None:
        job_config.maximum_bytes_billed = max_bytes_billed

    job = client.query(sql, job_config=job_config)

    if dry_run:
        # dry-runは結果を返せないので、空DFで返す
        return pd.DataFrame([{
            "dry_run": True,
            "total_bytes_processed": getattr(job, "total_bytes_processed", None),
            "total_bytes_billed": getattr(job, "total_bytes_billed", None),
        }])

    return job.to_dataframe(create_bqstorage_client=False)


# =============================
# Debug Panel
# =============================
def debug_panel():
    with st.expander("🔧 Debug Panel（ここに原因が全部出ます）", expanded=True):
        st.write("**Time**:", now_utc_str())
        st.write("**Python**:", platform.python_version())
        st.write("**Platform**:", platform.platform())

        st.write("**Env (抜粋)**")
        env_keys = [
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_CLOUD_PROJECT",
            "GCLOUD_PROJECT",
        ]
        env_view = {k: os.environ.get(k) for k in env_keys}
        st.json(env_view)

        # secrets確認（マスク）
        try:
            raw = st.secrets["gcp_service_account"]["json_key"]
            key_dict = json.loads(raw)
            st.write("**Service Account Key（マスク済）**")
            st.json(mask_sa_key(key_dict))
        except Exception as e:
            safe_show_exception(e, "Secrets parse error")

        # クライアント生成確認
        st.write("**BigQuery Client 생성テスト**")
        try:
            client = get_bq_client()
            st.success("get_bq_client() OK")
            st.write("client.project =", getattr(client, "project", None))
            st.write("client.location =", getattr(client, "location", None))
            # credentialsの型だけ表示（中身は出さない）
            creds = getattr(client, "_credentials", None)
            st.write("credentials type =", type(creds).__name__ if creds else None)
        except Exception as e:
            safe_show_exception(e, "get_bq_client() failed")
            st.stop()

        # ヘルスチェック（SELECT 1）
        st.write("**Query Test: SELECT 1**")
        try:
            df = bq_query_df("SELECT 1 AS ok")
            st.success("SELECT 1 OK")
            st.dataframe(df, width="stretch")
        except Exception as e:
            safe_show_exception(e, "SELECT 1 failed")
            st.stop()

        # ここから「落ちてるSQL」を入れてテストできる
        st.write("**SQL Dry-run Checker（実行せず検査）**")
        sample_sql = """\
SELECT
  *
FROM `salesdb-479915.sales_data.adoption_unpivoted`
LIMIT 5
"""
        sql = st.text_area("ここに実行したいSQLを貼ってください（dry-runで先に検査できます）", value=sample_sql, height=160)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Dry-run（実行せず検査）"):
                try:
                    out = bq_query_df(sql, dry_run=True)
                    st.success("Dry-run OK（SQLとして成立しています）")
                    st.dataframe(out, width="stretch")
                except Exception as e:
                    safe_show_exception(e, "Dry-run failed（ここに原因が出ます）")

        with col2:
            if st.button("▶ 実行（LIMIT付けてね）"):
                try:
                    out = bq_query_df(sql)
                    st.success("Query OK")
                    st.dataframe(out, width="stretch")
                except Exception as e:
                    safe_show_exception(e, "Query failed（ここに原因が出ます）")


# =============================
# Main
# =============================
st.title("SFA Debug（エラー特定モード）")

st.info(
    "この画面は「どこで落ちてるか」を確実に特定するためのデバッグ版です。\n"
    "- get_bq_client() が落ちる → secrets/認証/TransportError系\n"
    "- SELECT 1 が落ちる → 認証/ネットワーク/権限\n"
    "- Dry-run が落ちる → SQL/列名/テーブル場所/型不一致\n"
)

debug_panel()

st.divider()
st.write("✅ ここまで全部OKなら、アプリ本体のSQL/結合/列名の問題に絞れます。")
