# app.py  （診断用フルコード）
# Streamlit Cloud の Secrets（gcp_service_account）を使って BigQuery 疎通を確認するための診断アプリです。
# 目的：どこで落ちているか（Secrets / 認証 / Client生成 / Query / to_dataframe）を1発で切り分ける
#
# ✅ 注意：
# - 秘密鍵などは表示しません（キー名のみ表示）
# - 診断が終わったらこのファイルを本番コードに差し替えてください

import streamlit as st
import pandas as pd

from google.oauth2 import service_account
from google.cloud import bigquery

PROJECT_ID = "salesdb-479915"

st.set_page_config(page_title="BQ診断", layout="wide")
st.title("BigQuery 接続 診断（Streamlit Secrets / Service Account）")

# -----------------------------
# STEP 0: Secrets 読み取り
# -----------------------------
st.subheader("STEP0: Secrets 読み取り確認")
try:
    secret_keys = list(st.secrets.keys())
    st.write("secrets keys:", secret_keys)

    has_sa = "gcp_service_account" in st.secrets
    st.write("has gcp_service_account:", has_sa)

    if not has_sa:
        st.error("Secrets に [gcp_service_account] が見つかりません。Streamlit Cloud の Secrets を確認してください。")
        st.stop()

    sa_keys = list(st.secrets["gcp_service_account"].keys())
    st.write("gcp keys:", sa_keys)

    # 安全な範囲だけ確認表示（値は出さない）
    st.info("Secrets の読み取りはOK（キー一覧のみ表示）。")

except Exception as e:
    st.error("STEP0 FAILED: Secrets 読み取りで例外が発生しました。")
    st.exception(e)
    st.stop()

st.divider()

# -----------------------------
# STEP 1: Credentials 生成
# -----------------------------
st.subheader("STEP1: Credentials 生成")
try:
    # Secretsからサービスアカウント情報を取得
    sa_info = dict(st.secrets["gcp_service_account"])

    # 念のため、数値として解釈されがちな項目を文字列に寄せる（事故防止）
    for k in ["client_id", "private_key_id", "project_id", "client_email"]:
        if k in sa_info and sa_info[k] is not None:
            sa_info[k] = str(sa_info[k])

    creds = service_account.Credentials.from_service_account_info(sa_info)
    st.success("STEP1 OK: Credentials を生成できました。")
    st.write("client_email:", sa_info.get("client_email", "(unknown)"))
    st.write("project_id:", sa_info.get("project_id", "(unknown)"))

except Exception as e:
    st.error("STEP1 FAILED: Credentials 生成で失敗しました。Secrets の形式（特に private_key の改行）を確認してください。")
    st.exception(e)
    st.stop()

st.divider()

# -----------------------------
# STEP 2: BigQuery Client 生成（ADCフォールバック封じ）
# -----------------------------
st.subheader("STEP2: BigQuery Client 生成")
try:
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    st.success("STEP2 OK: bigquery.Client を生成できました。")

except Exception as e:
    st.error("STEP2 FAILED: bigquery.Client の生成で失敗しました。google-cloud-bigquery / google-auth の依存関係やSecretsを確認してください。")
    st.exception(e)
    st.stop()

st.divider()

# -----------------------------
# STEP 3: 最小クエリ（権限/疎通）
# -----------------------------
st.subheader("STEP3: 最小クエリ（SELECT 1）")
try:
    df_min = client.query("#standardSQL\nSELECT 1 AS ok").to_dataframe()
    st.success("STEP3 OK: クエリ実行＆to_dataframe 成功（基礎疎通OK）")
    st.dataframe(df_min, use_container_width=True)

except Exception as e:
    st.error("STEP3 FAILED: クエリ実行または to_dataframe で失敗しました。")
    st.caption("よくある原因：① IAM権限不足(403) ② db-dtypes/pyarrow不足 ③ ネットワーク/TransportError")
    st.exception(e)
    st.stop()

st.divider()

# -----------------------------
# STEP 4: 実データ参照（あなたのVIEWの存在/権限確認）
# -----------------------------
st.subheader("STEP4: 実VIEW参照（存在/権限）")
default_view = f"{PROJECT_ID}.sales_data.jp_new_deliveries_realized_staff_period_summary"
view_name = st.text_input("確認するVIEW（Fully Qualified）", value=default_view)

sql = f"""
#standardSQL
SELECT *
FROM `{view_name}`
LIMIT 5
"""

try:
    df_view = client.query(sql).to_dataframe()
    st.success("STEP4 OK: 実VIEWを読めました。")
    st.dataframe(df_view, use_container_width=True)

except Exception as e:
    st.error("STEP4 FAILED: 実VIEWの参照で失敗しました。")
    st.caption("原因候補：① VIEW名間違い ② dataset location違い ③ BigQuery Data Viewer権限不足")
    st.exception(e)
    st.stop()

st.divider()

# -----------------------------
# STEP 5: 環境チェック（依存関係）
# -----------------------------
st.subheader("STEP5: 依存関係チェック（参考）")
st.write("pandas version:", pd.__version__)
try:
    import pyarrow  # noqa
    st.write("pyarrow: OK")
except Exception as e:
    st.write("pyarrow: NOT FOUND or ERROR")
    st.exception(e)

try:
    import db_dtypes  # noqa
    st.write("db-dtypes: OK")
except Exception as e:
    st.write("db-dtypes: NOT FOUND or ERROR")
    st.exception(e)

st.success("診断完了：ここまで全部OKなら認証・権限・依存は成立しています。")
