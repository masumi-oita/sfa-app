# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.4.6
- 起動時に重いクエリを走らせない（遅延ロード）
- timeout / BigQuery Storage API ON-OFF / SELECT1 ヘルスチェック
- BadRequest の詳細（job.errors, SQL, params）を必ず画面に表示
- Secrets 未設定時はクラッシュせず、設定手順とテンプレを表示して停止

【Streamlit Cloud Secrets（推奨）】
App -> Manage app -> Settings -> Secrets に secrets.toml を貼り付け

【必要な secrets.toml（テンプレ）】
[bigquery]
project_id = "salesdb-479915"
location = "asia-northeast1"

[bigquery.service_account]
type = "service_account"
project_id = "salesdb-479915"
private_key_id = "YOUR_PRIVATE_KEY_ID"
private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
client_email = "YOUR_SA@YOUR_PROJECT.iam.gserviceaccount.com"
client_id = "YOUR_CLIENT_ID"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/YOUR_SA%40YOUR_PROJECT.iam.gserviceaccount.com"
universe_domain = "googleapis.com"

default_login_email = "masumi@example.com"  # 任意
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, GoogleAPICallError


# =============================================================================
# 基本設定
# =============================================================================

APP_TITLE = "SFA｜入口高速版（判断専用）"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300


# =============================================================================
# SQL（あなたの実体に合わせて VIEW 名などを調整）
# =============================================================================

# FYTD（組織）
SQL_FYTD_ORG_SUMMARY = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_admin_org_fytd_summary_scoped`
WHERE login_email = @login_email
LIMIT 2000
"""

# 当月YoY（上/下/比較不能）
SQL_YOY_TOP = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_top_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

SQL_YOY_BOTTOM = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_bottom_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

SQL_YOY_UNCOMPARABLE = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_uncomparable_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

# 新規納品（入口4本固定）
# あなたの環境が period_key を持っていない場合は、別VIEW/SQLに置き換えてOK
SQL_NEW_DELIVERIES_YESTERDAY = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_new_deliveries_realized_daily_fact_all_months`
WHERE login_email = @login_email
  AND period_key = "yesterday"
LIMIT 2000
"""

SQL_NEW_DELIVERIES_LAST7D = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_new_deliveries_realized_daily_fact_all_months`
WHERE login_email = @login_email
  AND period_key = "last_7_days"
LIMIT 2000
"""

SQL_NEW_DELIVERIES_THIS_MONTH = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_new_deliveries_realized_daily_fact_all_months`
WHERE login_email = @login_email
  AND period_key = "this_month"
LIMIT 2000
"""

SQL_NEW_DELIVERIES_FYTD = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_new_deliveries_realized_daily_fact_all_months`
WHERE login_email = @login_email
  AND period_key = "fytd"
LIMIT 2000
"""

# ロール（権限）
SQL_ROLE_LOOKUP = """
SELECT
  login_email,
  role_key,
  role_admin_view,
  role_admin_edit,
  role_sales_view,
  area_key
FROM `salesdb-479915.sales_data.v_dim_staff_role_dedup`
WHERE login_email = @login_email
LIMIT 1
"""

# 表示名（メール→氏名）
SQL_STAFF_NAME = """
SELECT
  login_email,
  display_name
FROM `salesdb-479915.sales_data.v_staff_email_name`
WHERE login_email = @login_email
LIMIT 1
"""


# =============================================================================
# データ構造
# =============================================================================

@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    role_key: str = "SALES"
    role_admin_view: bool = False
    role_admin_edit: bool = False
    role_sales_view: bool = True
    area_key: str = ""


# =============================================================================
# Secrets ユーティリティ（★未設定でも落ちずにガイド表示）
# =============================================================================

def _secrets_ready() -> bool:
    if "bigquery" not in st.secrets:
        return False
    bq = st.secrets.get("bigquery", {})
    if not bq.get("project_id"):
        return False
    if not bq.get("service_account"):
        return False
    return True


def render_secrets_setup_guide_and_stop():
    st.error("BigQuery Secrets が未設定です（st.secrets に [bigquery] がありません / 必須項目が不足）。")
    st.write("Streamlit Cloud を使う場合：右下 **Manage app** → **Settings** → **Secrets** に以下を貼り付けて Reboot してください。")
    st.write("ローカルの場合：リポジトリ直下に `.streamlit/secrets.toml` を作成し、同じ内容を入れてください。")

    template = """[bigquery]
project_id = "salesdb-479915"
location = "asia-northeast1"

[bigquery.service_account]
type = "service_account"
project_id = "salesdb-479915"
private_key_id = "YOUR_PRIVATE_KEY_ID"
private_key = "-----BEGIN PRIVATE KEY-----\\nYOUR_KEY_BODY\\n-----END PRIVATE KEY-----\\n"
client_email = "YOUR_SA@YOUR_PROJECT.iam.gserviceaccount.com"
client_id = "YOUR_CLIENT_ID"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/YOUR_SA%40YOUR_PROJECT.iam.gserviceaccount.com"
universe_domain = "googleapis.com"

# 任意：ログイン入力の初期値
default_login_email = "masumi@example.com"
"""
    st.code(template, language="toml")
    st.warning("注意：private_key は複数行ではなく、上のように \\n を含む1行文字列にしてください（TOML崩れ防止）。")
    st.stop()


def _get_secrets() -> Tuple[str, str, Dict[str, Any]]:
    """
    必要な secrets がある前提で呼ばれる関数。
    """
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    return project_id, location, sa


# =============================================================================
# BigQuery Client
# =============================================================================

@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    project_id, location, sa = _get_secrets()
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)


# =============================================================================
# クエリ実行（キャッシュ安全 + エラー可視化）
# =============================================================================

def _build_query_parameters(params: Optional[Dict[str, Any]]) -> list[bigquery.ScalarQueryParameter]:
    qparams: list[bigquery.ScalarQueryParameter] = []
    if not params:
        return qparams

    for k, v in params.items():
        if isinstance(v, bool):
            qparams.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
        elif isinstance(v, int):
            qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        elif isinstance(v, float):
            qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
        elif v is None:
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", ""))
        else:
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return qparams


def _show_bq_error_context(
    title: str,
    sql: str,
    params: Optional[Dict[str, Any]],
    job: Optional[bigquery.job.QueryJob],
    exc: Exception,
) -> None:
    st.error(f"BigQuery クエリ失敗：{title}")
    st.write("**発生時刻**:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    st.write("**params**:")
    st.code(json.dumps(params or {}, ensure_ascii=False, indent=2), language="json")

    if job is not None:
        st.write("**job_id**:", getattr(job, "job_id", None))
        st.write("**location**:", getattr(job, "location", None))
        if getattr(job, "errors", None):
            st.write("**job.errors（最重要）**:")
            st.json(job.errors)

    st.write("**sql**:")
    st.code(sql, language="sql")
    st.write("**exception**:", str(exc))


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SEC)
def cached_query_df(
    sql: str,
    params_json: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    """
    cache_data 内では session_state を触らない（OS v1.4.6）
    params は hash 安定化のため JSON文字列で受け取る
    """
    client = get_bq_client()
    params = json.loads(params_json) if params_json else {}

    job_config = bigquery.QueryJobConfig()
    qparams = _build_query_parameters(params)
    if qparams:
        job_config.query_parameters = qparams

    job = client.query(sql, job_config=job_config)
    job.result(timeout=timeout_sec)
    return job.to_dataframe(create_bqstorage_client=use_bqstorage)


def query_df(
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    use_bqstorage: bool = True,
    timeout_sec: int = 60,
) -> pd.DataFrame:
    """
    UI層でエラーを確実に表示するラッパー
    """
    params_json = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)

    try:
        return cached_query_df(sql, params_json=params_json, use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)

    except BadRequest as e:
        # job.errors を取るため、ノーキャッシュで一度だけ再実行して job 情報を表示
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig()
        qparams = _build_query_parameters(params or {})
        if qparams:
            job_config.query_parameters = qparams

        job = None
        try:
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
        except Exception as e2:
            _show_bq_error_context(label or "query_df", sql, params, job, e2)
            raise
        _show_bq_error_context(label or "query_df", sql, params, job, e)
        raise

    except GoogleAPICallError as e:
        _show_bq_error_context(label or "query_df", sql, params, None, e)
        raise

    except Exception as e:
        _show_bq_error_context(label or "query_df", sql, params, None, e)
        raise


# =============================================================================
# UI：共通
# =============================================================================

def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.6｜遅延ロード｜timeout/Storage API 切替｜BadRequest詳細表示｜Secrets未設定ガイド")


def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.header("設定（切り分け）")
    use_bqstorage = st.sidebar.toggle("BigQuery Storage API を使う（高速）", value=True)
    timeout_sec = st.sidebar.slider("クエリタイムアウト（秒）", min_value=10, max_value=300, value=60, step=10)
    show_sql = st.sidebar.toggle("SQL を表示する（デバッグ）", value=False)
    if st.sidebar.button("キャッシュをクリア（cache_data）"):
        st.cache_data.clear()
        st.sidebar.success("cache_data をクリアしました")
    return {"use_bqstorage": use_bqstorage, "timeout_sec": timeout_sec, "show_sql": show_sql}


def get_login_email_ui() -> str:
    st.sidebar.header("ログイン（暫定）")
    default_email = st.secrets.get("default_login_email", "") if _secrets_ready() else ""
    login_email = st.sidebar.text_input("login_email（メール）", value=default_email, placeholder="例: masumi@example.com")
    login_email = (login_email or "").strip()
    if not login_email:
        st.info("左サイドバーで login_email を入力してください（暫定ログイン）。")
        st.stop()
    return login_email


def render_health_check(use_bqstorage: bool, timeout_sec: int):
    st.subheader("ヘルスチェック")
    st.write("最初にここを通すことで「Secrets」「権限」「接続」「Storage API」の切り分けができます。")

    cols = st.columns([1, 3])
    with cols[0]:
        if st.button("SELECT 1（接続チェック）", use_container_width=True):
            df = query_df("SELECT 1 AS ok", params=None, label="SELECT 1 ヘルスチェック",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.success("OK（BigQuery 接続成功）")
            st.dataframe(df, use_container_width=True)

    with cols[1]:
        st.write("- まず SELECT 1 が通るか")
        st.write("- 次に Storage API ON/OFF で挙動が変わるか")
        st.write("- timeout を短くして「どのクエリが重いか」特定する")


# =============================================================================
# ロール解決
# =============================================================================

def resolve_role(login_email: str, use_bqstorage: bool, timeout_sec: int) -> RoleInfo:
    df_role = query_df(SQL_ROLE_LOOKUP, params={"login_email": login_email}, label="ロール取得",
                       use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
    if df_role.empty:
        return RoleInfo(login_email=login_email, role_key="SALES", role_sales_view=True)

    r = df_role.iloc[0].to_dict()
    return RoleInfo(
        login_email=str(r.get("login_email", login_email)),
        role_key=str(r.get("role_key", "SALES")),
        role_admin_view=bool(r.get("role_admin_view", False)),
        role_admin_edit=bool(r.get("role_admin_edit", False)),
        role_sales_view=bool(r.get("role_sales_view", True)),
        area_key=str(r.get("area_key", "")) if r.get("area_key") is not None else "",
    )


def resolve_display_name(login_email: str, use_bqstorage: bool, timeout_sec: int) -> str:
    df = query_df(SQL_STAFF_NAME, params={"login_email": login_email}, label="氏名表示取得",
                  use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
    if df.empty:
        return login_email
    v = df.iloc[0].to_dict()
    return str(v.get("display_name") or login_email)


# =============================================================================
# 入口：FYTD（組織）
# =============================================================================

def render_fytd_org_summary(role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("年度累計（FYTD）｜組織サマリー")
    st.write("今年度累計 vs 昨年度累計（差額も含む）")
    if show_sql:
        st.code(SQL_FYTD_ORG_SUMMARY, language="sql")

    if st.button("年度累計（組織）を読み込む", use_container_width=True, type="primary"):
        df = query_df(SQL_FYTD_ORG_SUMMARY, params={"login_email": role.login_email},
                      label="年度累計（組織）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
        st.dataframe(df, use_container_width=True)


# =============================================================================
# 入口：当月YoY
# =============================================================================

def render_yoy_rankings(role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("当月（前年同月比）ランキング")
    st.write("Top / Bottom / 比較不能（新規・前年なし）を分離して表示（事故防止）")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("#### 上がっている先（Top）")
        if show_sql:
            st.code(SQL_YOY_TOP, language="sql")
        if st.button("Top を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_TOP, params={"login_email": role.login_email},
                          label="当月YoY Top", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with c2:
        st.markdown("#### 下がっている先（Bottom）")
        if show_sql:
            st.code(SQL_YOY_BOTTOM, language="sql")
        if st.button("Bottom を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_BOTTOM, params={"login_email": role.login_email},
                          label="当月YoY Bottom", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with c3:
        st.markdown("#### 比較不能（Uncomparable）")
        if show_sql:
            st.code(SQL_YOY_UNCOMPARABLE, language="sql")
        if st.button("Uncomparable を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_UNCOMPARABLE, params={"login_email": role.login_email},
                          label="当月YoY Uncomparable", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)


# =============================================================================
# 入口：新規納品（昨日/7日/当月/FYTD）
# =============================================================================

def render_new_deliveries(role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("新規納品サマリー（入口固定：昨日 / 直近7日 / 当月 / FYTD）")

    tabs = st.tabs(["昨日", "直近7日", "当月", "FYTD"])

    with tabs[0]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_YESTERDAY, language="sql")
        if st.button("昨日分を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_YESTERDAY, params={"login_email": role.login_email},
                          label="新規納品（昨日）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[1]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_LAST7D, language="sql")
        if st.button("直近7日を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_LAST7D, params={"login_email": role.login_email},
                          label="新規納品（直近7日）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[2]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_THIS_MONTH, language="sql")
        if st.button("当月を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_THIS_MONTH, params={"login_email": role.login_email},
                          label="新規納品（当月）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[3]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_FYTD, language="sql")
        if st.button("FYTD を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_FYTD, params={"login_email": role.login_email},
                          label="新規納品（FYTD）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)


# =============================================================================
# Main
# =============================================================================

def main():
    set_page()

    # ★ Secrets 未設定なら、ここでガイドを出して停止（クラッシュさせない）
    if not _secrets_ready():
        render_secrets_setup_guide_and_stop()

    opts = sidebar_controls()
    login_email = get_login_email_ui()

    # ヘルスチェック
    render_health_check(use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
    st.divider()

    # ロール & 表示名（ここは軽い想定。重ければボタン化してもOK）
    st.subheader("ログイン情報")
    with st.spinner("ロール・氏名を取得中..."):
        role = resolve_role(login_email, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
        display_name = resolve_display_name(login_email, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])

    st.write(f"**ログイン:** {display_name}")
    st.write(f"**メール:** {role.login_email}")
    st.write(f"**ロール:** {role.role_key}（admin_view={role.role_admin_view}, admin_edit={role.role_admin_edit}, sales_view={role.role_sales_view}）")
    if role.area_key:
        st.write(f"**エリア:** {role.area_key}")

    st.divider()

    # 入口順序（OS v1.4.6）
    st.header("管理者入口（判断専用・高速）")
    render_fytd_org_summary(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    render_yoy_rankings(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    render_new_deliveries(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])

    st.divider()
    st.caption("BadRequest が出たら job.errors / SQL / params を画面に出します（Streamlitの赤塗り回避）。")


if __name__ == "__main__":
    main()
