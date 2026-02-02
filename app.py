# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.4.6

✅ この版でできること
- 起動時に重いクエリを走らせない（遅延ロード）
- timeout / BigQuery Storage API ON-OFF / SELECT1 ヘルスチェック
- BadRequest の詳細（job.errors, SQL, params）を必ず画面に表示
- st.secrets が未設定でも、画面で「サービスアカウントJSON貼り付け」で一時起動可能（セッション限定）

推奨（本番）:
- Streamlit Cloud -> Manage app -> Settings -> Secrets に secrets.toml を設定
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

SQL_FYTD_ORG_SUMMARY = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_admin_org_fytd_summary_scoped`
WHERE login_email = @login_email
LIMIT 2000
"""

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
# period_key が無い場合は、あなたのビュー仕様に合わせてこのSQLを差し替えてください
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
# Secrets / Credentials
# =============================================================================

def _secrets_has_bigquery() -> bool:
    if "bigquery" not in st.secrets:
        return False
    bq = st.secrets.get("bigquery", {})
    return bool(bq.get("project_id")) and bool(bq.get("service_account"))


def _get_bq_from_secrets() -> Tuple[str, str, Dict[str, Any]]:
    """
    secrets.toml:
    [bigquery]
    project_id = "..."
    location = "asia-northeast1"
    [bigquery.service_account]
    ... service account fields ...
    """
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    return project_id, location, sa


def _parse_service_account_json(text: str) -> Dict[str, Any]:
    """
    画面貼り付け用。JSON文字列を dict にするだけ。
    """
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("JSONの形式が不正です（dictではありません）")
    # 最低限チェック
    for k in ["type", "project_id", "private_key", "client_email"]:
        if k not in obj:
            raise ValueError(f"サービスアカウントJSONに {k} がありません")
    return obj


def ensure_credentials_ui() -> Tuple[str, str, Dict[str, Any]]:
    """
    優先順位:
    1) st.secrets[bigquery] があればそれを使う（本番）
    2) 無ければ、画面でサービスアカウントJSONを貼り付けて一時利用（セッション限定）
    """
    st.sidebar.header("接続設定")

    if _secrets_has_bigquery():
        project_id, location, sa = _get_bq_from_secrets()
        st.sidebar.success("Secrets: OK（st.secrets から BigQuery 設定を読み込み）")
        return project_id, location, sa

    st.sidebar.warning("Secrets が未設定です。下で『サービスアカウントJSON貼り付け』で暫定接続できます。")

    with st.expander("（推奨）Streamlit Cloud Secrets 設定テンプレ", expanded=False):
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

default_login_email = "masumi@example.com"
"""
        st.code(template, language="toml")
        st.caption("private_key は複数行ではなく \\n を含む1行文字列にしてください。")

    st.markdown("### サービスアカウントJSON貼り付け（暫定・セッション限定）")
    st.caption("※ Secrets に入れるのが本番推奨。ここは“いま動かして原因切り分け”用です。")

    default_project = "salesdb-479915"
    project_id = st.sidebar.text_input("project_id（暫定）", value=default_project)
    location = st.sidebar.text_input("location（暫定）", value=DEFAULT_LOCATION)

    sa_text = st.sidebar.text_area(
        "サービスアカウントJSON（貼り付け）",
        value="",
        height=200,
        placeholder='{"type":"service_account", ... } を丸ごと貼り付け',
    )

    if not sa_text.strip():
        st.info("左サイドバーにサービスアカウントJSONを貼り付けると接続できるようになります。")
        st.stop()

    try:
        sa = _parse_service_account_json(sa_text.strip())
    except Exception as e:
        st.error("サービスアカウントJSONの読み取りに失敗しました。")
        st.write(str(e))
        st.stop()

    # project_id は JSON にもあるが、上で入力した方を優先（誤差吸収）
    sa["project_id"] = project_id.strip() or sa.get("project_id")
    st.sidebar.success("貼り付けJSON: OK（このセッション中のみ有効）")
    return str(project_id), str(location), sa


@st.cache_resource(show_spinner=False)
def get_bq_client(project_id: str, location: str, sa: Dict[str, Any]) -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)


# =============================================================================
# BigQuery query helper（キャッシュ安全 + エラー可視化）
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
    project_id: str,
    location: str,
    sa_json: str,
    sql: str,
    params_json: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    """
    cache_data 内で session_state を触らない（OS v1.4.6）
    sa は hash 安定化のため JSON 文字列で受ける
    """
    sa = json.loads(sa_json)
    client = get_bq_client(project_id, location, sa)

    params = json.loads(params_json) if params_json else {}

    job_config = bigquery.QueryJobConfig()
    qparams = _build_query_parameters(params)
    if qparams:
        job_config.query_parameters = qparams

    job = client.query(sql, job_config=job_config)
    job.result(timeout=timeout_sec)
    return job.to_dataframe(create_bqstorage_client=use_bqstorage)


def query_df(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    use_bqstorage: bool = True,
    timeout_sec: int = 60,
    cache_key: Optional[Tuple[str, str, str]] = None,
) -> pd.DataFrame:
    """
    UI層でエラーを確実に表示するラッパー
    cache_key = (project_id, location, sa_json)
    """
    params_json = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)

    try:
        if cache_key is None:
            # キャッシュを使わず直実行（例: 緊急切り分け）
            job_config = bigquery.QueryJobConfig()
            qparams = _build_query_parameters(params or {})
            if qparams:
                job_config.query_parameters = qparams
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
            return job.to_dataframe(create_bqstorage_client=use_bqstorage)

        project_id, location, sa_json = cache_key
        return cached_query_df(
            project_id=project_id,
            location=location,
            sa_json=sa_json,
            sql=sql,
            params_json=params_json,
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
        )

    except BadRequest as e:
        # job.errors を取るため、ノーキャッシュで再実行して job を表示
        job = None
        try:
            job_config = bigquery.QueryJobConfig()
            qparams = _build_query_parameters(params or {})
            if qparams:
                job_config.query_parameters = qparams
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
# UI
# =============================================================================

def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.6｜遅延ロード｜timeout/Storage API 切替｜BadRequest詳細表示｜Secrets/貼り付け起動対応")


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
    default_email = ""
    if "default_login_email" in st.secrets:
        default_email = st.secrets.get("default_login_email", "")
    login_email = st.sidebar.text_input("login_email（メール）", value=default_email, placeholder="例: masumi@example.com")
    login_email = (login_email or "").strip()
    if not login_email:
        st.info("左サイドバーで login_email を入力してください（暫定ログイン）。")
        st.stop()
    return login_email


def render_health_check(client: bigquery.Client, cache_key: Tuple[str, str, str], use_bqstorage: bool, timeout_sec: int):
    st.subheader("ヘルスチェック")
    st.write("まずここで BigQuery 接続を確定させます（SELECT 1）。")

    if st.button("SELECT 1（接続チェック）", use_container_width=True, type="primary"):
        df = query_df(
            client,
            "SELECT 1 AS ok",
            params=None,
            label="SELECT 1 ヘルスチェック",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        st.success("OK（BigQuery 接続成功）")
        st.dataframe(df, use_container_width=True)


def resolve_role(client: bigquery.Client, cache_key: Tuple[str, str, str], login_email: str, use_bqstorage: bool, timeout_sec: int) -> RoleInfo:
    df_role = query_df(
        client,
        SQL_ROLE_LOOKUP,
        params={"login_email": login_email},
        label="ロール取得",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )
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


def resolve_display_name(client: bigquery.Client, cache_key: Tuple[str, str, str], login_email: str, use_bqstorage: bool, timeout_sec: int) -> str:
    df = query_df(
        client,
        SQL_STAFF_NAME,
        params={"login_email": login_email},
        label="氏名表示取得",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )
    if df.empty:
        return login_email
    v = df.iloc[0].to_dict()
    return str(v.get("display_name") or login_email)


def render_fytd_org_summary(client: bigquery.Client, cache_key: Tuple[str, str, str], role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("年度累計（FYTD）｜組織サマリー")
    if show_sql:
        st.code(SQL_FYTD_ORG_SUMMARY, language="sql")
    if st.button("年度累計（組織）を読み込む", use_container_width=True):
        df = query_df(
            client,
            SQL_FYTD_ORG_SUMMARY,
            params={"login_email": role.login_email},
            label="年度累計（組織）",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        st.dataframe(df, use_container_width=True)


def render_yoy_rankings(client: bigquery.Client, cache_key: Tuple[str, str, str], role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("当月（前年同月比）ランキング（Top / Bottom / 比較不能）")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("#### Top（伸び先）")
        if show_sql:
            st.code(SQL_YOY_TOP, language="sql")
        if st.button("Top を読み込む", use_container_width=True):
            df = query_df(client, SQL_YOY_TOP, params={"login_email": role.login_email},
                          label="当月YoY Top", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)

    with c2:
        st.markdown("#### Bottom（下落先）")
        if show_sql:
            st.code(SQL_YOY_BOTTOM, language="sql")
        if st.button("Bottom を読み込む", use_container_width=True):
            df = query_df(client, SQL_YOY_BOTTOM, params={"login_email": role.login_email},
                          label="当月YoY Bottom", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)

    with c3:
        st.markdown("#### 比較不能（Uncomparable）")
        if show_sql:
            st.code(SQL_YOY_UNCOMPARABLE, language="sql")
        if st.button("Uncomparable を読み込む", use_container_width=True):
            df = query_df(client, SQL_YOY_UNCOMPARABLE, params={"login_email": role.login_email},
                          label="当月YoY Uncomparable", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)


def render_new_deliveries(client: bigquery.Client, cache_key: Tuple[str, str, str], role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("新規納品（入口固定：昨日 / 直近7日 / 当月 / FYTD）")
    tabs = st.tabs(["昨日", "直近7日", "当月", "FYTD"])

    with tabs[0]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_YESTERDAY, language="sql")
        if st.button("昨日分を読み込む", use_container_width=True):
            df = query_df(client, SQL_NEW_DELIVERIES_YESTERDAY, params={"login_email": role.login_email},
                          label="新規納品（昨日）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)

    with tabs[1]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_LAST7D, language="sql")
        if st.button("直近7日を読み込む", use_container_width=True):
            df = query_df(client, SQL_NEW_DELIVERIES_LAST7D, params={"login_email": role.login_email},
                          label="新規納品（直近7日）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)

    with tabs[2]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_THIS_MONTH, language="sql")
        if st.button("当月を読み込む", use_container_width=True):
            df = query_df(client, SQL_NEW_DELIVERIES_THIS_MONTH, params={"login_email": role.login_email},
                          label="新規納品（当月）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)

    with tabs[3]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_FYTD, language="sql")
        if st.button("FYTD を読み込む", use_container_width=True):
            df = query_df(client, SQL_NEW_DELIVERIES_FYTD, params={"login_email": role.login_email},
                          label="新規納品（FYTD）", use_bqstorage=use_bqstorage, timeout_sec=timeout_sec, cache_key=cache_key)
            st.dataframe(df, use_container_width=True)


def main():
    set_page()

    # 1) 接続情報の確定（Secrets or JSON貼り付け）
    project_id, location, sa = ensure_credentials_ui()
    sa_json = json.dumps(sa, ensure_ascii=False, sort_keys=True)
    cache_key = (project_id, location, sa_json)

    # 2) クライアント作成
    client = get_bq_client(project_id, location, sa)

    # 3) UI設定
    opts = sidebar_controls()
    login_email = get_login_email_ui()

    # 4) ヘルスチェック（最優先）
    render_health_check(client, cache_key, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
    st.divider()

    # 5) ロール/氏名（軽いはず。重ければボタン化可能）
    st.subheader("ログイン情報")
    with st.spinner("ロール・氏名を取得中..."):
        role = resolve_role(client, cache_key, login_email, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
        display_name = resolve_display_name(client, cache_key, login_email, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])

    st.write(f"**ログイン:** {display_name}")
    st.write(f"**メール:** {role.login_email}")
    st.write(f"**ロール:** {role.role_key}（admin_view={role.role_admin_view}, admin_edit={role.role_admin_edit}, sales_view={role.role_sales_view}）")
    if role.area_key:
        st.write(f"**エリア:** {role.area_key}")

    st.divider()

    # 6) 入口（OS v1.4.6 順）
    st.header("管理者入口（判断専用・高速）")
    render_fytd_org_summary(client, cache_key, role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    render_yoy_rankings(client, cache_key, role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    render_new_deliveries(client, cache_key, role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])

    st.divider()
    st.caption("BadRequest が出たら job.errors / SQL / params を画面に出します（Streamlitの赤塗り回避）。")


if __name__ == "__main__":
    main()
