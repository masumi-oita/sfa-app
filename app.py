# app.py
# -*- coding: utf-8 -*-
"""
SFA・入口高速版（判断専用） - OS v1.4.6
- BigQuery: asia-northeast1
- UI: 遅延ロード（ボタン押下で必要な集計だけ実行）
- 切り分け: timeout / BigQuery Storage API ON-OFF / SELECT1 ヘルスチェック
- 事故対策: BigQuery BadRequest の詳細（job.errors 等）を画面に表示

必要な secrets（Streamlit Cloud）例:
[bigquery]
project_id = "salesdb-479915"
location = "asia-northeast1"

# サービスアカウントJSON（丸ごと）
[bigquery.service_account]
type = "service_account"
project_id = "salesdb-479915"
private_key_id = "xxxx"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "xxx@xxx.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
universe_domain = "googleapis.com"
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
# 設定
# =============================================================================

APP_TITLE = "SFA｜入口高速版（判断専用）"
DEFAULT_TZ = "Asia/Tokyo"

# ---- あなたのBigQuery VIEW名に合わせてここを編集してください ----
# 入口（管理者） FYTD サマリー（組織）
SQL_FYTD_ORG_SUMMARY = """
-- 年度累計（組織）: 今年度累計 vs 昨年度累計
-- 必須列例:
--   fiscal_year, fytd_sales, fytd_gp, fytd_gp_rate,
--   prev_fiscal_year, prev_fytd_sales, prev_fytd_gp, prev_fytd_gp_rate,
--   diff_sales, diff_gp
SELECT
  *
FROM `salesdb-479915.sales_data.v_admin_org_fytd_summary_scoped`
WHERE login_email = @login_email
LIMIT 2000
"""

# 当月YoYランキング（上）
SQL_YOY_TOP = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_top_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

# 当月YoYランキング（下）
SQL_YOY_BOTTOM = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_bottom_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

# 当月YoY（比較不能：新規・前年なし）
SQL_YOY_UNCOMPARABLE = """
SELECT
  *
FROM `salesdb-479915.sales_data.v_sales_customer_yoy_uncomparable_current_month`
WHERE login_email = @login_email
LIMIT 2000
"""

# 新規納品（入口4本固定）: 昨日 / 直近7日 / 当月 / FYTD
# ここは「計算済みVIEW」が理想。無ければ、v_new_deliveries_realized_daily_fact_all_months を軽く集計してもOK。
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

# ロール（権限）取得：あなたの環境のDIM/VIEWに合わせて調整してください
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

# 担当者表示名（メール→氏名）
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
    area_key: str = ""  # 例: "oita" / "kumamoto" / ""


# =============================================================================
# BigQuery Client
# =============================================================================

def _get_secrets() -> Tuple[str, str, Dict[str, Any]]:
    # secrets から取得（必須）
    if "bigquery" not in st.secrets:
        raise RuntimeError("st.secrets に [bigquery] がありません。project_id/location/service_account を設定してください。")

    bq = st.secrets["bigquery"]
    project_id = bq.get("project_id")
    location = bq.get("location", "asia-northeast1")
    sa = bq.get("service_account")

    if not project_id or not sa:
        raise RuntimeError("st.secrets['bigquery'] に project_id と service_account が必要です。")

    return str(project_id), str(location), dict(sa)


@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    project_id, location, sa = _get_secrets()
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    return client


# =============================================================================
# クエリ実行（キャッシュ安全 & エラー可視化）
# =============================================================================

def _build_query_parameters(params: Optional[Dict[str, Any]]) -> list[bigquery.ScalarQueryParameter]:
    qparams: list[bigquery.ScalarQueryParameter] = []
    if not params:
        return qparams

    for k, v in params.items():
        # 明示型（まずは安全な推論）
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
    exc: Exception
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


@st.cache_data(show_spinner=False, ttl=300)
def cached_query_df(
    sql: str,
    params_json: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    """
    注意:
    - st.cache_data 内で session_state を触らない（OS v1.4.6）
    - params は hash 安定化のため JSON 文字列で受ける
    """
    client = get_bq_client()
    params = json.loads(params_json) if params_json else {}

    job_config = bigquery.QueryJobConfig()
    qparams = _build_query_parameters(params)
    if qparams:
        job_config.query_parameters = qparams

    job: Optional[bigquery.job.QueryJob] = None
    try:
        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)  # ここで BadRequest が出ることがある
        df = job.to_dataframe(create_bqstorage_client=use_bqstorage)
        return df

    except Exception as e:
        # cache関数内は st.* しない（返り値のみ）を徹底したいが、
        # 原因特定中は "見える化" が最優先なので、例外だけ投げて上位で表示する
        raise e


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
        df = cached_query_df(sql, params_json=params_json, use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
        return df

    except BadRequest as e:
        # BadRequest は job.errors が欲しいが cached_query_df 内では job を取れないため、
        # 同じSQLを「ノーキャッシュ」で一度だけ再実行して job.errors を取得して表示する
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig()
        qparams = _build_query_parameters(params or {})
        if qparams:
            job_config.query_parameters = qparams

        job: Optional[bigquery.job.QueryJob] = None
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
    st.caption("OS v1.4.6｜起動時に重いクエリを自動実行しない（遅延ロード）｜BigQuery エラー詳細を必ず画面に表示")


def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.header("設定（切り分け）")
    use_bqstorage = st.sidebar.toggle("BigQuery Storage API を使う（高速）", value=True)
    timeout_sec = st.sidebar.slider("クエリタイムアウト（秒）", min_value=10, max_value=300, value=60, step=10)
    show_sql = st.sidebar.toggle("SQL を表示する（デバッグ）", value=False)
    return {
        "use_bqstorage": use_bqstorage,
        "timeout_sec": timeout_sec,
        "show_sql": show_sql,
    }


def get_login_email_ui() -> str:
    st.sidebar.header("ログイン（簡易）")
    default_email = st.secrets.get("default_login_email", "")
    login_email = st.sidebar.text_input("login_email（メール）", value=default_email, placeholder="例: masumi@example.com")
    login_email = (login_email or "").strip()
    if not login_email:
        st.info("左サイドバーで login_email を入力してください（暫定ログイン）。")
        st.stop()
    return login_email


def render_health_check(use_bqstorage: bool, timeout_sec: int):
    st.subheader("ヘルスチェック")
    cols = st.columns([1, 3])
    with cols[0]:
        if st.button("SELECT 1（接続チェック）", use_container_width=True):
            df = query_df("SELECT 1 AS ok", params=None, label="SELECT 1 ヘルスチェック",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.success("OK（BigQuery 接続成功）")
            st.dataframe(df, use_container_width=True)

    with cols[1]:
        st.write("切り分けの基本:")
        st.write("- まず SELECT 1 が通るか")
        st.write("- 次に Storage API ON/OFF を切り替えて挙動が変わるか")
        st.write("- timeout を短くして『どこで詰まるか』を特定する")


# =============================================================================
# ロール解決
# =============================================================================

def resolve_role(login_email: str, use_bqstorage: bool, timeout_sec: int) -> RoleInfo:
    # 1) role
    df_role = query_df(SQL_ROLE_LOOKUP, params={"login_email": login_email}, label="ロール取得",
                       use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
    if df_role.empty:
        # 未登録は SALES 扱い
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
# 入口：年度累計（組織）
# =============================================================================

def render_fytd_org_summary(role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("年度累計（FYTD）｜組織サマリー")
    st.write("今年度累計 vs 昨年度累計（差額も含む）")

    if show_sql:
        st.code(SQL_FYTD_ORG_SUMMARY, language="sql")

    if st.button("年度累計（組織）を読み込む", use_container_width=True, type="primary"):
        df = query_df(SQL_FYTD_ORG_SUMMARY, params={"login_email": role.login_email},
                      label="年度累計（組織）",
                      use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
        st.dataframe(df, use_container_width=True)


# =============================================================================
# 入口：当月YoYランキング
# =============================================================================

def render_yoy_rankings(role: RoleInfo, use_bqstorage: bool, timeout_sec: int, show_sql: bool):
    st.subheader("当月（前年同月比）ランキング")
    st.write("上がっている先 / 下がっている先 / 比較不能（新規・前年なし） を分離して表示")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown("#### 上がっている先（Top）")
        if show_sql:
            st.code(SQL_YOY_TOP, language="sql")
        if st.button("Top を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_TOP, params={"login_email": role.login_email},
                          label="当月YoY Top",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with c2:
        st.markdown("#### 下がっている先（Bottom）")
        if show_sql:
            st.code(SQL_YOY_BOTTOM, language="sql")
        if st.button("Bottom を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_BOTTOM, params={"login_email": role.login_email},
                          label="当月YoY Bottom",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with c3:
        st.markdown("#### 比較不能（Uncomparable）")
        if show_sql:
            st.code(SQL_YOY_UNCOMPARABLE, language="sql")
        if st.button("Uncomparable を読み込む", use_container_width=True):
            df = query_df(SQL_YOY_UNCOMPARABLE, params={"login_email": role.login_email},
                          label="当月YoY Uncomparable",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
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
                          label="新規納品（昨日）",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[1]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_LAST7D, language="sql")
        if st.button("直近7日を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_LAST7D, params={"login_email": role.login_email},
                          label="新規納品（直近7日）",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[2]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_THIS_MONTH, language="sql")
        if st.button("当月を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_THIS_MONTH, params={"login_email": role.login_email},
                          label="新規納品（当月）",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)

    with tabs[3]:
        if show_sql:
            st.code(SQL_NEW_DELIVERIES_FYTD, language="sql")
        if st.button("FYTD を読み込む", use_container_width=True):
            df = query_df(SQL_NEW_DELIVERIES_FYTD, params={"login_email": role.login_email},
                          label="新規納品（FYTD）",
                          use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            st.dataframe(df, use_container_width=True)


# =============================================================================
# Main
# =============================================================================

def main():
    set_page()
    opts = sidebar_controls()
    login_email = get_login_email_ui()

    # まずはヘルスチェックを最上段に置く（事故切り分け最短）
    render_health_check(use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
    st.divider()

    # ロール解決（ここも遅延にしたい場合はボタン化も可）
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

    # 入口：管理者（または admin_view）優先
    # ※ 入口順序：FYTD → 当月YoY → 新規納品 → その後（担当別は Phase1.6）
    st.header("管理者入口（判断専用・高速）")

    # FYTD（組織）
    render_fytd_org_summary(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    # 当月YoY
    render_yoy_rankings(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])
    st.divider()

    # 新規納品
    render_new_deliveries(role, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"], show_sql=opts["show_sql"])

    st.divider()
    st.caption("注：BigQuery BadRequest が出た場合、画面に job.errors / SQL / params を出します（赤塗り回避）。")


if __name__ == "__main__":
    main()
