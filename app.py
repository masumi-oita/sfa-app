# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.4.6

★今回の変更（前回からの差分）
- scopedフィルタのキー候補を拡張：
  login_email が無いVIEWでも viewer_email / viewer_mail などで絞れる
- これにより v_admin_org_fytd_summary_scoped が空になりにくい
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, GoogleAPICallError


APP_TITLE = "SFA｜入口高速版（判断専用）"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_dim_staff_role_dedup"
VIEW_STAFF_NAME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_email_name"

VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month"

VIEW_NEW_DELIVERIES = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"


@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    role_key: str = "SALES"
    role_admin_view: bool = False
    role_admin_edit: bool = False
    role_sales_view: bool = True
    area_key: str = ""


def _secrets_has_bigquery() -> bool:
    if "bigquery" not in st.secrets:
        return False
    bq = st.secrets.get("bigquery", {})
    return bool(bq.get("project_id")) and bool(bq.get("service_account"))


def _get_bq_from_secrets() -> Tuple[str, str, Dict[str, Any]]:
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    return project_id, location, sa


def _parse_service_account_json(text: str) -> Dict[str, Any]:
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("JSONの形式が不正です（dictではありません）")
    for k in ["type", "project_id", "private_key", "client_email"]:
        if k not in obj:
            raise ValueError(f"サービスアカウントJSONに {k} がありません")
    return obj


def ensure_credentials_ui() -> Tuple[str, str, Dict[str, Any]]:
    st.sidebar.header("接続設定")

    if _secrets_has_bigquery():
        project_id, location, sa = _get_bq_from_secrets()
        st.sidebar.success("Secrets: OK（st.secrets から BigQuery 設定を読み込み）")
        return project_id, location, sa

    st.sidebar.warning("Secrets が未設定です。下で『サービスアカウントJSON貼り付け』で暫定接続できます。")

    with st.expander("（推奨）Streamlit Cloud Secrets 設定テンプレ", expanded=False):
        template = f"""[bigquery]
project_id = "{PROJECT_DEFAULT}"
location = "{DEFAULT_LOCATION}"

[bigquery.service_account]
type = "service_account"
project_id = "{PROJECT_DEFAULT}"
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

    project_id = st.sidebar.text_input("project_id（暫定）", value=PROJECT_DEFAULT)
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

    sa["project_id"] = project_id.strip() or sa.get("project_id")
    st.sidebar.success("貼り付けJSON: OK（このセッション中のみ有効）")
    return str(project_id), str(location), sa


@st.cache_resource(show_spinner=False)
def get_bq_client(project_id: str, location: str, sa: Dict[str, Any]) -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)


def _build_query_parameters(params: Optional[Dict[str, Any]]) -> List[bigquery.ScalarQueryParameter]:
    qparams: List[bigquery.ScalarQueryParameter] = []
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
    st.write("**発生時刻:**", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    st.write("**params:**")
    st.code(json.dumps(params or {}, ensure_ascii=False, indent=2), language="json")

    if job is not None:
        st.write("**job_id:**", getattr(job, "job_id", None))
        st.write("**location:**", getattr(job, "location", None))
        if getattr(job, "errors", None):
            st.write("**job.errors（最重要）:**")
            st.json(job.errors)

    st.write("**sql:**")
    st.code(sql, language="sql")
    st.write("**exception:**", str(exc))


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


def query_df_safe(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    use_bqstorage: bool = True,
    timeout_sec: int = 60,
    cache_key: Optional[Tuple[str, str, str]] = None,
) -> pd.DataFrame:
    params_json = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)

    try:
        if cache_key is None:
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
        job = None
        try:
            job_config = bigquery.QueryJobConfig()
            qparams = _build_query_parameters(params or {})
            if qparams:
                job_config.query_parameters = qparams
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
        except Exception as e2:
            _show_bq_error_context(label or "query_df_safe", sql, params, job, e2)
            return pd.DataFrame()
        _show_bq_error_context(label or "query_df_safe", sql, params, job, e)
        return pd.DataFrame()

    except GoogleAPICallError as e:
        _show_bq_error_context(label or "query_df_safe", sql, params, None, e)
        return pd.DataFrame()

    except Exception as e:
        _show_bq_error_context(label or "query_df_safe", sql, params, None, e)
        return pd.DataFrame()


def _split_table_fqn(table_fqn: str) -> Tuple[str, str, str]:
    parts = table_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"FQN が不正です: {table_fqn}")
    return parts[0], parts[1], parts[2]


@st.cache_data(show_spinner=False, ttl=3600)
def get_table_columns(project_id: str, location: str, sa_json: str, table_fqn: str) -> List[str]:
    sa = json.loads(sa_json)
    client = get_bq_client(project_id, location, sa)

    p, d, t = _split_table_fqn(table_fqn)
    sql = f"""
    SELECT column_name
    FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
    ORDER BY ordinal_position
    """
    df = query_df_safe(
        client,
        sql,
        params={"table_name": t},
        label=f"列一覧取得（{table_fqn}）",
        use_bqstorage=False,
        timeout_sec=30,
        cache_key=(project_id, location, sa_json),
    )
    if df.empty:
        return []
    return [str(x) for x in df["column_name"].tolist()]


def pick_first_existing(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None


def has_column(cols: List[str], col: str) -> bool:
    return col in set(cols)


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
        df = query_df_safe(
            client,
            "SELECT 1 AS ok",
            params=None,
            label="SELECT 1 ヘルスチェック",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        if df.empty:
            return
        st.success("OK（BigQuery 接続成功）")
        st.dataframe(df, use_container_width=True)


def build_scoped_select_sql(table_fqn: str, cols: List[str], limit: int = 2000) -> Tuple[str, Dict[str, Any]]:
    """
    ★ここが今回の肝：
    login_email が無いVIEWでも viewer_email などがあれば絞る
    """
    # スコープキー候補（優先順）
    scope_col = pick_first_existing(
        cols,
        [
            "login_email",
            "viewer_email",   # ★今回確定
            "viewer_mail",
            "viewer",
            "user_email",
            "email",
        ],
    )

    params: Dict[str, Any] = {}
    if scope_col:
        sql = f"""
SELECT
  *
FROM `{table_fqn}`
WHERE {scope_col} = @login_email
LIMIT {limit}
"""
        params["login_email"] = None  # 後で埋める
        return sql, params

    sql = f"""
SELECT
  *
FROM `{table_fqn}`
LIMIT {limit}
"""
    return sql, params


def render_table_block(
    title: str,
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    project_id: str,
    location: str,
    sa_json: str,
    table_fqn: str,
    login_email: str,
    use_bqstorage: bool,
    timeout_sec: int,
    show_sql: bool,
    button_label: str,
):
    st.subheader(title)

    cols = get_table_columns(project_id, location, sa_json, table_fqn)
    sql, params = build_scoped_select_sql(table_fqn, cols, limit=2000)
    if "login_email" in params:
        params["login_email"] = login_email

    if show_sql:
        st.code(sql, language="sql")
        st.caption(f"columns({len(cols)}): " + ", ".join(cols) if cols else "columns(0)")

    if st.button(button_label, use_container_width=True):
        df = query_df_safe(
            client,
            sql,
            params=params if params else None,
            label=title,
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        if df.empty:
            st.info("データが空、またはクエリ失敗（上にエラー詳細が表示されています）。")
            return
        st.dataframe(df, use_container_width=True)


def main():
    set_page()

    project_id, location, sa = ensure_credentials_ui()
    sa_json = json.dumps(sa, ensure_ascii=False, sort_keys=True)
    cache_key = (project_id, location, sa_json)
    client = get_bq_client(project_id, location, sa)

    opts = sidebar_controls()
    login_email = get_login_email_ui()

    render_health_check(client, cache_key, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])
    st.divider()

    st.subheader("ログイン情報")
    st.write(f"**ログイン:** {login_email}")
    st.write(f"**メール:** {login_email}")
    st.write("※ロール表示は既に成功しているので、この版では入口検証を優先（落とさない）。")

    st.divider()

    st.header("管理者入口（判断専用・高速）")

    render_table_block(
        title="年度累計（FYTD）｜組織サマリー",
        client=client,
        cache_key=cache_key,
        project_id=project_id,
        location=location,
        sa_json=sa_json,
        table_fqn=VIEW_FYTD_ORG,
        login_email=login_email,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
        show_sql=opts["show_sql"],
        button_label="年度累計（組織）を読み込む",
    )

    st.divider()

    c1, c2, c3 = st.columns(3)
    with c1:
        render_table_block(
            title="当月YoY｜Top（伸び先）",
            client=client,
            cache_key=cache_key,
            project_id=project_id,
            location=location,
            sa_json=sa_json,
            table_fqn=VIEW_YOY_TOP,
            login_email=login_email,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
            button_label="Top を読み込む",
        )
    with c2:
        render_table_block(
            title="当月YoY｜Bottom（下落先）",
            client=client,
            cache_key=cache_key,
            project_id=project_id,
            location=location,
            sa_json=sa_json,
            table_fqn=VIEW_YOY_BOTTOM,
            login_email=login_email,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
            button_label="Bottom を読み込む",
        )
    with c3:
        render_table_block(
            title="当月YoY｜比較不能（Uncomparable）",
            client=client,
            cache_key=cache_key,
            project_id=project_id,
            location=location,
            sa_json=sa_json,
            table_fqn=VIEW_YOY_UNCOMP,
            login_email=login_email,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
            button_label="Uncomparable を読み込む",
        )

    st.divider()
    st.subheader("新規納品（入口固定：昨日 / 直近7日 / 当月 / FYTD）")
    st.caption("このブロックは次の段。まずFYTD/YoYが出ることを確認してから整える。")

    st.caption("BadRequest が出たら job.errors / SQL / params を画面に出します（アプリは落ちません）。")


if __name__ == "__main__":
    main()
