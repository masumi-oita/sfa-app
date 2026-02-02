# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.4.6

特徴
- 遅延ロード（起動時に重いクエリを走らせない）
- timeout / BigQuery Storage API ON-OFF / SELECT1 ヘルスチェック
- BadRequest の詳細（job.errors, SQL, params）を必ず画面に表示
- Secrets 未設定でも「サービスアカウントJSON貼り付け」で一時起動可（セッション限定）

★今回の変更（前回からの差分）
1) VIEWの列名差分を吸収するため、INFORMATION_SCHEMA.COLUMNS で列一覧を取得し、SQLを自動生成
   - role: area_key が無くても落ちない
   - staff_name: display_name が無くても落ちない（候補列を探索）
   - 各集計VIEW: login_email 列が無い場合は WHERE login_email を自動で外す
2) クエリ失敗時に raise してアプリを落とさず、空DataFrameで継続（入口高速版として正しい）
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


# =============================================================================
# 基本設定
# =============================================================================

APP_TITLE = "SFA｜入口高速版（判断専用）"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"


# =============================================================================
# 対象オブジェクト（VIEW）
# =============================================================================

VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_dim_staff_role_dedup"
VIEW_STAFF_NAME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_email_name"

VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month"

VIEW_NEW_DELIVERIES = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"


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
    area_key: str = ""  # 無い環境があるので、取れたら入れる程度（無ければ空）


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
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("JSONの形式が不正です（dictではありません）")
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

# 任意：ログイン入力の初期値
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


# =============================================================================
# BigQuery helper（エラー可視化＋スキーマ自動吸収）
# =============================================================================

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
    """
    ★重要：失敗しても raise しない（入口高速版としてアプリを落とさない）
    """
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
    # "project.dataset.table"
    parts = table_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"FQN が不正です: {table_fqn}")
    return parts[0], parts[1], parts[2]


@st.cache_data(show_spinner=False, ttl=3600)
def get_table_columns(
    project_id: str,
    location: str,
    sa_json: str,
    table_fqn: str,
) -> List[str]:
    """
    ★軽量：INFORMATION_SCHEMA から列名一覧を取得（スキーマ差分吸収用）
    """
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


def build_role_sql(role_cols: List[str]) -> str:
    """
    ★role_key / role_tier の揺れ、area_key の有無を吸収して SELECT を組む
    """
    # role_key の候補（実際には role_tier があるケースが多い）
    role_key_col = pick_first_existing(role_cols, ["role_key", "role_tier", "role", "role_name"])
    # area の候補
    area_col = pick_first_existing(role_cols, ["area_key", "area", "area_code", "area_name", "エリア", "エリアキー"])

    select_parts = ["login_email"]

    if role_key_col:
        if role_key_col != "role_key":
            select_parts.append(f"{role_key_col} AS role_key")
        else:
            select_parts.append("role_key")

    # 権限フラグ（無ければ後でデフォルト）
    for c in ["role_admin_view", "role_admin_edit", "role_sales_view"]:
        if has_column(role_cols, c):
            select_parts.append(c)

    # area は存在する場合だけ
    if area_col:
        if area_col != "area_key":
            select_parts.append(f"{area_col} AS area_key")
        else:
            select_parts.append("area_key")

    select_sql = ",\n  ".join(select_parts)

    return f"""
SELECT
  {select_sql}
FROM `{VIEW_ROLE}`
WHERE login_email = @login_email
LIMIT 1
"""


def resolve_role(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    project_id: str,
    location: str,
    sa_json: str,
    login_email: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> RoleInfo:
    role_cols = get_table_columns(project_id, location, sa_json, VIEW_ROLE)
    if not role_cols:
        st.warning("ロールVIEWの列一覧取得に失敗。暫定 SALES で継続します。")
        return RoleInfo(login_email=login_email)

    sql = build_role_sql(role_cols)
    df = query_df_safe(
        client,
        sql,
        params={"login_email": login_email},
        label="ロール取得",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )

    if df.empty:
        st.warning("ロール取得：該当なし or 失敗。暫定 SALES で継続します。")
        return RoleInfo(login_email=login_email)

    r = df.iloc[0].to_dict()

    return RoleInfo(
        login_email=str(r.get("login_email", login_email)),
        role_key=str(r.get("role_key", "SALES") or "SALES"),
        role_admin_view=bool(r.get("role_admin_view", False)) if "role_admin_view" in r else False,
        role_admin_edit=bool(r.get("role_admin_edit", False)) if "role_admin_edit" in r else False,
        role_sales_view=bool(r.get("role_sales_view", True)) if "role_sales_view" in r else True,
        area_key=str(r.get("area_key", "") or ""),
    )


def build_staff_name_sql(name_cols: List[str]) -> Optional[str]:
    """
    ★display_name が無い環境を吸収：候補列を探索して SELECT を組む
    """
    # 名前の候補（あなたの実体に合わせて増やしてOK）
    name_col = pick_first_existing(
        name_cols,
        ["display_name", "staff_name", "name", "full_name", "user_name", "氏名", "担当者名", "担当者"],
    )
    if not name_col:
        return None

    if name_col != "display_name":
        name_expr = f"{name_col} AS display_name"
    else:
        name_expr = "display_name"

    return f"""
SELECT
  login_email,
  {name_expr}
FROM `{VIEW_STAFF_NAME}`
WHERE login_email = @login_email
LIMIT 1
"""


def resolve_display_name(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    project_id: str,
    location: str,
    sa_json: str,
    login_email: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> str:
    cols = get_table_columns(project_id, location, sa_json, VIEW_STAFF_NAME)
    if not cols:
        return login_email

    sql = build_staff_name_sql(cols)
    if not sql:
        return login_email

    df = query_df_safe(
        client,
        sql,
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


def build_scoped_select_sql(table_fqn: str, cols: List[str], limit: int = 2000) -> str:
    """
    ★login_email 列がある時だけ WHERE を付ける（無いなら付けない）
    """
    if has_column(cols, "login_email"):
        return f"""
SELECT
  *
FROM `{table_fqn}`
WHERE login_email = @login_email
LIMIT {limit}
"""
    return f"""
SELECT
  *
FROM `{table_fqn}`
LIMIT {limit}
"""


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
    extra_params: Optional[Dict[str, Any]] = None,
):
    st.subheader(title)

    cols = get_table_columns(project_id, location, sa_json, table_fqn)
    sql = build_scoped_select_sql(table_fqn, cols, limit=2000)

    params = {"login_email": login_email} if "login_email" in sql else {}
    if extra_params:
        params.update(extra_params)

    if show_sql:
        st.code(sql, language="sql")
        st.caption(f"columns({len(cols)}): " + ", ".join(cols[:50]) + (" ..." if len(cols) > 50 else ""))

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
    with st.spinner("ロール・氏名を取得中..."):
        role = resolve_role(
            client, cache_key, project_id, location, sa_json, login_email,
            use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"]
        )
        display_name = resolve_display_name(
            client, cache_key, project_id, location, sa_json, login_email,
            use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"]
        )

    st.write(f"**ログイン:** {display_name}")
    st.write(f"**メール:** {role.login_email}")
    st.write(f"**ロール:** {role.role_key}（admin_view={role.role_admin_view}, admin_edit={role.role_admin_edit}, sales_view={role.role_sales_view}）")
    if role.area_key:
        st.write(f"**エリア:** {role.area_key}")

    st.divider()

    st.header("管理者入口（判断専用・高速）")

    # FYTD（組織）
    render_table_block(
        title="年度累計（FYTD）｜組織サマリー",
        client=client,
        cache_key=cache_key,
        project_id=project_id,
        location=location,
        sa_json=sa_json,
        table_fqn=VIEW_FYTD_ORG,
        login_email=role.login_email,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
        show_sql=opts["show_sql"],
        button_label="年度累計（組織）を読み込む",
    )
    st.divider()

    # YoY（Top / Bottom / Uncomparable）
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
            login_email=role.login_email,
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
            login_email=role.login_email,
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
            login_email=role.login_email,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
            button_label="Uncomparable を読み込む",
        )

    st.divider()

    # 新規納品（入口4本固定）
    st.subheader("新規納品（入口固定：昨日 / 直近7日 / 当月 / FYTD）")
    st.caption("※ period_key 列が無いVIEWの場合、ここはVIEW側の仕様に合わせてSQLを差し替えます。まずは列一覧を表示して確認できます。")

    tabs = st.tabs(["昨日", "直近7日", "当月", "FYTD"])

    # VIEW_NEW_DELIVERIES の列一覧を確認して、period_key があればフィルタ、無ければ単純表示
    nd_cols = get_table_columns(project_id, location, sa_json, VIEW_NEW_DELIVERIES)
    has_period = has_column(nd_cols, "period_key")

    def new_delivery_sql(period_value: str) -> Tuple[str, Dict[str, Any]]:
        base = build_scoped_select_sql(VIEW_NEW_DELIVERIES, nd_cols, limit=2000)
        params = {"login_email": role.login_email} if "login_email" in base else {}
        if has_period:
            # WHERE句の有無に応じて AND / WHERE を切替
            if "WHERE" in base:
                sql = base.replace(f"LIMIT 2000", f'  AND period_key = "{period_value}"\nLIMIT 2000')
            else:
                sql = base.replace(f"LIMIT 2000", f'WHERE period_key = "{period_value}"\nLIMIT 2000')
            return sql, params
        return base, params

    for i, (label, period_value) in enumerate([("昨日", "yesterday"), ("直近7日", "last_7_days"), ("当月", "this_month"), ("FYTD", "fytd")]):
        with tabs[i]:
            sql, params = new_delivery_sql(period_value)
            if opts["show_sql"]:
                st.code(sql, language="sql")
                st.caption(f"columns({len(nd_cols)}): " + ", ".join(nd_cols[:50]) + (" ..." if len(nd_cols) > 50 else ""))

            if st.button(f"{label} を読み込む", use_container_width=True):
                df = query_df_safe(
                    client,
                    sql,
                    params=params if params else None,
                    label=f"新規納品（{label}）",
                    use_bqstorage=opts["use_bqstorage"],
                    timeout_sec=opts["timeout_sec"],
                    cache_key=cache_key,
                )
                if df.empty:
                    st.info("データが空、またはクエリ失敗（上にエラー詳細が表示されています）。")
                else:
                    st.dataframe(df, use_container_width=True)

    st.divider()
    st.caption("BadRequest が出たら job.errors / SQL / params を画面に出します（赤落ちさせずに切り分け可能）。")


if __name__ == "__main__":
    main()
