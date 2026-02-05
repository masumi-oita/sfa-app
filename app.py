# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.4.7

★今回の修正（Step 1: 入口UIの洗練）
- ロジック・データ・接続周りは v1.4.6 を完全踏襲
- ロールに応じてタブ構成を動的に変更
    - HQ_ADMIN / AREA_MANAGER: [全社状況] [自分の担当]
    - SALES: [今年の成績(FYTD)] [得意先別(YoY)]
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

# -----------------------------
# BigQuery Views（FQN）
# -----------------------------
VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_dim_staff_role_dedup"

# 全社FYTD（管理者用）
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
# 自分FYTD（全員用）★前年差が出る本線
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"

# 当月YoY（得意先ランキング）
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"


# -----------------------------
# 日本語ラベル（表示専用）
# -----------------------------
JP_COLS_FYTD = {
    "viewer_email": "閲覧者メール",
    "login_email": "ログインメール",
    "display_name": "担当者名",
    "role_tier": "ロール",
    "area_name": "エリア",
    "current_month": "当月（月初）",
    "fy_start": "年度開始",
    "sales_amount_fytd": "売上（FYTD）",
    "gross_profit_fytd": "粗利（FYTD）",
    "gross_profit_rate_fytd": "粗利率（FYTD）",
    "sales_amount_py_fytd": "売上（前年FYTD）",
    "gross_profit_py_fytd": "粗利（前年FYTD）",
    "sales_diff_fytd": "前年差（売上）",
    "gp_diff_fytd": "前年差（粗利）",
}

JP_COLS_YOY = {
    "login_email": "ログインメール",
    "display_name": "担当者名",
    "month": "対象月（月初）",
    "customer_code": "得意先コード",
    "customer_name": "得意先名",
    "sales_amount": "売上（当月）",
    "gross_profit": "粗利（当月）",
    "gross_profit_rate": "粗利率（当月）",
    "sales_amount_py": "売上（前年同月）",
    "gross_profit_py": "粗利（前年同月）",
    "gross_profit_rate_py": "粗利率（前年同月）",
    "sales_diff_yoy": "前年差（売上）",
    "gp_diff_yoy": "前年差（粗利）",
    "sales_yoy_rate": "前年同月比（売上）",
    "gp_yoy_rate": "前年同月比（粗利）",
    "pri_gp_abs": "優先度：粗利額",
    "pri_gp_rate_abs": "優先度：粗利率",
    "pri_sales_abs": "優先度：売上",
}


def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)


# -----------------------------
# Role
# -----------------------------
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    role_key: str = "SALES"  # HQ_ADMIN / AREA_MANAGER / SALES
    role_admin_view: bool = False
    role_admin_edit: bool = False
    role_sales_view: bool = True
    area_name: str = "未設定"


def normalize_role_key(role_key: str) -> str:
    rk = (role_key or "").strip().upper()
    if rk in ("HQ_ADMIN", "AREA_MANAGER", "SALES"):
        return rk
    return "SALES"


# -----------------------------
# Secrets / Client
# -----------------------------
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
        # 例外時もエラー詳細を表示して落とさない
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


def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.7｜UI洗練（ロール別タブ）｜遅延ロード｜timeout/Storage API 切替")


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


def resolve_role(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    login_email: str,
    use_bqstorage: bool,
    timeout_sec: int,
) -> RoleInfo:
    sql = f"""
SELECT
  login_email,
  role_tier AS role_key,
  role_admin_view,
  role_admin_edit,
  role_sales_view,
  IFNULL(area_name, "未設定") AS area_name
FROM `{VIEW_ROLE}`
WHERE login_email = @login_email
LIMIT 1
"""
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
        # ★ロールが取れない＝事故防止で SALES 扱い（全社を見せない）
        return RoleInfo(
            login_email=login_email,
            role_key="SALES",
            role_admin_view=False,
            role_admin_edit=False,
            role_sales_view=True,
            area_name="未設定",
        )

    r = df.iloc[0].to_dict()
    role_key = normalize_role_key(str(r.get("role_key", "SALES")))
    return RoleInfo(
        login_email=login_email,
        role_key=role_key,
        role_admin_view=bool(r.get("role_admin_view", False)),
        role_admin_edit=bool(r.get("role_admin_edit", False)),
        role_sales_view=bool(r.get("role_sales_view", True)),
        area_name=str(r.get("area_name", "未設定")),
    )


def render_health_check(client: bigquery.Client, cache_key: Tuple[str, str, str], use_bqstorage: bool, timeout_sec: int):
    st.sidebar.divider()
    if st.sidebar.button("ヘルスチェック (SELECT 1)"):
        df = query_df_safe(
            client,
            "SELECT 1 AS ok",
            params=None,
            label="SELECT 1 ヘルスチェック",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        if not df.empty:
            st.sidebar.success("BigQuery: OK")
        else:
            st.sidebar.error("BigQuery: NG")


def run_scoped_then_fallback(
    title: str,
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    table_fqn: str,
    scope_col: str,
    login_email: str,
    allow_org_fallback: bool,
    use_bqstorage: bool,
    timeout_sec: int,
    show_sql: bool,
) -> pd.DataFrame:
    # 1) scoped
    sql1 = f"""
SELECT *
FROM `{table_fqn}`
WHERE {scope_col} = @login_email
LIMIT 2000
"""
    if show_sql:
        st.code(sql1.strip(), language="sql")

    df = query_df_safe(
        client,
        sql1,
        params={"login_email": login_email},
        label=title,
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )
    st.caption(f"取得件数: {len(df)} 件（scope: {scope_col}=@login_email）")
    if not df.empty:
        return df

    # 2) viewer_email系の all fallback（ただし許可された場合のみ）
    if allow_org_fallback and scope_col in ("viewer_email", "viewer_mail", "viewer"):
        st.warning("0件でした。viewer_email='all' の全社fallbackを試します。")
        sql2 = f"""
SELECT *
FROM `{table_fqn}`
WHERE {scope_col} = "all"
LIMIT 2000
"""
        if show_sql:
            st.code(sql2.strip(), language="sql")

        df2 = query_df_safe(
            client,
            sql2,
            params=None,
            label=title + "（fallback all）",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        st.caption(f"取得件数: {len(df2)} 件（fallback all）")
        if not df2.empty:
            return df2

    # 3) WHERE無し fallback（管理者のみ許可）
    if allow_org_fallback:
        st.warning("それでも0件です。WHEREを外して全社表示（管理者fallback）を試します。")
        sql3 = f"SELECT * FROM `{table_fqn}` LIMIT 2000"
        if show_sql:
            st.code(sql3.strip(), language="sql")

        df3 = query_df_safe(
            client,
            sql3,
            params=None,
            label=title + "（fallback no-filter）",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
            cache_key=cache_key,
        )
        st.caption(f"取得件数: {len(df3)} 件（fallback no-filter）")
        return df3

    return pd.DataFrame()


# ----------------------------------------
# UI Components
# ----------------------------------------
def render_fytd_org_section(
    client: bigquery.Client, cache_key: Any, login_email: str, opts: Dict[str, Any]
):
    st.subheader("🏢 年度累計（FYTD）｜全社")
    if st.button("全社FYTDを読み込む", key="btn_fytd_org", use_container_width=True):
        df_org = run_scoped_then_fallback(
            title="全社FYTD",
            client=client,
            cache_key=cache_key,
            table_fqn=VIEW_FYTD_ORG,
            scope_col="viewer_email",
            login_email=login_email,
            allow_org_fallback=True,  # 全社タブなのでfallback許可
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
        )
        df_org = rename_columns_for_display(df_org, JP_COLS_FYTD)
        if df_org.empty:
            st.info("全社FYTDは0件です。")
        else:
            st.dataframe(df_org, use_container_width=True)


def render_fytd_me_section(
    client: bigquery.Client, cache_key: Any, login_email: str, opts: Dict[str, Any]
):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分FYTDを読み込む", key="btn_fytd_me", use_container_width=True):
        df_me = run_scoped_then_fallback(
            title="自分FYTD",
            client=client,
            cache_key=cache_key,
            table_fqn=VIEW_FYTD_ME,      # ★v_staff_fytd_summary_scoped
            scope_col="login_email",     # ★login_email
            login_email=login_email,
            allow_org_fallback=False,    # ★自分用はfallback禁止
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            show_sql=opts["show_sql"],
        )
        df_me = rename_columns_for_display(df_me, JP_COLS_FYTD)
        if df_me.empty:
            st.warning("自分FYTDが0件です。")
        else:
            st.dataframe(df_me, use_container_width=True)


def render_yoy_section(
    client: bigquery.Client, cache_key: Any, login_email: str, allow_org_fallback: bool, opts: Dict[str, Any]
):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        if st.button("YoY Top", key="btn_yoy_top", use_container_width=True):
            df = run_scoped_then_fallback(
                title="YoY Top",
                client=client,
                cache_key=cache_key,
                table_fqn=VIEW_YOY_TOP,
                scope_col="login_email",
                login_email=login_email,
                allow_org_fallback=allow_org_fallback,
                use_bqstorage=opts["use_bqstorage"],
                timeout_sec=opts["timeout_sec"],
                show_sql=opts["show_sql"],
            )
            df = rename_columns_for_display(df, JP_COLS_YOY)
            if df.empty:
                st.info("0件です。")
            else:
                st.dataframe(df, use_container_width=True)
    with c2:
        if st.button("YoY Bottom", key="btn_yoy_btm", use_container_width=True):
            df = run_scoped_then_fallback(
                title="YoY Bottom",
                client=client,
                cache_key=cache_key,
                table_fqn=VIEW_YOY_BOTTOM,
                scope_col="login_email",
                login_email=login_email,
                allow_org_fallback=allow_org_fallback,
                use_bqstorage=opts["use_bqstorage"],
                timeout_sec=opts["timeout_sec"],
                show_sql=opts["show_sql"],
            )
            df = rename_columns_for_display(df, JP_COLS_YOY)
            if df.empty:
                st.info("0件です。")
            else:
                st.dataframe(df, use_container_width=True)
    with c3:
        if st.button("YoY 比較不能", key="btn_yoy_unc", use_container_width=True):
            df = run_scoped_then_fallback(
                title="YoY Uncomparable",
                client=client,
                cache_key=cache_key,
                table_fqn=VIEW_YOY_UNCOMP,
                scope_col="login_email",
                login_email=login_email,
                allow_org_fallback=allow_org_fallback,
                use_bqstorage=opts["use_bqstorage"],
                timeout_sec=opts["timeout_sec"],
                show_sql=opts["show_sql"],
            )
            df = rename_columns_for_display(df, JP_COLS_YOY)
            if df.empty:
                st.info("0件です。")
            else:
                st.dataframe(df, use_container_width=True)


# ----------------------------------------
# Main
# ----------------------------------------
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

    role = resolve_role(client, cache_key, login_email, use_bqstorage=opts["use_bqstorage"], timeout_sec=opts["timeout_sec"])

    st.subheader("ログイン情報")
    st.write(f"**ログイン:** {role.login_email}")
    st.write(f"**ロール:** {role.role_key} (エリア: {role.area_name})")

    allow_org_fallback = role.role_key in ("HQ_ADMIN", "AREA_MANAGER")
    
    st.divider()
    
    # -----------------------------
    # ロール別 タブ表示切り替え
    # -----------------------------
    if allow_org_fallback:
        # 管理者系: [全社] [自分の担当]
        t1, t2 = st.tabs(["🏢 全社状況 (経営)", "👤 担当エリア/個人の成績 (行動)"])
        
        with t1:
            render_fytd_org_section(client, cache_key, login_email, opts)
            
        with t2:
            render_fytd_me_section(client, cache_key, login_email, opts)
            st.divider()
            render_yoy_section(client, cache_key, login_email, allow_org_fallback, opts)
            
    else:
        # SALES: [今年の成績] [得意先別] (全社は見せない)
        t1, t2 = st.tabs(["👤 今年の成績 (FYTD)", "📊 得意先別 (YoY)"])
        
        with t1:
            render_fytd_me_section(client, cache_key, login_email, opts)
            
        with t2:
            render_yoy_section(client, cache_key, login_email, allow_org_fallback, opts)

    st.caption("※ VIEWを置き換えた直後に表示がズレる場合は『キャッシュをクリア（cache_data）』→再読込してください。")


if __name__ == "__main__":
    main()
