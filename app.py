# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.9.8 (Master Integrated / High Performance)

【更新履歴 v1.9.8】
- [Integration] 担当者台帳(sales_staff_master)をマスターとして統合。
- [Feature] ログインユーザーの「電話番号」表示およびメールアドレスベースの権限判定を実装。
- [Structure] v1.9.7の動的SQLアーキテクチャを完全踏襲。

★今回の更新提案（踏襲のうえでの確定アップデート）
- [Security] ランキング/ドリルダウン（VIEW_UNIFIED直集計）にも権限スコープを強制注入（漏洩防止）
- [Stability] fiscal_year の固定値（2025/2024）を全廃し、MAX(fiscal_year) から動的に計算
- [Master] sales_staff_master に area_name 列が無い場合でも動作するフォールバック（role文字列→エリア推定は任意）
- [Recommendation] v_sales_recommendation_engine の列名差異に耐える（存在列だけ表示）
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, GoogleAPICallError

# -----------------------------
# 1. Configuration
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

APP_URL = "https://sfa-premium-app-2.streamlit.app/"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# 統合View（分析の土台）
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"

# KPI/ランキング既存View
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"

# 戦略提案
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"
VIEW_ITEM_MASTER = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.vw_item_master_norm"

# 担当者台帳（スプレッドシート連携テーブル）
VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.sales_staff_master"

# 除外JAN
NOISE_JAN_SQL = "('0', '22221', '99998', '33334')"

# -----------------------------
# 2. Helpers (Display)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.9.8 (Master Integrated)｜BigQuery集計・動的SQL版｜RBAC注入済み")

def get_qr_code_url(url: str) -> str:
    return f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={url}"

def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差", "実績", "予測", "GAP"]):
            config[col] = st.column_config.NumberColumn(col, format="¥%d")
        elif any(k in col for k in ["率", "比", "ペース"]):
            config[col] = st.column_config.NumberColumn(col, format="%.1f%%")
        elif is_numeric_dtype(df[col]):
            config[col] = st.column_config.NumberColumn(col, format="%d")
        else:
            config[col] = st.column_config.TextColumn(col)
    return config

def get_safe_float(row: pd.Series, key: str) -> float:
    val = row.get(key)
    if pd.isna(val):
        return 0.0
    try:
        return float(val)
    except Exception:
        return 0.0

JP_COLS_FYTD = {
    "login_email": "ログインメール",
    "display_name": "担当者名",
    "sales_amount_fytd": "売上（FYTD）",
    "gross_profit_fytd": "粗利（FYTD）",
    "sales_amount_py_total": "前年売上実績（年）",
    "sales_forecast_total": "売上着地予測（年）",
    "gross_profit_py_total": "前年粗利実績（年）",
    "gp_forecast_total": "粗利着地予測（年）",
}
JP_COLS_YOY = {
    "customer_code": "得意先コード",
    "customer_name": "得意先名",
    "sales_amount": "売上（当月）",
    "gross_profit": "粗利（当月）",
    "sales_amount_py": "売上（前年同月）",
    "sales_diff_yoy": "前年差（売上）",
}

# -----------------------------
# 3. BigQuery Connection
# -----------------------------
def setup_bigquery_client() -> Tuple[bigquery.Client, str, str, str]:
    if "bigquery" not in st.secrets:
        st.error("❌ Secrets設定が見つかりません。")
        st.stop()
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    return client, project_id, location, json.dumps(sa, ensure_ascii=False, sort_keys=True)

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

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SEC)
def cached_query_df(project_id: str, location: str, sa_json: str, sql: str, params_json: str, use_bqstorage: bool, timeout_sec: int) -> pd.DataFrame:
    sa = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)

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
            qparams = _build_query_parameters(params)
            if qparams:
                job_config.query_parameters = qparams
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
            return job.to_dataframe(create_bqstorage_client=use_bqstorage)

        project_id, location, sa_json = cache_key
        return cached_query_df(project_id, location, sa_json, sql, params_json, use_bqstorage, timeout_sec)

    except (BadRequest, GoogleAPICallError) as e:
        st.error(f"Query Failed: {label}\n{e}")
        st.code(sql, language="sql")
        if params:
            st.code(json.dumps(params, ensure_ascii=False, indent=2), language="json")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Query Failed: {label}\n{e}")
        st.code(sql, language="sql")
        if params:
            st.code(json.dumps(params, ensure_ascii=False, indent=2), language="json")
        return pd.DataFrame()

# -----------------------------
# 3.5 Role / RBAC
# -----------------------------
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    staff_name: str = "ゲスト"
    role_key: str = "SALES"          # HQ_ADMIN / SALES （最小）
    role_admin_view: bool = False    # Trueなら全社/エリアの閲覧を許可
    phone: str = "-"
    area_name: str = "未設定"        # 本来は台帳の area_name 列を正とする

def _normalize_role_key(role_key: str) -> str:
    rk = (role_key or "").strip().upper()
    if rk in ("HQ_ADMIN", "AREA_MANAGER", "SALES"):
        return rk
    return "SALES"

def _infer_area_from_role_text(raw_role: str) -> str:
    """
    フォールバック用：role文字列に「熊本」「大分」等が含まれていれば拾う。
    ※ 正本は sales_staff_master の area_name 列。
    """
    r = (raw_role or "").upper()
    for a in ["熊本", "大分", "福岡", "久留米", "八女", "柳川", "大牟田", "みやま", "本社", "HQ"]:
        if a.upper() in r:
            return a
    return "未設定"

def resolve_role(client: bigquery.Client, cache_key: Tuple[str, str, str], login_email: str, use_bqstorage: bool, timeout_sec: int) -> RoleInfo:
    # area_name 列が存在しない可能性に備え、まず INFORMATION_SCHEMA で列存在チェック
    sql_cols = f"""
    SELECT column_name
    FROM `{PROJECT_DEFAULT}.{DATASET_DEFAULT}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sales_staff_master'
    """
    df_cols = query_df_safe(client, sql_cols, None, "Role Columns", use_bqstorage, timeout_sec, cache_key)
    colset = set(df_cols["column_name"].astype(str).tolist()) if not df_cols.empty else set()

    has_area = "area_name" in colset
    has_role_key = "role_key" in colset  # もし台帳に role_key を持たせた場合

    select_cols = ["email", "staff_name", "role", "phone"]
    if has_area:
        select_cols.append("area_name")
    if has_role_key:
        select_cols.append("role_key")

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM `{VIEW_ROLE}`
    WHERE email = @login_email
    LIMIT 1
    """
    df = query_df_safe(
        client,
        sql,
        {"login_email": login_email},
        "Role Check",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )

    if df.empty:
        return RoleInfo(login_email=login_email)

    r = df.iloc[0].to_dict()
    raw_role = str(r.get("role", "")).strip()
    raw_role_up = raw_role.upper()

    # 管理者判定（あなたの現行ロジック踏襲）
    is_admin = any(x in raw_role_up for x in ["ADMIN", "MANAGER", "HQ", "統括", "本社"])
    role_key = _normalize_role_key(str(r.get("role_key", "HQ_ADMIN" if is_admin else "SALES")))

    # area_name は台帳の列を正とする。無ければ role文字列から推定（暫定）
    area_name = str(r.get("area_name", "")).strip() if has_area else ""
    if not area_name:
        area_name = _infer_area_from_role_text(raw_role)

    return RoleInfo(
        login_email=login_email,
        staff_name=str(r.get("staff_name", "不明")),
        role_key=role_key,
        role_admin_view=bool(is_admin),
        phone=str(r.get("phone", "-")),
        area_name=area_name or "未設定",
    )

def get_scope_filter_sql(role: RoleInfo) -> Tuple[str, Dict[str, Any]]:
    """
    ★重要：VIEW_UNIFIED（直集計）へ注入するRBAC条件
    - HQ_ADMIN: 全社
    - それ以外: 原則 login_email
    - もし VIEW_UNIFIED に area_name 列があり、role.area_name が有効ならエリアスコープも可能
      （列有無はここでは判定せずSQL側でエラーになるので、デフォルトは login_email を推奨）
    """
    if role.role_key == "HQ_ADMIN" or role.role_admin_view:
        return "1=1", {}
    return "login_email = @login_email", {"login_email": role.login_email}

# -----------------------------
# 4. BigQuery Calculation Logic (RBAC注入)
# -----------------------------
def fetch_ranking_from_bq(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    ranking_type: str,
    axis_mode: str,
    is_sales_mode: bool,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    is_worst = (ranking_type == "worst")
    is_product = (axis_mode == "product")
    group_col = "product_name" if is_product else "customer_name"
    target_val = "sales_amount" if is_sales_mode else "gross_profit"
    order_dir = "ASC" if is_worst else "DESC"

    scope_sql, scope_params = get_scope_filter_sql(role)

    sql = f"""
    WITH base_stats AS (
        SELECT MAX(fiscal_year) AS current_fy FROM `{VIEW_UNIFIED}`
    )
    SELECT
        {group_col} AS name,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) AS sales_cur,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) AS gp_cur,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_prev,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN {target_val} ELSE 0 END) -
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN {target_val} ELSE 0 END) AS diff_val,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) -
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_diff,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) -
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN gross_profit ELSE 0 END) AS gp_diff
    FROM `{VIEW_UNIFIED}`
    WHERE
        ({scope_sql})
        AND jan_code NOT IN {NOISE_JAN_SQL}
        AND jan_code NOT LIKE '999%'
        AND LENGTH(jan_code) > 5
    GROUP BY {group_col}
    HAVING (sales_cur > 0 OR sales_prev > 0)
    ORDER BY diff_val {order_dir}
    LIMIT 1000
    """
    return query_df_safe(
        client,
        sql,
        scope_params,
        "Ranking Query",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )

def fetch_drilldown_from_bq(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    key_col: str,
    key_val: str,
    target_col: str,
    is_worst: bool,
    is_sales_mode: bool,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    order_dir = "ASC" if is_worst else "DESC"
    sort_col_alias = "売上差額" if is_sales_mode else "粗利差額"
    target_label = "得意先名" if target_col == "customer_name" else "商品名"

    scope_sql, scope_params = get_scope_filter_sql(role)

    sql = f"""
    WITH base_stats AS (
        SELECT MAX(fiscal_year) AS current_fy FROM `{VIEW_UNIFIED}`
    )
    SELECT
        {target_col} AS `{target_label}`,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) AS `今年売上`,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS `前年売上`,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) -
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS `売上差額`,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) AS `今年粗利`,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) -
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN gross_profit ELSE 0 END) AS `粗利差額`
    FROM `{VIEW_UNIFIED}`
    WHERE
        ({scope_sql})
        AND {key_col} = @key_val
    GROUP BY 1
    ORDER BY `{sort_col_alias}` {order_dir}
    LIMIT 500
    """
    params = dict(scope_params)
    params["key_val"] = key_val

    return query_df_safe(
        client,
        sql,
        params,
        "Drilldown Query",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
        cache_key=cache_key,
    )

def run_scoped_query(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    sql_template: str,
    scope_col: str,
    login_email: str,
    allow_fallback: bool,
    use_bqstorage: bool,
    timeout_sec: int,
    label: str,
):
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    df = query_df_safe(
        client, sql, {"login_email": login_email},
        label, use_bqstorage, timeout_sec, cache_key
    )
    if not df.empty:
        return df

    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        return query_df_safe(
            client, sql_all, None,
            label + "（fallback）", use_bqstorage, timeout_sec, cache_key
        )
    return pd.DataFrame()

# -----------------------------
# 5. UI Layout
# -----------------------------
def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.image(get_qr_code_url(APP_URL), caption="📱スマホでアクセス", width=150)
    st.sidebar.divider()

    use_bqstorage = st.sidebar.toggle("BigQuery Storage API（高速）", value=True)
    timeout_sec = st.sidebar.slider("クエリタイムアウト（秒）", 10, 300, 60, 10)
    show_sql = st.sidebar.toggle("SQL表示（デバッグ）", value=False)

    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.sidebar.success("Cache Cleared.")
    return {"use_bqstorage": use_bqstorage, "timeout_sec": timeout_sec, "show_sql": show_sql}

def get_login_email_ui() -> str:
    st.sidebar.header("Login Simulation")
    default = st.secrets.get("default_login_email", "") if "default_login_email" in st.secrets else ""
    return st.sidebar.text_input("Login Email", value=default).strip()

def render_interactive_ranking_matrix(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    ranking_type: str,
    axis_mode: str,
    is_sales_mode: bool,
    opts: Dict[str, Any],
):
    is_worst = (ranking_type == "worst")
    is_product = (axis_mode == "product")
    label_col = "商品名" if is_product else "得意先名"
    mode_label = "売上" if is_sales_mode else "粗利"

    df_rank = fetch_ranking_from_bq(
        client=client,
        cache_key=cache_key,
        role=role,
        ranking_type=ranking_type,
        axis_mode=axis_mode,
        is_sales_mode=is_sales_mode,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
    )
    if df_rank.empty:
        st.info("データがありません。")
        return

    df_disp = df_rank.rename(
        columns={
            "name": label_col,
            "sales_cur": "今年売上",
            "sales_prev": "前年売上",
            "sales_diff": "売上差額",
            "gp_cur": "今年粗利",
            "gp_diff": "粗利差額",
        }
    )

    if is_sales_mode:
        cols = [label_col, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
    else:
        cols = [label_col, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]

    st.markdown(f"##### ① {label_col}を選択 ({mode_label}ベース)")
    st.caption(f"※{mode_label}の増減額が大きい順（計算: BigQuery / RBAC注入済み）")

    key_suffix = f"{ranking_type}_{axis_mode}_{mode_label}"
    event = st.dataframe(
        df_disp[cols],
        use_container_width=True,
        hide_index=True,
        column_config=create_default_column_config(df_disp),
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        key=f"t1_{key_suffix}",
    )

    if len(event.selection["rows"]) > 0:
        idx = event.selection["rows"][0]
        selected_val = df_disp.iloc[idx][label_col]

        st.divider()
        st.subheader(f"🔎 内訳分析: {selected_val}")

        key_col = "product_name" if is_product else "customer_name"
        target_col = "customer_name" if is_product else "product_name"

        df_drill = fetch_drilldown_from_bq(
            client=client,
            cache_key=cache_key,
            role=role,
            key_col=key_col,
            key_val=str(selected_val),
            target_col=target_col,
            is_worst=is_worst,
            is_sales_mode=is_sales_mode,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
        )
        if df_drill.empty:
            st.warning("詳細データなし")
        else:
            drill_label = "得意先名" if is_product else "商品名"
            if is_sales_mode:
                d_cols = [drill_label, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
            else:
                d_cols = [drill_label, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]

            st.dataframe(
                df_drill[d_cols],
                use_container_width=True,
                hide_index=True,
                column_config=create_default_column_config(df_drill),
                key=f"t2_{key_suffix}",
            )

def render_fytd_org_section(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    opts: Dict[str, Any],
):
    st.subheader("🏢 年度累計（FYTD）｜全社")

    if st.button("全社データを読み込む", key="btn_org_load", use_container_width=True):
        st.session_state.org_data_loaded = True

    if not st.session_state.org_data_loaded:
        st.info("👆 上のボタンを押して全社データを読み込んでください")
        return

    sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
    df_org = run_scoped_query(
        client=client,
        cache_key=cache_key,
        sql_template=sql_kpi,
        scope_col="viewer_email",
        login_email=role.login_email,
        allow_fallback=True,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
        label="ORG KPI",
    )

    if not df_org.empty:
        row = df_org.iloc[0]
        s_cur = get_safe_float(row, "sales_amount_fytd")
        s_py = get_safe_float(row, "sales_amount_py_total")
        s_fc = get_safe_float(row, "sales_forecast_total")
        gp_cur = get_safe_float(row, "gross_profit_fytd")
        gp_py = get_safe_float(row, "gross_profit_py_total")
        gp_fc = get_safe_float(row, "gp_forecast_total")

        st.markdown("##### ■ 売上 (Sales)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("① 現状", f"¥{s_cur:,.0f}")
        c2.metric("② 昨年", f"¥{s_py:,.0f}")
        c3.metric("③ 予測", f"¥{s_fc:,.0f}")
        c4.metric("④ GAP", f"¥{s_fc - s_py:,.0f}", delta_color="off")

        st.markdown("##### ■ 粗利 (Gross Profit)")
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("① 現状", f"¥{gp_cur:,.0f}")
        c6.metric("② 昨年", f"¥{gp_py:,.0f}")
        c7.metric("③ 予測", f"¥{gp_fc:,.0f}")
        c8.metric("④ GAP", f"¥{gp_fc - gp_py:,.0f}", delta_color="off")
        st.divider()

    st.subheader("📊 増減要因分析 (多次元)")
    c_axis, c_val = st.columns(2)
    with c_axis:
        axis_sel = st.radio("集計軸:", ["📦 商品軸", "🏥 得意先軸"], horizontal=True)
        axis_mode = "product" if "商品" in axis_sel else "customer"
    with c_val:
        val_sel = st.radio("評価指標:", ["💰 売上金額", "💹 粗利金額"], horizontal=True)
        is_sales_mode = "売上" in val_sel

    tab_worst, tab_best = st.tabs(["📉 ワースト (減)", "📈 ベスト (増)"])
    with tab_worst:
        render_interactive_ranking_matrix(client, cache_key, role, "worst", axis_mode, is_sales_mode, opts)
    with tab_best:
        render_interactive_ranking_matrix(client, cache_key, role, "best", axis_mode, is_sales_mode, opts)

def render_fytd_me_section(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    opts: Dict[str, Any],
):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(
            client=client,
            cache_key=cache_key,
            sql_template=sql,
            scope_col="login_email",
            login_email=role.login_email,
            allow_fallback=False,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            label="ME KPI",
        )
        if df_me.empty:
            st.info("0件です。")
            return

        df_disp = rename_columns_for_display(df_me, JP_COLS_FYTD)
        cols = list(df_disp.columns)
        if "担当者名" in cols:
            cols.remove("担当者名")
            cols.insert(0, "担当者名")
        st.dataframe(
            df_disp[cols],
            use_container_width=True,
            hide_index=True,
            column_config=create_default_column_config(df_disp[cols]),
        )

def render_yoy_section(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    allow_fallback: bool,
    opts: Dict[str, Any],
):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)

    def _show_table(title: str, view_name: str, key: str):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(
                client=client,
                cache_key=cache_key,
                sql_template=sql,
                scope_col="login_email",
                login_email=role.login_email,
                allow_fallback=allow_fallback,
                use_bqstorage=opts["use_bqstorage"],
                timeout_sec=opts["timeout_sec"],
                label=title,
            )
            if df.empty:
                st.info("0件です。")
                return
            df_disp = rename_columns_for_display(df, JP_COLS_YOY)
            st.dataframe(df_disp, use_container_width=True, hide_index=True)

    with c1:
        _show_table("YoY Top (伸び)", VIEW_YOY_TOP, "btn_top")
    with c2:
        _show_table("YoY Bottom (落ち)", VIEW_YOY_BOTTOM, "btn_btm")
    with c3:
        _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")

def _pick_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def render_customer_drilldown(
    client: bigquery.Client,
    cache_key: Tuple[str, str, str],
    role: RoleInfo,
    opts: Dict[str, Any],
):
    st.subheader("🎯 得意先別・戦略提案")

    # SALES: 自分の得意先のみ / ADMIN: まず自分の得意先（必要なら将来スコープ拡張）
    sql_cust = f"""
    SELECT DISTINCT customer_code, customer_name
    FROM `{VIEW_FACT_DAILY}`
    WHERE login_email = @login_email
    ORDER BY customer_code
    """
    df_cust = query_df_safe(
        client, sql_cust, {"login_email": role.login_email},
        "Cust List",
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
        cache_key=cache_key,
    )
    if df_cust.empty:
        st.info("得意先リストが0件です。")
        return

    cust_options = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in df_cust.iterrows()}
    selected_code = st.selectbox(
        "分析する得意先を選択してください",
        options=list(cust_options.keys()),
        format_func=lambda x: cust_options[x],
    )
    if not selected_code:
        return

    st.divider()

    # 推奨ビュー（列名差異に耐える）
    sql_rec = f"SELECT * FROM `{VIEW_RECOMMEND}` WHERE customer_code = @cust_code"
    df_rec = query_df_safe(
        client, sql_rec, {"cust_code": selected_code},
        "Recommendation",
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
        cache_key=cache_key,
    )
    # 並び順（priorityが無ければそのまま）
    pr_col = _pick_first_existing_column(df_rec, ["priority_rank", "category_rank", "rank", "priority"])
    if df_rec is not None and not df_rec.empty and pr_col:
        df_rec = df_rec.sort_values(pr_col, ascending=True)

    c1, c2 = st.columns([1, 2])

    with c1:
        st.markdown("#### 🏥 プロファイル")
        strong_col = _pick_first_existing_column(df_rec, ["strong_category", "targeted_category", "main_category", "target_category"])
        strong = "-"
        if df_rec is not None and not df_rec.empty and strong_col:
            strong = str(df_rec.iloc[0].get(strong_col, "-"))
        st.info(f"主力領域: **{strong}**")

    with c2:
        st.markdown("#### 💡 AI提案リスト")

        if df_rec is None or df_rec.empty:
            st.info("提案データはありません。")
        else:
            # 表示列を安全に選ぶ
            col_rank = _pick_first_existing_column(df_rec, ["priority_rank", "category_rank", "rank", "priority"])
            col_prod = _pick_first_existing_column(df_rec, ["recommend_product", "product_name", "recommend_name"])
            col_manu = _pick_first_existing_column(df_rec, ["manufacturer", "maker_name", "maker", "maker"])
            col_scale = _pick_first_existing_column(df_rec, ["market_scale", "total_sales", "total_sales_amount", "total_sales_yen"])

            disp_cols = [c for c in [col_rank, col_prod, col_manu, col_scale] if c]
            disp = df_rec[disp_cols].copy()

            rename_map = {}
            if col_rank: rename_map[col_rank] = "順位"
            if col_prod: rename_map[col_prod] = "商品"
            if col_manu: rename_map[col_manu] = "メーカー"
            if col_scale: rename_map[col_scale] = "規模"
            disp = disp.rename(columns=rename_map)

            st.dataframe(disp, use_container_width=True, hide_index=True, column_config=create_default_column_config(disp))

    with st.expander("参考: 現在の採用品リストを見る"):
        # FYは動的（MAX fiscal_year）
        sql_adopted = f"""
        WITH base_stats AS (SELECT MAX(fiscal_year) AS current_fy FROM `{VIEW_FACT_DAILY}`)
        SELECT
          m.product_name,
          SUM(CASE WHEN t.fiscal_year = (SELECT current_fy FROM base_stats) THEN t.sales_amount ELSE 0 END) AS sales_fytd,
          SUM(CASE WHEN t.fiscal_year = (SELECT current_fy FROM base_stats) THEN t.gross_profit ELSE 0 END) AS gp_fytd
        FROM `{VIEW_FACT_DAILY}` t
        LEFT JOIN `{VIEW_ITEM_MASTER}` m
          ON CAST(t.jan AS STRING) = CAST(m.jan_code AS STRING)
        WHERE t.customer_code = @cust_code
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 100
        """
        df_adopted = query_df_safe(
            client, sql_adopted, {"cust_code": selected_code},
            "Adopted List",
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
            cache_key=cache_key,
        )
        if df_adopted.empty:
            st.info("採用品が0件です。")
        else:
            renamed = df_adopted.rename(columns={"product_name": "商品名", "sales_fytd": "売上(FY)", "gp_fytd": "粗利(FY)"})
            st.dataframe(renamed, use_container_width=True, hide_index=True, column_config=create_default_column_config(renamed))

# -----------------------------
# 6. Main
# -----------------------------
def main():
    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    set_page()

    client, project_id, location, sa_json = setup_bigquery_client()
    cache_key = (project_id, location, sa_json)

    opts = sidebar_controls()
    login_email = get_login_email_ui()

    if not login_email:
        st.info("👈 サイドバーに Login Email を入力してください。")
        st.stop()

    st.divider()

    # 権限解決（sales_staff_master）
    role = resolve_role(
        client=client,
        cache_key=cache_key,
        login_email=login_email,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
    )

    # ログイン表示（名前・電話・権限）
    st.write(f"👤 **担当:** {role.staff_name}")
    st.write(f"📧 **Email:** {role.login_email}")
    st.write(f"🛡️ **Role:** {role.role_key}")
    st.write(f"🗺️ **Area:** {role.area_name}")
    phone_tail = str(role.phone).replace("-", "").strip()[-4:] if role.phone else "----"
    st.write(f"📞 **Phone:** ***-****-{phone_tail}")
    st.divider()

    allow_org_fallback = bool(role.role_admin_view or role.role_key == "HQ_ADMIN")

    if allow_org_fallback:
        tabs = st.tabs(["🏢 組織/エリア状況", "👤 個人成績", "🎯 戦略提案"])
        with tabs[0]:
            render_fytd_org_section(client, cache_key, role, opts)
        with tabs[1]:
            render_fytd_me_section(client, cache_key, role, opts)
            st.divider()
            render_yoy_section(client, cache_key, role, allow_fallback=True, opts=opts)
        with tabs[2]:
            render_customer_drilldown(client, cache_key, role, opts)
    else:
        tabs = st.tabs(["👤 今年の成績", "📊 得意先分析", "🎯 提案を作る"])
        with tabs[0]:
            render_fytd_me_section(client, cache_key, role, opts)
        with tabs[1]:
            render_yoy_section(client, cache_key, role, allow_fallback=False, opts=opts)
        with tabs[2]:
            render_customer_drilldown(client, cache_key, role, opts)

    st.caption("※ VIEW差し替え直後にズレる場合：Clear Cache → 再読込")

if __name__ == "__main__":
    main()
