# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.9.9 (RBAC + Staff Master BQ Fallback / No Drive Creds)

【この版の狙い（403 Drive credentials を確実に潰す）】
- Role参照は「BASE TABLE」を最優先（sales_staff_master_bq）に固定
- 互換のため、BQ側で VIEW `sales_staff_master` を BASE TABLE参照にしてもOK
- どうしても EXTERNAL(Sheets) を踏まないように設計（Drive credentials を取りに行かない）
- v1.9.8 の機能（FYTD/YoY/多次元増減要因/得意先ドリル/提案）を踏襲

前提：
- `salesdb-479915.sales_data.sales_staff_master_bq` が BASE TABLE として存在（GAS同期後）
- `v_sales_fact_unified` / `v_admin_org_fytd_summary_scoped` / `v_staff_fytd_summary_scoped`
  / `v_sales_customer_yoy_*` / `v_sales_fact_login_jan_daily` / `v_sales_recommendation_engine` が存在
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

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

# ★分析の土台となる統合View
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"

# KPIカード用などの既存View
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"

# 戦略提案（v1.6.0〜）
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"
VIEW_ITEM_MASTER = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.vw_item_master_norm"

# ★重要：Role/Staff Master は BASE TABLE を参照（403を確実に回避）
#   - ここは「必ず BASE TABLE」を指す
TABLE_STAFF_MASTER_BQ = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.sales_staff_master_bq"
#   - 互換のために VIEW を使う場合は、BQ側でこのVIEWが BASE TABLE参照であること（EXTERNAL不可）
VIEW_STAFF_MASTER = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.sales_staff_master"

# 除外コード定義
NOISE_JAN_SQL = "('0', '22221', '99998', '33334')"


# -----------------------------
# 2. Helpers (Display)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.9.9｜RBAC + Staff Master(BQ)｜BigQuery集計・動的SQL版（403回避）")


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
# 3. BigQuery Connection / Query
# -----------------------------
def setup_bigquery_client() -> Tuple[bigquery.Client, str, str, str]:
    if "bigquery" not in st.secrets:
        st.error("❌ Secrets設定が見つかりません。")
        st.stop()

    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id") or PROJECT_DEFAULT)
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))

    # ★Drive credentials 系のエラー回避：
    # - EXTERNAL（Sheets）を踏まなければ drive スコープ自体は不要
    # - ただし、今後 EXTERNAL を使う可能性があるなら scopes を付与しても良い
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        # 必要な場合のみ有効化（EXTERNAL参照を使う時）
        # "https://www.googleapis.com/auth/drive.readonly",
        # "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]

    creds = service_account.Credentials.from_service_account_info(sa, scopes=scopes)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    return client, project_id, location, json.dumps(sa, ensure_ascii=False)


def _build_qparams(params: Optional[Dict[str, Any]]) -> list:
    qparams = []
    if not params:
        return qparams
    for k, v in params.items():
        if isinstance(v, bool):
            qparams.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
        elif isinstance(v, int):
            qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        elif isinstance(v, float):
            qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
        else:
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return qparams


def query_df_safe(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    use_bqstorage: bool = True,
    timeout_sec: int = 60,
) -> pd.DataFrame:
    try:
        job_config = bigquery.QueryJobConfig()
        qparams = _build_qparams(params)
        if qparams:
            job_config.query_parameters = qparams
        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except (BadRequest, GoogleAPICallError, Exception) as e:
        st.error(f"Query Failed: {label}\n{e}")
        with st.expander("詳細（SQL / params）", expanded=False):
            st.code(sql.strip(), language="sql")
            st.json(params or {})
        return pd.DataFrame()


# -----------------------------
# 4. Role / RBAC (Master Integrated)
# -----------------------------
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    staff_name: str = "ゲスト"
    role_key: str = "SALES"  # HQ_ADMIN / AREA_MANAGER / SALES
    role_admin_view: bool = False
    phone: str = "-"
    area_name: str = "未設定"


def _normalize_role(raw_role: str) -> Tuple[str, bool, str]:
    """
    raw_role 例:
      - "HQ_ADMIN"
      - "AREA_MANAGER（熊本）"
      - "SALES（大分）"
      - "ADMIN"
    """
    rr = (raw_role or "").strip().upper()
    is_admin = any(x in rr for x in ["ADMIN", "MANAGER", "HQ", "統括"])
    role_key = "HQ_ADMIN" if is_admin else "SALES"

    # エリア抽出（括弧があれば中身、なければ role文字列そのものを area のヒントに）
    area = "未設定"
    if "（" in rr and "）" in rr:
        try:
            area = rr.split("（", 1)[1].split("）", 1)[0].strip() or "未設定"
        except Exception:
            area = "未設定"
    else:
        # 例: "KUMAMOTO" などが混じる場合に備え、そのまま残す
        area = rr if rr else "未設定"

    return role_key, is_admin, area


def resolve_role(client: bigquery.Client, login_email: str, use_bqstorage: bool, timeout_sec: int) -> RoleInfo:
    """
    ★最優先で BASE TABLE(sales_staff_master_bq) を参照
    互換VIEW(sales_staff_master)は、BQ側で BASE TABLE参照になっている場合のみ使うこと。
    """
    login_email = (login_email or "").strip().lower()
    if not login_email:
        return RoleInfo(login_email="")

    sql_bq = f"""
SELECT
  email,
  staff_name,
  role,
  phone
FROM `{TABLE_STAFF_MASTER_BQ}`
WHERE LOWER(email) = @login_email
LIMIT 1
"""
    df = query_df_safe(
        client,
        sql_bq,
        params={"login_email": login_email},
        label="Role Check (BASE TABLE)",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
    )

    # 互換（任意）：BASE TABLEが未整備の時だけ VIEW を試す
    if df.empty:
        sql_view = f"""
SELECT
  email,
  staff_name,
  role,
  phone
FROM `{VIEW_STAFF_MASTER}`
WHERE LOWER(email) = @login_email
LIMIT 1
"""
        df = query_df_safe(
            client,
            sql_view,
            params={"login_email": login_email},
            label="Role Check (VIEW fallback)",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
        )

    if df.empty:
        return RoleInfo(login_email=login_email)

    r = df.iloc[0]
    raw_role = str(r.get("role", "")).strip()
    role_key, is_admin, area = _normalize_role(raw_role)

    return RoleInfo(
        login_email=login_email,
        staff_name=str(r.get("staff_name", "不明")),
        role_key=role_key,
        role_admin_view=bool(is_admin),
        phone=str(r.get("phone", "-")),
        area_name=area,
    )


# -----------------------------
# 5. Query Runner (scoped views)
# -----------------------------
def run_scoped_query(
    client: bigquery.Client,
    sql_template: str,
    scope_col: str,
    login_email: str,
    allow_fallback: bool,
    use_bqstorage: bool,
    timeout_sec: int,
) -> pd.DataFrame:
    # __WHERE__ を差し替え（scoped viewの列に合わせる）
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    df = query_df_safe(
        client,
        sql,
        params={"login_email": login_email},
        label="Scoped Query",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
    )
    if not df.empty:
        return df

    if allow_fallback:
        sql_all = sql_template.replace(
            "__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL'
        )
        return query_df_safe(
            client,
            sql_all,
            params=None,
            label="Fallback(all) Query",
            use_bqstorage=use_bqstorage,
            timeout_sec=timeout_sec,
        )

    return pd.DataFrame()


# -----------------------------
# 6. BigQuery Calculation Logic（多次元増減）
# -----------------------------
def fetch_ranking_from_bq(
    client: bigquery.Client,
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

    sql = f"""
WITH base_stats AS (
  SELECT MAX(fiscal_year) AS current_fy FROM `{VIEW_UNIFIED}`
)
SELECT
  {group_col} AS name,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) AS sales_cur,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) AS gp_cur,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_prev,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN {target_val} ELSE 0 END)
  - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN {target_val} ELSE 0 END) AS diff_val,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END)
  - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_diff,
  SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END)
  - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN gross_profit ELSE 0 END) AS gp_diff
FROM `{VIEW_UNIFIED}`
WHERE
  jan_code NOT IN {NOISE_JAN_SQL}
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
        params=None,
        label="Ranking Query",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
    )


def fetch_drilldown_from_bq(
    client: bigquery.Client,
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

    sql = f"""
SELECT
  {target_col} AS `{target_label}`,
  SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) AS `今年売上`,
  SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) AS `前年売上`,
  SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END)
  - SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) AS `売上差額`,
  SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) AS `今年粗利`,
  SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END)
  - SUM(CASE WHEN fiscal_year = 2024 THEN gross_profit ELSE 0 END) AS `粗利差額`
FROM `{VIEW_UNIFIED}`
WHERE {key_col} = @key_val
GROUP BY 1
ORDER BY `{sort_col_alias}` {order_dir}
LIMIT 500
"""
    return query_df_safe(
        client,
        sql,
        params={"key_val": key_val},
        label="Drilldown Query",
        use_bqstorage=use_bqstorage,
        timeout_sec=timeout_sec,
    )


# -----------------------------
# 7. UI Components
# -----------------------------
def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.image(get_qr_code_url(APP_URL), caption="📱スマホでアクセス", width=150)
    st.sidebar.divider()
    use_bqstorage = st.sidebar.toggle("BigQuery Storage API（高速）", value=True)
    timeout_sec = st.sidebar.slider("クエリタイムアウト（秒）", min_value=10, max_value=300, value=60, step=10)
    show_sql = st.sidebar.toggle("SQL表示（デバッグ）", value=False)
    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.sidebar.success("Cache Cleared.")
    return {"use_bqstorage": use_bqstorage, "timeout_sec": timeout_sec, "show_sql": show_sql}


def get_login_email_ui() -> str:
    st.sidebar.header("Login Simulation")
    default = st.secrets.get("default_login_email", "") if "default_login_email" in st.secrets else ""
    login_email = st.sidebar.text_input("Login Email", value=default).strip()
    if not login_email:
        st.info("👈 左の Login Email を入力してください")
        st.stop()
    return login_email


def render_interactive_ranking_matrix(
    client: bigquery.Client,
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
        client,
        ranking_type,
        axis_mode,
        is_sales_mode,
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

    st.markdown(f"##### ① {label_col}を選択（{mode_label}ベース）")
    st.caption(f"※{mode_label}の増減額が大きい順（計算: BigQuery）")

    key_suffix = f"{ranking_type}_{axis_mode}_{mode_label}"
    event = st.dataframe(
        df_disp[cols],
        use_container_width=True,
        hide_index=True,
        column_config=create_default_column_config(df_disp[cols]),
        height=420,
        on_select="rerun",
        selection_mode="single-row",
        key=f"rank_{key_suffix}",
    )

    if len(event.selection["rows"]) == 0:
        return

    idx = event.selection["rows"][0]
    selected_val = df_disp.iloc[idx][label_col]

    st.divider()
    st.subheader(f"🔎 内訳分析: {selected_val}")

    key_col = "product_name" if is_product else "customer_name"
    target_col = "customer_name" if is_product else "product_name"

    df_drill = fetch_drilldown_from_bq(
        client,
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
        return

    drill_label = "得意先名" if is_product else "商品名"
    if is_sales_mode:
        d_cols = [drill_label, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
    else:
        d_cols = [drill_label, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]

    st.dataframe(
        df_drill[d_cols],
        use_container_width=True,
        hide_index=True,
        column_config=create_default_column_config(df_drill[d_cols]),
        height=500,
        key=f"drill_{key_suffix}",
    )


def render_fytd_org_section(client: bigquery.Client, login_email: str, role: RoleInfo, opts: Dict[str, Any]):
    st.subheader("🏢 年度累計（FYTD）｜全社")
    if st.button("全社データを読み込む", key="btn_org_load", use_container_width=True):
        st.session_state.org_data_loaded = True

    if not st.session_state.org_data_loaded:
        st.info("👆 上のボタンを押して全社データを読み込んでください")
        return

    sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
    df_org = run_scoped_query(
        client,
        sql_kpi,
        scope_col="viewer_email",
        login_email=login_email,
        allow_fallback=True,  # admin_viewならall fallback許容
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
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
        c4.metric("④ GAP", f"¥{(s_fc - s_py):,.0f}", delta_color="off")

        st.markdown("##### ■ 粗利 (Gross Profit)")
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("① 現状", f"¥{gp_cur:,.0f}")
        c6.metric("② 昨年", f"¥{gp_py:,.0f}")
        c7.metric("③ 予測", f"¥{gp_fc:,.0f}")
        c8.metric("④ GAP", f"¥{(gp_fc - gp_py):,.0f}", delta_color="off")
        st.divider()

    st.subheader("📊 増減要因分析（多次元）")

    c_axis, c_val = st.columns(2)
    with c_axis:
        axis_sel = st.radio("集計軸:", ["📦 商品軸", "🏥 得意先軸"], horizontal=True)
        axis_mode = "product" if "商品" in axis_sel else "customer"
    with c_val:
        val_sel = st.radio("評価指標:", ["💰 売上金額", "💹 粗利金額"], horizontal=True)
        is_sales_mode = "売上" in val_sel

    tab_worst, tab_best = st.tabs(["📉 ワースト（減）", "📈 ベスト（増）"])
    with tab_worst:
        render_interactive_ranking_matrix(client, "worst", axis_mode, is_sales_mode, opts)
    with tab_best:
        render_interactive_ranking_matrix(client, "best", axis_mode, is_sales_mode, opts)


def render_fytd_me_section(client: bigquery.Client, login_email: str, opts: Dict[str, Any]):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(
            client,
            sql,
            scope_col="login_email",
            login_email=login_email,
            allow_fallback=False,
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
        )
        if df_me.empty:
            st.warning("自分FYTDが0件です。")
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
            height=260,
        )


def render_yoy_section(client: bigquery.Client, login_email: str, allow_fallback: bool, opts: Dict[str, Any]):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)

    def _show_table(title: str, view_name: str, key: str):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(
                client,
                sql,
                scope_col="login_email",
                login_email=login_email,
                allow_fallback=allow_fallback,
                use_bqstorage=opts["use_bqstorage"],
                timeout_sec=opts["timeout_sec"],
            )
            if df.empty:
                st.info("0件です。")
                return
            df_disp = rename_columns_for_display(df, JP_COLS_YOY)
            st.dataframe(df_disp, use_container_width=True, hide_index=True, height=420)

    with c1:
        _show_table("YoY Top（伸び）", VIEW_YOY_TOP, "btn_top")
    with c2:
        _show_table("YoY Bottom（落ち）", VIEW_YOY_BOTTOM, "btn_btm")
    with c3:
        _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")


def render_customer_drilldown(client: bigquery.Client, login_email: str, opts: Dict[str, Any]):
    st.subheader("🎯 得意先別・戦略提案（プロファイル & 未採用ギャップ）")

    sql_cust = f"""
SELECT DISTINCT customer_code, customer_name
FROM `{VIEW_FACT_DAILY}`
WHERE login_email = @login_email
ORDER BY customer_code
"""
    df_cust = query_df_safe(
        client,
        sql_cust,
        params={"login_email": login_email},
        label="Cust List",
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
    )
    if df_cust.empty:
        st.info("得意先が取得できません（売上データがない可能性）")
        return

    cust_options = {
        row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}"
        for _, row in df_cust.iterrows()
    }
    selected_code = st.selectbox(
        "分析する得意先を選択してください",
        options=list(cust_options.keys()),
        format_func=lambda x: cust_options[x],
    )
    if not selected_code:
        return

    st.divider()

    # 推奨（v_sales_recommendation_engine 側の列は環境で異なる可能性があるため、まずは * で取得）
    sql_rec = f"""
SELECT *
FROM `{VIEW_RECOMMEND}`
WHERE customer_code = @cust_code
ORDER BY priority_rank ASC
"""
    df_rec = query_df_safe(
        client,
        sql_rec,
        params={"cust_code": selected_code},
        label="Recommendation",
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
    )

    c1, c2 = st.columns([1, 2])

    with c1:
        st.markdown("#### 🏥 プロファイル")
        strong = "-"
        if not df_rec.empty:
            # strong_category / targeted_category など揺れ吸収
            for k in ["strong_category", "targeted_category", "main_category", "category_name"]:
                if k in df_rec.columns:
                    strong = str(df_rec.iloc[0].get(k, "-") or "-")
                    break
        st.info(f"主力領域: **{strong}**")

    with c2:
        st.markdown("#### 💡 AI提案リスト（未採用品）")
        if df_rec.empty:
            st.info("提案データがありません（VIEW定義/列名を確認）")
        else:
            # 代表列の揺れ吸収
            cols = []
            if "priority_rank" in df_rec.columns:
                cols.append("priority_rank")
            if "recommend_product" in df_rec.columns:
                cols.append("recommend_product")
            elif "product_name" in df_rec.columns:
                cols.append("product_name")
            if "manufacturer" in df_rec.columns:
                cols.append("manufacturer")
            elif "maker_name" in df_rec.columns:
                cols.append("maker_name")
            if "market_scale" in df_rec.columns:
                cols.append("market_scale")
            if "recommend_jan" in df_rec.columns:
                cols.append("recommend_jan")

            disp = df_rec[cols].copy() if cols else df_rec.copy()
            rename_map = {
                "priority_rank": "順位",
                "recommend_product": "商品",
                "product_name": "商品",
                "manufacturer": "メーカー",
                "maker_name": "メーカー",
                "market_scale": "規模",
                "recommend_jan": "JAN",
            }
            disp = disp.rename(columns=rename_map)
            st.dataframe(disp, use_container_width=True, hide_index=True, height=420)

    with st.expander("参考：現在の採用品リスト（FYTD）を見る"):
        sql_adopted = f"""
SELECT
  m.product_name,
  SUM(t.sales_amount) AS sales_fytd,
  SUM(t.gross_profit) AS gp_fytd
FROM `{VIEW_FACT_DAILY}` t
LEFT JOIN `{VIEW_ITEM_MASTER}` m
  ON CAST(t.jan AS STRING) = CAST(m.jan_code AS STRING)
WHERE
  t.customer_code = @cust_code
  AND t.fiscal_year = 2025
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
"""
        df_adopted = query_df_safe(
            client,
            sql_adopted,
            params={"cust_code": selected_code},
            label="Adopted List",
            use_bqstorage=opts["use_bqstorage"],
            timeout_sec=opts["timeout_sec"],
        )
        if df_adopted.empty:
            st.info("採用品が取得できません。")
        else:
            renamed = df_adopted.rename(
                columns={"product_name": "商品名", "sales_fytd": "売上(FYTD)", "gp_fytd": "粗利(FYTD)"}
            )
            st.dataframe(
                renamed,
                use_container_width=True,
                hide_index=True,
                column_config=create_default_column_config(renamed),
                height=420,
            )


# -----------------------------
# 8. Main
# -----------------------------
def main():
    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    set_page()
    client, project_id, location, sa_json = setup_bigquery_client()
    opts = sidebar_controls()

    login_email = get_login_email_ui()
    st.divider()

    # ★Role解決（BASE TABLE優先）
    role = resolve_role(
        client,
        login_email=login_email,
        use_bqstorage=opts["use_bqstorage"],
        timeout_sec=opts["timeout_sec"],
    )

    # ログイン表示
    st.write(f"👤 **担当:** {role.staff_name}")
    st.write(f"📧 **Email:** {role.login_email}")
    st.write(f"🛡️ **Role:** {role.role_key}")
    st.write(f"🗺️ **Area:** {role.area_name}")
    # 電話は末尾4桁だけ見せる（内部仕様）
    phone_tail = (role.phone or "").replace("-", "").strip()[-4:] if role.phone and role.phone != "-" else ""
    st.write(f"📞 **Phone:** ***-****-{phone_tail}" if phone_tail else "📞 **Phone:** -")
    st.divider()

    allow_fallback = role.role_admin_view  # adminのみ all fallback を許可

    if role.role_admin_view:
        t1, t2, t3 = st.tabs(["🏢 組織状況（経営）", "👤 個人成績（行動）", "🎯 戦略提案（現場）"])
        with t1:
            render_fytd_org_section(client, login_email, role, opts)
        with t2:
            render_fytd_me_section(client, login_email, opts)
            st.divider()
            render_yoy_section(client, login_email, allow_fallback=True, opts=opts)
        with t3:
            render_customer_drilldown(client, login_email, opts)
    else:
        t1, t2, t3 = st.tabs(["👤 今年の成績（FYTD）", "📊 得意先別（YoY）", "🎯 提案を作る"])
        with t1:
            render_fytd_me_section(client, login_email, opts)
        with t2:
            render_yoy_section(client, login_email, allow_fallback=False, opts=opts)
        with t3:
            render_customer_drilldown(client, login_email, opts)

    st.caption("※ 403(Drive credentials) が出る場合：Role参照が EXTERNAL を踏んでいます。必ず sales_staff_master_bq(BASE TABLE) を参照してください。")


if __name__ == "__main__":
    main()
