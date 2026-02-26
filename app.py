# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.10 (v1.4.9踏襲 + 新規納品トレンド追加)

【v1.4.9 踏襲】
- YoY：VIEW_UNIFIED から動的集計に統一（YJ同一で商品名が2行問題を抑止）
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定
- Group Display: official先頭 + raw併記
- 新機能：得意先グループ / 得意先単体の切替 ＆ 商品要因ドリルダウン（全件表示）
- 新機能：順位アイコンの追加と、不要なYJコード列の非表示
- 修正：WHERE二重エラー解消 ＆ 選択状態の消失バグ解消 ＆ 表示順序の最適化
- 修正：Reco（VIEW_RECOMMEND）の customer_code が INT64 のため、STRINGキー（VIEW_UNIFIED）と照合できるよう CAST 対応
- ColMap（列名吸収）導入：jan/jan_code、pack_unit/package_unit 等の差異を自動解決
- 全SQLで colmap を貫通：列名揺れ起因の "Unrecognized name" を根絶
- 必須列が見つからない場合は、起動直後に「不足列一覧」を明示して停止（沈黙しない）

【v1.4.10 ★追加（今回）】
- ★ 新規納品トレンド（得意先 / グループ / 商品）を追加
  - 直近N日 vs その前N日の比較で「増えている」ランキングを表示
  - 追加の増分テーブル・成長更新SQLは作らない（現行VIEW更新を最大活用）
  - グループ切り口は VIEW_UNIFIED の group_expr を参照（無ければ非表示）
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.api.types import is_numeric_dtype


# -----------------------------
# 1. Configuration (設定)
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified_grouped"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

CUSTOMER_GROUP_COLUMN_CANDIDATES = (
    "customer_group_display",
    "customer_group_official",
    "customer_group_raw",
    "sales_group_name",
)


# -----------------------------
# 2. Helpers (表示用)
# -----------------------------
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.10｜v1.4.9踏襲 + 新規納品トレンド（得意先/グループ/商品）")


def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差額", "実績", "予測", "GAP"]):
            config[col] = st.column_config.NumberColumn(col, format="¥%d")
        elif any(k in col for k in ["率", "比", "ペース"]):
            config[col] = st.column_config.NumberColumn(col, format="%.1f%%")
        elif "日" in col or pd.api.types.is_datetime64_any_dtype(df[col]):
            config[col] = st.column_config.DateColumn(col, format="YYYY-MM-DD")
        elif is_numeric_dtype(df[col]):
            config[col] = st.column_config.NumberColumn(col, format="%d")
        else:
            config[col] = st.column_config.TextColumn(col)
    return config


def get_safe_float(row: pd.Series, key: str) -> float:
    val = row.get(key)
    return float(val) if not pd.isna(val) else 0.0


def normalize_product_display_name(name: Any) -> str:
    if pd.isna(name):
        return ""
    text = str(name).strip()
    text = re.sub(r"[/／].*$", "", text)
    return text.strip()


# -----------------------------
# 3. BigQuery Connection & Auth
# -----------------------------
@st.cache_resource
def setup_bigquery_client() -> bigquery.Client:
    bq = st.secrets["bigquery"]
    sa_info = dict(bq["service_account"])
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return bigquery.Client(
        project=PROJECT_DEFAULT,
        credentials=creds,
        location=DEFAULT_LOCATION,
    )


def _normalize_param(value: Any) -> Tuple[str, Optional[Any]]:
    if isinstance(value, tuple) and len(value) == 2:
        p_type, p_value = value
        return str(p_type).upper(), p_value

    if value is None:
        return "STRING", None
    if isinstance(value, bool):
        return "BOOL", value
    if isinstance(value, int):
        return "INT64", value
    if isinstance(value, float):
        return "FLOAT64", value
    if isinstance(value, pd.Timestamp):
        return "TIMESTAMP", value.to_pydatetime()

    return "STRING", str(value)


def query_df_safe(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    timeout_sec: int = 60,
) -> pd.DataFrame:
    use_bqstorage = st.session_state.get("use_bqstorage", True)
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            query_params = []
            for key, raw_value in params.items():
                p_type, p_value = _normalize_param(raw_value)
                query_params.append(bigquery.ScalarQueryParameter(key, p_type, p_value))
            job_config.query_parameters = query_params

        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except Exception as e:
        st.error(f"クエリエラー ({label}):\n{e}")
        return pd.DataFrame()


@dataclass(frozen=True)
class RoleInfo:
    is_authenticated: bool = False
    login_email: str = ""
    staff_name: str = "ゲスト"
    role_key: str = "GUEST"
    role_admin_view: bool = False
    phone: str = "-"


@dataclass(frozen=True)
class ScopeFilter:
    predicates: tuple[str, ...] = ()
    params: Dict[str, Any] | None = None

    def where_clause(self) -> str:
        if not self.predicates:
            return ""
        return " AND ".join(self.predicates)


def _compose_where(*parts: str) -> str:
    clauses = [p.strip() for p in parts if p and p.strip()]
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


def _split_table_fqn(table_fqn: str) -> Tuple[str, str, str]:
    parts = table_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid table FQN: {table_fqn}")
    return parts[0], parts[1], parts[2]


@st.cache_data(ttl=3600)
def role_table_has_login_code(_client: bigquery.Client) -> bool:
    project_id, dataset_id, table_name = _split_table_fqn(VIEW_ROLE_CLEAN)
    sql = f"""
        SELECT 1
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
          AND column_name = 'login_code'
        LIMIT 1
    """
    df = query_df_safe(_client, sql, {"table_name": table_name}, "Role Schema Check")
    return not df.empty


@st.cache_data(ttl=3600)
def get_unified_columns(_client: bigquery.Client) -> set[str]:
    project_id, dataset_id, table_name = _split_table_fqn(VIEW_UNIFIED)
    sql = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
    """
    df = query_df_safe(_client, sql, {"table_name": table_name}, "Unified Schema Check")
    if df.empty or "column_name" not in df.columns:
        return set()
    return {str(c).lower() for c in df["column_name"].dropna().tolist()}


def get_available_customer_group_columns(_client: bigquery.Client) -> list[str]:
    columns = get_unified_columns(_client)
    return [col for col in CUSTOMER_GROUP_COLUMN_CANDIDATES if col in columns]


@st.cache_data(ttl=3600)
def get_customer_group_column_profiles(_client: bigquery.Client) -> pd.DataFrame:
    available_cols = get_available_customer_group_columns(_client)
    if not available_cols:
        return pd.DataFrame()

    union_parts = []
    for col in available_cols:
        union_parts.append(
            f"""
            SELECT
              '{col}' AS column_name,
              COUNT(*) AS total_rows,
              COUNTIF(COALESCE(NULLIF(CAST({col} AS STRING), ''), '') != '') AS non_null_rows,
              COUNT(DISTINCT NULLIF(CAST({col} AS STRING), '')) AS distinct_groups
            FROM `{VIEW_UNIFIED}`
            """
        )

    sql = "\nUNION ALL\n".join(union_parts) + "\nORDER BY non_null_rows DESC, distinct_groups DESC"
    return query_df_safe(_client, sql, label="Customer Group Column Profile")


def resolve_customer_group_sql_expr(_client: bigquery.Client) -> Tuple[Optional[str], Optional[str]]:
    cols = get_unified_columns(_client)

    has_display = "customer_group_display" in cols
    has_official = "customer_group_official" in cols
    has_raw = "customer_group_raw" in cols
    has_old = "sales_group_name" in cols

    if has_display:
        expr = "COALESCE(NULLIF(CAST(customer_group_display AS STRING), ''), '未設定')"
        return expr, f"{VIEW_UNIFIED}.customer_group_display"

    if has_official and has_raw:
        official = "NULLIF(CAST(customer_group_official AS STRING), '')"
        raw = "NULLIF(CAST(customer_group_raw AS STRING), '')"
        expr = f"""
          COALESCE(
            CASE
              WHEN {official} IS NOT NULL AND {raw} IS NOT NULL AND {official} != {raw}
                THEN CONCAT({official}, '（', {raw}, '）')
              WHEN {official} IS NOT NULL THEN {official}
              WHEN {raw} IS NOT NULL THEN {raw}
              ELSE NULL
            END,
            '未設定'
          )
        """
        return " ".join(expr.split()), f"{VIEW_UNIFIED}.customer_group_official + customer_group_raw"

    if has_official:
        expr = "COALESCE(NULLIF(CAST(customer_group_official AS STRING), ''), '未設定')"
        return expr, f"{VIEW_UNIFIED}.customer_group_official"

    if has_raw:
        expr = "COALESCE(NULLIF(CAST(customer_group_raw AS STRING), ''), '未設定')"
        return expr, f"{VIEW_UNIFIED}.customer_group_raw"

    if has_old:
        expr = "COALESCE(NULLIF(CAST(sales_group_name AS STRING), ''), '未設定')"
        return expr, f"{VIEW_UNIFIED}.sales_group_name"

    return None, None


# -----------------------------
# ★ v1.4.9 ColMap（列名吸収）
# -----------------------------
@st.cache_data(ttl=3600)
def resolve_unified_colmap(_client: bigquery.Client) -> Dict[str, str]:
    """
    VIEW_UNIFIED の列名揺れを吸収して、SQLから参照する「論理キー→物理列名」を返す。
    ※ BigQueryは未クオート識別子は大文字小文字非区別なので、lowerで返す。
    """
    cols = get_unified_columns(_client)

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c.lower() in cols:
                return c.lower()
        return None

    colmap: Dict[str, Optional[str]] = {
        # 主要キー
        "customer_code": pick("customer_code", "得意先コード", "得意先CD"),
        "customer_name": pick("customer_name", "得意先名"),
        "login_email": pick("login_email", "email", "担当者メール", "担当メール", "login"),
        # 日付・年度・金額
        "sales_date": pick("sales_date", "販売日", "date"),
        "fiscal_year": pick("fiscal_year", "年度", "fy"),
        "sales_amount": pick("sales_amount", "売上", "合計価格", "sales"),
        "gross_profit": pick("gross_profit", "粗利", "gp"),
        # 商品系
        "product_name": pick("product_name", "商品名", "商品名称", "item_name", "商品名称"),
        "yj_code": pick("yj_code", "yjcode", "yj", "YJCode"),
        "jan_code": pick("jan_code", "jan", "JAN"),
        "package_unit": pick("package_unit", "pack_unit", "包装単位", "包装"),
    }

    # Optional（無くても止めない）
    opt = {
        "staff_name": pick("staff_name", "担当者名", "担当社員名", "担当社員氏", "担当"),
    }

    # 必須列チェック（ここが無いとアプリが成立しない）
    required = ["customer_code", "customer_name", "sales_date", "fiscal_year", "sales_amount", "gross_profit", "product_name"]
    missing = [k for k in required if not colmap.get(k)]
    if missing:
        colmap["_missing_required"] = ",".join(missing)

    # opt merge
    colmap.update({k: v for k, v in opt.items() if v})
    # Optional も None は捨てる
    return {k: v for k, v in colmap.items() if v is not None}


def c(colmap: Dict[str, str], key: str) -> str:
    """SQL内で使う列名解決。必須列が無い場合もここでは落とさない（起動前に止める）。"""
    return colmap.get(key, key)


def render_scope_filters(client: bigquery.Client, role: RoleInfo) -> ScopeFilter:
    st.markdown("### 🔍 分析スコープ設定")
    predicates: list[str] = []
    params: Dict[str, Any] = {}

    with st.expander("詳細絞り込み（得意先グループ・得意先名）", expanded=False):
        c1, c2 = st.columns(2)

        group_expr, group_src = resolve_customer_group_sql_expr(client)
        if group_expr:
            role_where = ""
            role_params: Dict[str, Any] = {}
            if not role.role_admin_view:
                role_where = "WHERE login_email = @login_email"
                role_params["login_email"] = role.login_email

            sql_group = f"""
                SELECT DISTINCT {group_expr} AS group_name
                FROM `{VIEW_UNIFIED}`
                {role_where}
                ORDER BY group_name
                LIMIT 500
            """
            df_group = query_df_safe(client, sql_group, role_params, "Scope Group Options")
            group_opts = ["指定なし"] + (df_group["group_name"].tolist() if not df_group.empty else [])
            selected_group = c1.selectbox("得意先グループ", options=group_opts)
            if selected_group != "指定なし":
                predicates.append(f"{group_expr} = @scope_group")
                params["scope_group"] = selected_group

            if group_src:
                c1.caption(f"抽出元: `{group_src}`")
        else:
            c1.caption("グループ列なし（VIEW_UNIFIEDに該当列が存在しません）")

        keyword = c2.text_input("得意先名（部分一致）", placeholder="例：古賀病院")
        if keyword.strip():
            predicates.append("customer_name LIKE @scope_customer_name")
            params["scope_customer_name"] = f"%{keyword.strip()}%"

    return ScopeFilter(predicates=tuple(predicates), params=params)


def resolve_role(client: bigquery.Client, login_email: str, login_code: str) -> RoleInfo:
    if not login_email or not login_code:
        return RoleInfo()

    has_login_code = role_table_has_login_code(client)

    if has_login_code:
        sql = f"""
            SELECT login_email, role_tier
            FROM `{VIEW_ROLE_CLEAN}`
            WHERE login_email = @login_email
              AND CAST(login_code AS STRING) = @login_code
            LIMIT 1
        """
        params: Dict[str, Any] = {"login_email": login_email, "login_code": login_code}
    else:
        sql = f"""
            SELECT login_email, role_tier
            FROM `{VIEW_ROLE_CLEAN}`
            WHERE login_email = @login_email
            LIMIT 1
        """
        params = {"login_email": login_email}

    df = query_df_safe(client, sql, params, "Auth Check")
    if df.empty:
        return RoleInfo(login_email=login_email)

    row = df.iloc[0]
    raw_role = str(row["role_tier"]).strip().upper()
    is_admin = any(x in raw_role for x in ["ADMIN", "MANAGER", "HQ"])

    return RoleInfo(
        is_authenticated=True,
        login_email=login_email,
        staff_name=login_email.split("@")[0],
        role_key="HQ_ADMIN" if is_admin else "SALES",
        role_admin_view=is_admin,
        phone="-",
    )


# -----------------------------
# 4. UI Sections (各セクション)
# -----------------------------
def render_summary_metrics(row: pd.Series) -> None:
    s_cur = get_safe_float(row, "sales_amount_fytd")
    s_py_ytd = get_safe_float(row, "sales_amount_py_ytd")
    s_py_total = get_safe_float(row, "sales_amount_py_total")

    s_fc = s_cur * (s_py_total / s_py_ytd) if s_py_ytd > 0 else s_cur

    gp_cur = get_safe_float(row, "gross_profit_fytd")
    gp_py_ytd = get_safe_float(row, "gross_profit_py_ytd")
    gp_py_total = get_safe_float(row, "gross_profit_py_total")
    gp_fc = gp_cur * (gp_py_total / gp_py_ytd) if gp_py_ytd > 0 else gp_cur

    st.caption(
        "※ 【今期予測】はAI予測ではなく、「今期実績 × (昨年度着地 ÷ 前年同期)」"
        "による季節変動を加味した推移ペース（着地見込）です。"
    )

    st.markdown("##### ■ 売上")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{s_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{s_py_ytd:,.0f}", delta=f"{int(s_cur - s_py_ytd):,}" if s_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{s_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{s_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{s_fc - s_py_total:,.0f}", delta=f"{int(s_fc - s_py_total):,}")

    st.markdown("##### ■ 粗利")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{gp_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{gp_py_ytd:,.0f}", delta=f"{int(gp_cur - gp_py_ytd):,}" if gp_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{gp_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{gp_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{gp_fc - gp_py_total:,.0f}", delta=f"{int(gp_fc - gp_py_total):,}")


def render_fytd_org_section(client: bigquery.Client, colmap: Dict[str, str]) -> None:
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")

    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    if st.button("全社データを読み込む", key="btn_org_load"):
        st.session_state.org_data_loaded = True

    if st.session_state.get("org_data_loaded"):
        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
        """
        df_org = query_df_safe(client, sql, None, "Org Summary")
        if not df_org.empty:
            render_summary_metrics(df_org.iloc[0])


def render_fytd_me_section(client: bigquery.Client, login_email: str, colmap: Dict[str, str]) -> None:
    st.subheader("👤 年度累計（FYTD）｜個人サマリー")
    if st.button("自分の成績を読み込む", key="btn_me_load"):
        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
            WHERE {c(colmap,'login_email')} = @login_email
        """
        df_me = query_df_safe(client, sql, {"login_email": login_email}, "Me Summary")
        if not df_me.empty:
            render_summary_metrics(df_me.iloc[0])


def render_group_underperformance_section(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("🏢 得意先・グループ別パフォーマンス ＆ 要因分析")

    if "group_perf_mode" not in st.session_state:
        st.session_state.group_perf_mode = "ワースト"

    c1, c2 = st.columns(2)
    view_choice = c1.radio("📊 分析の単位", ["🏢 グループ別", "🏥 得意先単体"], horizontal=True)
    mode_choice = c2.radio("🏆 ランキング基準", ["📉 下落幅ワースト", "📈 上昇幅ベスト"], horizontal=True)

    perf_view = "グループ別" if "グループ別" in view_choice else "得意先別"
    perf_mode = "ワースト" if "ワースト" in mode_choice else "ベスト"
    sort_order = "ASC" if perf_mode == "ワースト" else "DESC"

    group_expr, group_src = resolve_customer_group_sql_expr(client)
    if perf_view == "グループ別" and not group_expr:
        st.info("グループ分析に利用できる列が見つかりません（VIEW_UNIFIEDにグループ列がありません）。")
        return

    role_filter = "" if role.role_admin_view else f"{c(colmap,'login_email')} = @login_email"
    scope_filter_clause = scope.where_clause().replace("customer_name", c(colmap, "customer_name"))
    filter_sql = _compose_where(role_filter, scope_filter_clause)

    params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        params["login_email"] = role.login_email

    if perf_view == "グループ別":
        sql_parent = f"""
            WITH fy AS (
              SELECT
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                 - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today
            )
            SELECT
              {group_expr} AS `名称`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前年同期売上`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'gross_profit')} ELSE 0 END) AS `今期粗利`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'gross_profit')} ELSE 0 END) AS `前年同期粗利`
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN fy
            {filter_sql}
            GROUP BY `名称`
            HAVING `前年同期売上` > 0 OR `今期売上` > 0
            ORDER BY (`今期売上` - `前年同期売上`) {sort_order}
            LIMIT 50
        """
    else:
        sql_parent = f"""
            WITH fy AS (
              SELECT
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                 - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today
            )
            SELECT
              {c(colmap,'customer_code')} AS `コード`,
              ANY_VALUE({c(colmap,'customer_name')}) AS `名称`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前年同期売上`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'gross_profit')} ELSE 0 END) AS `今期粗利`,
              SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'gross_profit')} ELSE 0 END) AS `前年同期粗利`
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN fy
            {filter_sql}
            GROUP BY `コード`
            HAVING `前年同期売上` > 0 OR `今期売上` > 0
            ORDER BY (`今期売上` - `前年同期売上`) {sort_order}
            LIMIT 50
        """

    df_parent = query_df_safe(client, sql_parent, params, f"Parent Perf {perf_view}")
    if df_parent.empty:
        st.info("表示できるデータがありません。")
        return

    df_parent["売上差額"] = df_parent["今期売上"] - df_parent["前年同期売上"]
    df_parent["売上成長率"] = df_parent.apply(
        lambda r: ((r["今期売上"] / r["前年同期売上"] - 1) * 100) if r["前年同期売上"] else 0,
        axis=1,
    )
    df_parent["粗利差額"] = df_parent["今期粗利"] - df_parent["前年同期粗利"]

    def get_parent_rank_icon(rank: int, mode: str) -> str:
        if mode == "ベスト":
            if rank == 1:
                return "🥇 1位"
            if rank == 2:
                return "🥈 2位"
            if rank == 3:
                return "🥉 3位"
            return f"🌟 {rank}位"
        else:
            if rank == 1:
                return "🚨 1位"
            if rank == 2:
                return "⚠️ 2位"
            if rank == 3:
                return "⚡ 3位"
            return f"📉 {rank}位"

    df_parent.insert(0, "順位", [get_parent_rank_icon(i + 1, perf_mode) for i in range(len(df_parent))])

    if perf_view == "グループ別" and group_src:
        st.caption(f"抽出元グループ列: `{group_src}`")

    event = st.dataframe(
        df_parent.style.format(
            {
                "今期売上": "¥{:,.0f}",
                "前年同期売上": "¥{:,.0f}",
                "今期粗利": "¥{:,.0f}",
                "前年同期粗利": "¥{:,.0f}",
                "売上差額": "¥{:,.0f}",
                "売上成長率": "{:.1f}%",
                "粗利差額": "¥{:,.0f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun",
        key=f"grid_parent_{perf_view}_{perf_mode}",
    )

    selected_parent_id = None
    selected_parent_name = None

    try:
        sel_rows = []
        if hasattr(event, "selection") and hasattr(event.selection, "rows"):
            sel_rows = event.selection.rows
        elif isinstance(event, dict) and "selection" in event:
            sel_rows = event["selection"].get("rows", [])

        if sel_rows:
            idx = sel_rows[0]
            if perf_view == "グループ別":
                selected_parent_id = str(df_parent.iloc[idx]["名称"])
                selected_parent_name = selected_parent_id
            else:
                selected_parent_id = str(df_parent.iloc[idx]["コード"])
                selected_parent_name = str(df_parent.iloc[idx]["名称"])
    except Exception:
        pass

    if selected_parent_id:
        st.markdown(f"#### 🔍 【{selected_parent_name}】要因分析（商品ベース {perf_mode}・全件一覧）")

        drill_params = dict(params)

        if perf_view == "グループ別":
            drill_filter_sql = _compose_where(
                role_filter,
                scope_filter_clause,
                f"{group_expr} = @parent_id",
            )
        else:
            drill_filter_sql = _compose_where(
                role_filter,
                scope_filter_clause,
                f"{c(colmap,'customer_code')} = @parent_id",
            )

        drill_params["parent_id"] = selected_parent_id

        sql_drill = f"""
            WITH fy AS (
              SELECT (
                EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
              ) AS current_fy,
              DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today
            ),
            base_raw AS (
              SELECT
                COALESCE(
                  NULLIF(NULLIF(TRIM(CAST({c(colmap,'yj_code')} AS STRING)), ''), '0'),
                  NULLIF(NULLIF(TRIM(CAST({c(colmap,'jan_code')} AS STRING)), ''), '0'),
                  TRIM(CAST({c(colmap,'product_name')} AS STRING))
                ) AS yj_key,
                REGEXP_REPLACE(CAST({c(colmap,'product_name')} AS STRING), r"[/／].*$", "") AS product_base,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {drill_filter_sql}
              GROUP BY yj_key, product_base
            ),
            base AS (
              SELECT
                yj_key AS yj_code,
                ARRAY_AGG(product_base ORDER BY ty_sales DESC LIMIT 1)[OFFSET(0)] AS product_name,
                SUM(ty_sales) AS ty_sales,
                SUM(py_sales) AS py_sales
              FROM base_raw
              GROUP BY yj_code
            )
            SELECT
              yj_code,
              product_name,
              ty_sales AS sales_amount,
              py_sales AS py_sales_amount,
              (ty_sales - py_sales) AS sales_diff_yoy
            FROM base
            WHERE ty_sales > 0 OR py_sales > 0
            ORDER BY sales_diff_yoy {sort_order}
        """
        df_drill = query_df_safe(client, sql_drill, drill_params, "Parent Drilldown")

        if not df_drill.empty:
            df_drill["product_name"] = df_drill["product_name"].apply(normalize_product_display_name)
            df_drill = df_drill.fillna(0)

            df_drill.insert(0, "要因順位", [get_parent_rank_icon(i + 1, perf_mode) for i in range(len(df_drill))])

            st.dataframe(
                df_drill[["要因順位", "product_name", "sales_amount", "py_sales_amount", "sales_diff_yoy"]].rename(
                    columns={
                        "product_name": "代表商品名(成分)",
                        "sales_amount": "今期売上",
                        "py_sales_amount": "前年同期売上",
                        "sales_diff_yoy": "前年比差額",
                    }
                ).style.format(
                    {"今期売上": "¥{:,.0f}", "前年同期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("要因データが見つかりません。")


def render_yoy_section(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter, colmap: Dict[str, str]) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJ優先｜YJ=0/nullはJANキーで追跡）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = "ワースト"
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()
    if "selected_yoy_key" not in st.session_state:
        st.session_state.selected_yoy_key = "全成分を表示"

    c1, c2, c3 = st.columns(3)

    def load_yoy(mode_name: str) -> None:
        st.session_state.yoy_mode = mode_name

        role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
        scope_where = scope.where_clause().replace("customer_name", c(colmap, "customer_name"))
        where_sql = _compose_where(role_filter, scope_where)

        params: Dict[str, Any] = dict(scope.params or {})
        if not is_admin:
            params["login_email"] = login_email

        if mode_name == "ワースト":
            diff_filter = "py_sales > 0 AND (ty_sales - py_sales) < 0"
            order_by = "sales_diff_yoy ASC"
        elif mode_name == "ベスト":
            diff_filter = "py_sales > 0 AND (ty_sales - py_sales) > 0"
            order_by = "sales_diff_yoy DESC"
        else:
            diff_filter = "py_sales = 0 AND ty_sales > 0"
            order_by = "ty_sales DESC"

        sql = f"""
            WITH fy AS (
              SELECT
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                 - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            ),
            base_raw AS (
              SELECT
                COALESCE(
                  NULLIF(NULLIF(TRIM(CAST({c(colmap,'yj_code')} AS STRING)), ''), '0'),
                  NULLIF(NULLIF(TRIM(CAST({c(colmap,'jan_code')} AS STRING)), ''), '0'),
                  REGEXP_REPLACE(CAST({c(colmap,'product_name')} AS STRING), r"[/／].*$", "")
                ) AS yj_key,
                REGEXP_REPLACE(CAST({c(colmap,'product_name')} AS STRING), r"[/／].*$", "") AS product_base,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {where_sql}
              GROUP BY yj_key, product_base
            ),
            base AS (
              SELECT
                yj_key,
                ARRAY_AGG(product_base ORDER BY ty_sales DESC LIMIT 1)[OFFSET(0)] AS product_name,
                SUM(ty_sales) AS ty_sales,
                SUM(py_sales) AS py_sales
              FROM base_raw
              GROUP BY yj_key
            )
            SELECT
              yj_key,
              product_name,
              ty_sales,
              py_sales,
              (ty_sales - py_sales) AS sales_diff_yoy
            FROM base
            WHERE {diff_filter}
            ORDER BY {order_by}
            LIMIT 100
        """
        st.session_state.yoy_df = query_df_safe(client, sql, params, f"YoY Load {mode_name}")

    with c1:
        if st.button("📉 下落幅ワースト", use_container_width=True):
            load_yoy("ワースト")
    with c2:
        if st.button("📈 上昇幅ベスト", use_container_width=True):
            load_yoy("ベスト")
    with c3:
        if st.button("🆕 新規/比較不能", use_container_width=True):
            load_yoy("新規")

    if st.session_state.yoy_df.empty:
        st.info("ランキングを読み込むにはボタンを押してください。")
        return

    df_rank = st.session_state.yoy_df.copy()
    df_rank["product_name"] = df_rank["product_name"].apply(normalize_product_display_name)

    st.markdown(f"#### 🏆 第一階層：成分キー（YJ優先）{st.session_state.yoy_mode} ランキング")
    event = st.dataframe(
        df_rank[["product_name", "ty_sales", "py_sales", "sales_diff_yoy"]].rename(
            columns={
                "product_name": "代表商品名(成分)",
                "ty_sales": "今期売上",
                "py_sales": "前期売上",
                "sales_diff_yoy": "前年比差額",
            }
        ).style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun",
        key=f"grid_yoy_{st.session_state.yoy_mode}",
    )

    try:
        sel_rows = event.selection.rows if hasattr(event, "selection") else []
        if sel_rows:
            st.session_state.selected_yoy_key = str(df_rank.iloc[sel_rows[0]]["yj_key"])
    except Exception:
        pass

    st.divider()

    st.header("🔍 第二階層：詳細分析（スコープ内）")

    key_opts = ["全成分を表示"] + list(df_rank["yj_key"].astype(str).unique())
    display_map = {"全成分を表示": "🚩 スコープ内の全成分を合計して表示"}
    for _, r in df_rank.iterrows():
        k = str(r["yj_key"])
        display_map[k] = f"{normalize_product_display_name(r['product_name'])}（差額: ¥{r['sales_diff_yoy']:,.0f}）"

    idx = 0
    if st.session_state.selected_yoy_key in key_opts:
        idx = key_opts.index(st.session_state.selected_yoy_key)

    selected_key = st.selectbox(
        "詳細を見たい成分キーを選択（[全成分を表示]で全量）",
        options=key_opts,
        index=idx,
        format_func=lambda x: display_map.get(x, x),
    )
    st.session_state.selected_yoy_key = selected_key

    role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
    scope_where = scope.where_clause().replace("customer_name", c(colmap, "customer_name"))

    params: Dict[str, Any] = dict(scope.params or {})
    if not is_admin:
        params["login_email"] = login_email

    key_filter = ""
    if selected_key != "全成分を表示":
        key_expr = f"""
          COALESCE(
            NULLIF(NULLIF(TRIM(CAST({c(colmap,'yj_code')} AS STRING)), ''), '0'),
            NULLIF(NULLIF(TRIM(CAST({c(colmap,'jan_code')} AS STRING)), ''), '0'),
            REGEXP_REPLACE(CAST({c(colmap,'product_name')} AS STRING), r"[/／].*$", "")
          )
        """
        key_filter = f"{' '.join(key_expr.split())} = @target_key"
        params["target_key"] = selected_key

    where_sql = _compose_where(role_filter, scope_where, key_filter)
    sort_order = "ASC" if st.session_state.yoy_mode == "ワースト" else "DESC"

    st.markdown("#### 🧾 得意先別内訳（前年差額）")
    sql_cust = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
           - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
      ),
      base AS (
        SELECT
          {c(colmap,'customer_name')} AS customer_name,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {where_sql}
        GROUP BY customer_name
      )
      SELECT
        customer_name AS `得意先名`,
        ty_sales AS `今期売上`,
        py_sales AS `前期売上`,
        (ty_sales - py_sales) AS `前年差額`
      FROM base
      WHERE ty_sales != 0 OR py_sales != 0
      ORDER BY `前年差額` {sort_order}
      LIMIT 50
    """
    df_cust = query_df_safe(client, sql_cust, params, "YoY Detail Customers")
    if not df_cust.empty:
        st.dataframe(
            df_cust.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("得意先別内訳がありません。")

    st.markdown("#### 🧪 原因追及：JAN・商品別（前年差額寄与）")
    sql_jan = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
           - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
      ),
      base AS (
        SELECT
          CAST({c(colmap,'jan_code')} AS STRING) AS jan,
          REGEXP_REPLACE(CAST({c(colmap,'product_name')} AS STRING), r"[/／].*$", "") AS product_base,
          CAST({c(colmap,'package_unit')} AS STRING) AS package_unit,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {where_sql}
        GROUP BY jan, product_base, package_unit
      )
      SELECT
        jan AS `JAN`,
        product_base AS `代表商品名`,
        package_unit AS `包装`,
        ty_sales AS `今期売上`,
        py_sales AS `前期売上`,
        (ty_sales - py_sales) AS `前年差額`
      FROM base
      WHERE ty_sales != 0 OR py_sales != 0
      ORDER BY `前年差額` {sort_order}
    """
    df_jan = query_df_safe(client, sql_jan, params, "YoY Detail JAN")
    if not df_jan.empty:
        st.dataframe(
            df_jan.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("JAN別内訳がありません。")

    st.markdown("#### 📅 原因追及：月次推移（前年差額）")
    sql_month = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
           - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
      ),
      base AS (
        SELECT
          FORMAT_DATE('%Y-%m', {c(colmap,'sales_date')}) AS ym,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = fy.current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {where_sql}
        GROUP BY ym
      )
      SELECT
        ym AS `年月`,
        ty_sales AS `今期売上`,
        py_sales AS `前期売上`,
        (ty_sales - py_sales) AS `前年差額`
      FROM base
      ORDER BY `年月`
    """
    df_month = query_df_safe(client, sql_month, params, "YoY Detail Month")
    if not df_month.empty:
        st.dataframe(
            df_month.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("月次推移がありません。")


# -----------------------------
# ★ v1.4.10 新規納品トレンド（追加）
# -----------------------------
def _render_df_money(df: pd.DataFrame, money_cols: list[str]) -> None:
    fmt = {}
    for col in money_cols:
        if col in df.columns:
            fmt[col] = "¥{:,.0f}"
    st.dataframe(df.fillna(0).style.format(fmt), use_container_width=True, hide_index=True)


def render_new_deliveries_section(client: bigquery.Client, login_email: str, is_admin: bool, colmap: Dict[str, str]) -> None:
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")

    # ① 期間設定（UIは軽く、処理ルートは増やさない）
    c1, c2, c3 = st.columns([1, 1, 2])
    window_days = int(c1.selectbox("トレンド比較の窓（日数）", options=[7, 14, 30], index=1))
    top_n = int(c2.selectbox("表示件数", options=[10, 20, 50], index=1))
    c3.caption("直近N日 vs その前N日で「新規納品が増えた」対象を抽出（増分テーブルは作らない）")

    if not st.button("新規納品（サマリー＋トレンド）を読み込む", key="btn_new_deliv"):
        return

    # VIEW_NEW_DELIVERY 側は固定列前提：customer_code, customer_name, jan_code, product_name, first_sales_date, sales_amount, gross_profit, login_email
    where_ext = "" if is_admin else "AND login_email = @login_email"
    params = None if is_admin else {"login_email": login_email}

    # ② サマリー（既存踏襲）
    sql_summary = f"""
    WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
    SELECT
      '① 昨日' AS `期間`, COUNT(DISTINCT customer_code) AS `得意先数`, COUNT(DISTINCT jan_code) AS `品目数`, SUM(sales_amount) AS `売上`, SUM(gross_profit) AS `粗利`
    FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE first_sales_date = DATE_SUB(today, INTERVAL 1 DAY) {where_ext}
    UNION ALL
    SELECT '② 直近7日', COUNT(DISTINCT customer_code), COUNT(DISTINCT jan_code), SUM(sales_amount), SUM(gross_profit)
    FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE first_sales_date >= DATE_SUB(today, INTERVAL 7 DAY) {where_ext}
    UNION ALL
    SELECT '③ 当月', COUNT(DISTINCT customer_code), COUNT(DISTINCT jan_code), SUM(sales_amount), SUM(gross_profit)
    FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE DATE_TRUNC(first_sales_date, MONTH) = DATE_TRUNC(today, MONTH) {where_ext}
    ORDER BY `期間`
    """
    df_new = query_df_safe(client, sql_summary, params, label="New Deliveries Summary")
    if not df_new.empty:
        df_new[["売上", "粗利"]] = df_new[["売上", "粗利"]].fillna(0)
        _render_df_money(df_new, ["売上", "粗利"])
    else:
        st.info("新規納品サマリーがありません。")

    st.divider()

    # ③ トレンド：得意先（新規納品が増えている得意先）
    st.markdown("### 📈 得意先トレンド（新規納品が増えている得意先）")

    sql_customer_trend = f"""
    WITH td AS (
      SELECT
        CURRENT_DATE('Asia/Tokyo') AS today,
        DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w DAY) AS w_start,
        DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w*2 DAY) AS prev_start
    ),
    base AS (
      SELECT
        CAST(customer_code AS STRING) AS customer_code,
        ANY_VALUE(customer_name) AS customer_name,
        CASE
          WHEN first_sales_date >= (SELECT w_start FROM td) THEN 'recent'
          WHEN first_sales_date >= (SELECT prev_start FROM td) AND first_sales_date < (SELECT w_start FROM td) THEN 'prev'
          ELSE 'other'
        END AS bucket,
        jan_code,
        sales_amount,
        gross_profit
      FROM `{VIEW_NEW_DELIVERY}`
      CROSS JOIN td
      WHERE first_sales_date >= (SELECT prev_start FROM td)
        {where_ext}
    ),
    agg AS (
      SELECT
        customer_code,
        customer_name,
        SUM(CASE WHEN bucket='recent' THEN 1 ELSE 0 END) AS recent_rows,
        SUM(CASE WHEN bucket='prev' THEN 1 ELSE 0 END) AS prev_rows,
        COUNT(DISTINCT CASE WHEN bucket='recent' THEN jan_code END) AS recent_items,
        COUNT(DISTINCT CASE WHEN bucket='prev' THEN jan_code END) AS prev_items,
        SUM(CASE WHEN bucket='recent' THEN sales_amount ELSE 0 END) AS recent_sales,
        SUM(CASE WHEN bucket='prev' THEN sales_amount ELSE 0 END) AS prev_sales,
        SUM(CASE WHEN bucket='recent' THEN gross_profit ELSE 0 END) AS recent_gp,
        SUM(CASE WHEN bucket='prev' THEN gross_profit ELSE 0 END) AS prev_gp
      FROM base
      GROUP BY customer_code, customer_name
    )
    SELECT
      customer_code AS `得意先CD`,
      customer_name AS `得意先名`,
      recent_items AS `直近{window_days}日_新規品目数`,
      prev_items AS `前{window_days}日_新規品目数`,
      (recent_items - prev_items) AS `増減_品目数`,
      recent_sales AS `直近{window_days}日_売上`,
      prev_sales AS `前{window_days}日_売上`,
      (recent_sales - prev_sales) AS `増減_売上`,
      recent_gp AS `直近{window_days}日_粗利`,
      prev_gp AS `前{window_days}日_粗利`,
      (recent_gp - prev_gp) AS `増減_粗利`
    FROM agg
    WHERE (recent_items - prev_items) > 0
       OR (recent_sales - prev_sales) > 0
    ORDER BY `増減_品目数` DESC, `増減_売上` DESC
    LIMIT @topn
    """
    trend_params = {"w": window_days, "topn": top_n}
    if not is_admin:
        trend_params.update({"login_email": login_email})
    df_ct = query_df_safe(client, sql_customer_trend, trend_params, "New Delivery Trend Customers")
    if not df_ct.empty:
        _render_df_money(df_ct, ["直近14日_売上", "前14日_売上", "増減_売上", "直近14日_粗利", "前14日_粗利", "増減_粗利"])
        # 上の金額列名は window_days により変わるため、動的に当てる
        money_cols = [c for c in df_ct.columns if any(k in c for k in ["売上", "粗利"]) and "品目" not in c]
        _render_df_money(df_ct, money_cols)
    else:
        st.info("増加傾向の得意先がありません（または抽出期間にデータがありません）。")

    st.divider()

    # ④ トレンド：グループ（切り口）
    st.markdown("### 🏢 グループトレンド（新規納品が増えているグループ）")
    group_expr, group_src = resolve_customer_group_sql_expr(client)
    if not group_expr:
        st.info("グループ列が見つからないため、グループトレンドは表示できません。")
    else:
        if group_src:
            st.caption(f"グループ抽出元: `{group_src}`（VIEW_UNIFIED）")

        sql_group_trend = f"""
        WITH td AS (
          SELECT
            CURRENT_DATE('Asia/Tokyo') AS today,
            DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w DAY) AS w_start,
            DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w*2 DAY) AS prev_start
        ),
        nd AS (
          SELECT
            CAST(customer_code AS STRING) AS customer_code,
            jan_code,
            sales_amount,
            gross_profit,
            CASE
              WHEN first_sales_date >= (SELECT w_start FROM td) THEN 'recent'
              WHEN first_sales_date >= (SELECT prev_start FROM td) AND first_sales_date < (SELECT w_start FROM td) THEN 'prev'
              ELSE 'other'
            END AS bucket
          FROM `{VIEW_NEW_DELIVERY}`
          CROSS JOIN td
          WHERE first_sales_date >= (SELECT prev_start FROM td)
            {where_ext}
        ),
        dim_group AS (
          SELECT
            CAST({c(colmap,'customer_code')} AS STRING) AS customer_code,
            {group_expr} AS group_name
          FROM `{VIEW_UNIFIED}`
          GROUP BY customer_code, group_name
        ),
        base AS (
          SELECT
            COALESCE(dg.group_name, '未設定') AS group_name,
            nd.bucket,
            nd.jan_code,
            nd.sales_amount,
            nd.gross_profit
          FROM nd
          LEFT JOIN dim_group dg USING(customer_code)
        ),
        agg AS (
          SELECT
            group_name,
            COUNT(DISTINCT CASE WHEN bucket='recent' THEN jan_code END) AS recent_items,
            COUNT(DISTINCT CASE WHEN bucket='prev' THEN jan_code END) AS prev_items,
            SUM(CASE WHEN bucket='recent' THEN sales_amount ELSE 0 END) AS recent_sales,
            SUM(CASE WHEN bucket='prev' THEN sales_amount ELSE 0 END) AS prev_sales,
            SUM(CASE WHEN bucket='recent' THEN gross_profit ELSE 0 END) AS recent_gp,
            SUM(CASE WHEN bucket='prev' THEN gross_profit ELSE 0 END) AS prev_gp
          FROM base
          GROUP BY group_name
        )
        SELECT
          group_name AS `グループ`,
          recent_items AS `直近{window_days}日_新規品目数`,
          prev_items AS `前{window_days}日_新規品目数`,
          (recent_items - prev_items) AS `増減_品目数`,
          recent_sales AS `直近{window_days}日_売上`,
          prev_sales AS `前{window_days}日_売上`,
          (recent_sales - prev_sales) AS `増減_売上`,
          recent_gp AS `直近{window_days}日_粗利`,
          prev_gp AS `前{window_days}日_粗利`,
          (recent_gp - prev_gp) AS `増減_粗利`
        FROM agg
        WHERE (recent_items - prev_items) > 0
           OR (recent_sales - prev_sales) > 0
        ORDER BY `増減_品目数` DESC, `増減_売上` DESC
        LIMIT @topn
        """
        gp_params = {"w": window_days, "topn": top_n}
        if not is_admin:
            gp_params.update({"login_email": login_email})
        df_gt = query_df_safe(client, sql_group_trend, gp_params, "New Delivery Trend Groups")
        if not df_gt.empty:
            money_cols = [c for c in df_gt.columns if any(k in c for k in ["売上", "粗利"]) and "品目" not in c]
            _render_df_money(df_gt, money_cols)
        else:
            st.info("増加傾向のグループがありません（または抽出期間にデータがありません）。")

    st.divider()

    # ⑤ トレンド：商品（新規納品が増えている商品）
    st.markdown("### 💊 商品トレンド（新規納品が増えている商品）")

    sql_product_trend = f"""
    WITH td AS (
      SELECT
        CURRENT_DATE('Asia/Tokyo') AS today,
        DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w DAY) AS w_start,
        DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL @w*2 DAY) AS prev_start
    ),
    base AS (
      SELECT
        CAST(jan_code AS STRING) AS jan_code,
        REGEXP_REPLACE(CAST(product_name AS STRING), r"[/／].*$", "") AS product_base,
        CASE
          WHEN first_sales_date >= (SELECT w_start FROM td) THEN 'recent'
          WHEN first_sales_date >= (SELECT prev_start FROM td) AND first_sales_date < (SELECT w_start FROM td) THEN 'prev'
          ELSE 'other'
        END AS bucket,
        sales_amount,
        gross_profit,
        customer_code
      FROM `{VIEW_NEW_DELIVERY}`
      CROSS JOIN td
      WHERE first_sales_date >= (SELECT prev_start FROM td)
        {where_ext}
    ),
    agg AS (
      SELECT
        jan_code,
        product_base,
        COUNT(DISTINCT CASE WHEN bucket='recent' THEN customer_code END) AS recent_customers,
        COUNT(DISTINCT CASE WHEN bucket='prev' THEN customer_code END) AS prev_customers,
        SUM(CASE WHEN bucket='recent' THEN sales_amount ELSE 0 END) AS recent_sales,
        SUM(CASE WHEN bucket='prev' THEN sales_amount ELSE 0 END) AS prev_sales,
        SUM(CASE WHEN bucket='recent' THEN gross_profit ELSE 0 END) AS recent_gp,
        SUM(CASE WHEN bucket='prev' THEN gross_profit ELSE 0 END) AS prev_gp
      FROM base
      GROUP BY jan_code, product_base
    )
    SELECT
      jan_code AS `JAN`,
      product_base AS `代表商品名(成分)`,
      recent_customers AS `直近{window_days}日_新規得意先数`,
      prev_customers AS `前{window_days}日_新規得意先数`,
      (recent_customers - prev_customers) AS `増減_得意先数`,
      recent_sales AS `直近{window_days}日_売上`,
      prev_sales AS `前{window_days}日_売上`,
      (recent_sales - prev_sales) AS `増減_売上`,
      recent_gp AS `直近{window_days}日_粗利`,
      prev_gp AS `前{window_days}日_粗利`,
      (recent_gp - prev_gp) AS `増減_粗利`
    FROM agg
    WHERE (recent_customers - prev_customers) > 0
       OR (recent_sales - prev_sales) > 0
    ORDER BY `増減_得意先数` DESC, `増減_売上` DESC
    LIMIT @topn
    """
    pr_params = {"w": window_days, "topn": top_n}
    if not is_admin:
        pr_params.update({"login_email": login_email})
    df_pt = query_df_safe(client, sql_product_trend, pr_params, "New Delivery Trend Products")
    if not df_pt.empty:
        money_cols = [c for c in df_pt.columns if any(k in c for k in ["売上", "粗利"]) and "得意先" not in c]
        _render_df_money(df_pt, money_cols)
    else:
        st.info("増加傾向の新規納品商品がありません（または抽出期間にデータがありません）。")


def render_adoption_alerts_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
    st.subheader("🚨 採用アイテム・失注アラート")
    where_clause = "" if is_admin else "WHERE login_email = @login_email"
    params = None if is_admin else {"login_email": login_email}
    sql = f"""
        SELECT
            staff_name AS `担当者名`,
            customer_name AS `得意先名`,
            product_name AS `商品名`,
            last_purchase_date AS `最終購入日`,
            adoption_status AS `ステータス`,
            current_fy_sales AS `今期売上`,
            previous_fy_sales AS `前期売上`,
            (current_fy_sales - previous_fy_sales) AS `売上差額`
        FROM `{VIEW_ADOPTION}`
        {where_clause}
        ORDER BY
            CASE
                WHEN adoption_status LIKE '%🔴%' THEN 1
                WHEN adoption_status LIKE '%🟡%' THEN 2
                ELSE 3
            END,
            `売上差額` ASC
    """
    df_alerts = query_df_safe(client, sql, params, "Adoption Alerts")
    if df_alerts.empty:
        st.info("現在、アラート対象のアイテムはありません。")
        return

    df_alerts["担当者名"] = df_alerts["担当者名"].fillna("未設定")
    col1, col2 = st.columns(2)
    with col1:
        selected_status = st.multiselect(
            "🎯 ステータスで絞り込み",
            options=df_alerts["ステータス"].unique(),
            default=[s for s in df_alerts["ステータス"].unique() if "🟡" in s or "🔴" in s],
        )
    with col2:
        all_staffs = sorted(df_alerts["担当者名"].unique().tolist())
        selected_staffs = st.multiselect("👤 担当者で絞り込み", options=all_staffs, default=[])

    df_display = df_alerts.copy()
    if selected_status:
        df_display = df_display[df_display["ステータス"].isin(selected_status)]
    if selected_staffs:
        df_display = df_display[df_display["担当者名"].isin(selected_staffs)]

    if df_display.empty:
        st.info("選択された条件に一致するアイテムはありません。")
        return

    for col in ["今期売上", "前期売上", "売上差額"]:
        df_display[col] = pd.to_numeric(df_display[col], errors="coerce").fillna(0)

    st.dataframe(
        df_display.style.format(
            {
                "今期売上": "¥{:,.0f}",
                "前期売上": "¥{:,.0f}",
                "売上差額": "¥{:,.0f}",
                "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else "",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_customer_drilldown(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter, colmap: Dict[str, str]) -> None:
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")

    role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
    scope_filter = scope.where_clause().replace("customer_name", c(colmap, "customer_name"))
    customer_where = _compose_where(role_filter, scope_filter, f"{c(colmap,'customer_name')} IS NOT NULL")

    customer_params: Dict[str, Any] = dict(scope.params or {})
    if not is_admin:
        customer_params["login_email"] = login_email

    sql_cust = f"""
        SELECT DISTINCT {c(colmap,'customer_code')} AS customer_code, {c(colmap,'customer_name')} AS customer_name
        FROM `{VIEW_UNIFIED}`
        {customer_where}
    """
    df_cust = query_df_safe(client, sql_cust, customer_params, "Scoped Customers")
    if df_cust.empty:
        st.info("表示できる得意先データがありません。")
        return

    search_term = st.text_input("🔍 得意先名で検索（一部入力）", placeholder="例：古賀")
    filtered_df = df_cust[df_cust["customer_name"].str.contains(search_term, na=False)] if search_term else df_cust
    if filtered_df.empty:
        st.info("検索条件に一致する得意先がありません。")
        return

    opts = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in filtered_df.iterrows()}
    sel = st.selectbox("得意先を選択", options=list(opts.keys()), format_func=lambda x: opts[x])
    if not sel:
        return

    st.divider()
    st.markdown("##### 📦 現在の採用アイテム（稼働状況）")
    sql_adopt = f"""
        SELECT
            product_name AS `商品名`,
            adoption_status AS `ステータス`,
            last_purchase_date AS `最終購入日`,
            current_fy_sales AS `今期売上`,
            previous_fy_sales AS `前期売上`
        FROM `{VIEW_ADOPTION}`
        WHERE customer_code = @c
        ORDER BY
            CASE
                WHEN adoption_status LIKE '%🟢%' THEN 1
                WHEN adoption_status LIKE '%🟡%' THEN 2
                ELSE 3
            END,
            current_fy_sales DESC
    """
    df_adopt = query_df_safe(client, sql_adopt, {"c": sel}, "Customer Adoption")
    if not df_adopt.empty:
        for col in ["今期売上", "前期売上"]:
            df_adopt[col] = pd.to_numeric(df_adopt[col], errors="coerce").fillna(0)
        st.dataframe(
            df_adopt.style.format(
                {
                    "今期売上": "¥{:,.0f}",
                    "前期売上": "¥{:,.0f}",
                    "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else "",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("この得意先の採用データはありません。")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("##### 💡 AI 推奨提案商品（Reco）")

    sql_rec = f"""
        SELECT
          customer_name,
          strong_category,
          priority_rank,
          recommend_jan,
          recommend_product,
          manufacturer,
          market_scale
        FROM `{VIEW_RECOMMEND}`
        WHERE CAST(customer_code AS STRING) = @c
        ORDER BY priority_rank ASC
        LIMIT 10
    """
    df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
    if not df_rec.empty:
        df_disp = df_rec[["priority_rank", "recommend_product", "manufacturer", "strong_category", "market_scale"]].rename(
            columns={
                "priority_rank": "順位",
                "recommend_product": "推奨商品",
                "manufacturer": "メーカー",
                "strong_category": "強み分類",
                "market_scale": "市場規模",
            }
        )
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
    else:
        st.info("現在、この得意先への推奨商品はありません。")


# -----------------------------
# 5. Main Loop
# -----------------------------
def main() -> None:
    set_page()
    client = setup_bigquery_client()

    colmap = resolve_unified_colmap(client)
    missing = colmap.get("_missing_required")
    if missing:
        st.error("VIEW_UNIFIED の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing}")
        st.stop()

    with st.sidebar:
        st.header("🔑 ログイン")
        login_id = st.text_input("ログインID (メールアドレス)")
        login_pw = st.text_input("パスコード (携帯下4桁)", type="password")

        st.divider()
        st.session_state.use_bqstorage = st.checkbox("高速読込 (Storage API)", value=True)

        if st.button("📡 通信ヘルスチェック"):
            try:
                client.query("SELECT 1").result(timeout=10)
                st.success("BigQuery 接続正常")
            except Exception as e:
                st.error(f"接続エラー: {e}")

        if st.button("🧹 キャッシュクリア"):
            st.cache_data.clear()
            st.cache_resource.clear()

        with st.expander("🔧 VIEW_UNIFIED 列マップ（自動解決結果）", expanded=False):
            st.json(colmap)

    if not login_id or not login_pw:
        st.info("👈 サイドバーからログインしてください。")
        return

    role = resolve_role(client, login_id.strip(), login_pw.strip())
    if not role.is_authenticated:
        st.error("❌ ログイン情報が正しくありません。")
        return

    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    c1, c2, c3 = st.columns(3)
    c1.metric("👤 担当", role.staff_name)
    c2.metric("🛡️ 権限", role.role_key)
    c3.metric("📞 電話", role.phone)
    st.divider()

    if role.role_admin_view:
        render_fytd_org_section(client, colmap)
    else:
        render_fytd_me_section(client, role.login_email, colmap)

    st.divider()

    scope = render_scope_filters(client, role)
    st.divider()

    if role.role_admin_view:
        render_group_underperformance_section(client, role, scope, colmap)
        st.divider()
        render_yoy_section(client, role.login_email, is_admin=True, scope=scope, colmap=colmap)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=True, colmap=colmap)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=True)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=True, scope=scope, colmap=colmap)
    else:
        render_yoy_section(client, role.login_email, is_admin=False, scope=scope, colmap=colmap)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=False, colmap=colmap)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=False)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=False, scope=scope, colmap=colmap)


if __name__ == "__main__":
    main()
