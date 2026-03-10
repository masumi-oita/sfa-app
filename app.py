# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.5.0 (Stable Summary Fix Edition)

【v1.4.9 踏襲機能】
- YoY：VIEW_UNIFIED から動的集計に統一
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定
- Group Display: official先頭 + raw併記
- 得意先グループ / 得意先単体の切替 ＆ 商品要因ドリルダウン（全件表示）
- 順位アイコンの追加と、不要なYJコード列の非表示
- ColMap（列名吸収）導入：jan/jan_code 等の差異を自動解決
- VIEW_NEW_DELIVERY 必須列不足時の自動補完
- YoY第二階層を「全成分を表示」デフォルト対応
- 冗長なラジオボタンを撤廃し、データフレーム（表）のクリック連動にUIを洗練化
- 成長率に「¥」がつくフォーマット干渉バグを撤廃
- 包装（パッケージ）の表示を完全復活
- 体外診断薬等の名称が「/」で途切れる旧ロジックを完全撤廃
- 指数表記化したJANをJOINキーに使う事故を根絶（名寄せキーは yj_code / product_name）
- メーカー別パフォーマンス（前期/今期 売上・粗利）セクション
- サマリーに総薬価・加重平均を追加

【v1.5.0 今回の確定修正】
- 当月実績を CURRENT_DATE のみで見に行く旧ロジックを修正
- 当月データ未反映時に「0円」ではなく「— / 未反映」表示へ変更
- FYTD前年同期の比較基準を CURRENT_DATE ではなく MAX(sales_date) 基準へ補正
- 最新反映日 / 最新取込月 / 最新確定月 / 反映遅延日数を表示
- 総薬価列が無い場合でも安全に動作するよう修正
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Iterable, List

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
    st.caption("OS v1.5.0｜Stable Summary Fix Edition（当月0円誤表示・比較軸ズレ修正済）")


def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
    for col in df.columns:
        if any(k in col for k in ["率", "比", "ペース", "成長"]):
            config[col] = st.column_config.NumberColumn(col, format="%.1f%%")
        elif any(k in col for k in ["売上", "粗利", "金額", "差額", "実績", "予測", "GAP"]):
            config[col] = st.column_config.NumberColumn(col)
        elif "日" in col or pd.api.types.is_datetime64_any_dtype(df[col]):
            config[col] = st.column_config.DateColumn(col, format="YYYY-MM-DD")
        elif is_numeric_dtype(df[col]):
            config[col] = st.column_config.NumberColumn(col)
        else:
            config[col] = st.column_config.TextColumn(col)
    return config


def get_safe_float(row: pd.Series, key: str) -> float:
    val = row.get(key)
    return float(val) if val is not None and not pd.isna(val) else 0.0


def get_nullable_float(row: pd.Series, key: str) -> Optional[float]:
    val = row.get(key)
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    try:
        return float(val)
    except Exception:
        return None


def normalize_product_display_name(name: Any) -> str:
    if pd.isna(name):
        return ""
    return str(name).strip()


def normalize_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def fmt_yen_or_dash(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"¥{v:,.0f}"


def fmt_pct_or_dash(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:,.1f}%"


def fmt_delta_yen(cur: Optional[float], prev: Optional[float]) -> Optional[str]:
    if cur is None or prev is None:
        return None
    return f"{int(cur - prev):,}"


def fmt_delta_pct(cur: Optional[float], prev: Optional[float]) -> Optional[str]:
    if cur is None or prev is None:
        return None
    return f"{cur - prev:,.1f}%"


def fmt_date_or_dash(v: Any) -> str:
    if v is None:
        return "—"
    try:
        if pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%d")
    except Exception:
        return str(v)


def fmt_month_or_dash(v: Any) -> str:
    if v is None:
        return "—"
    try:
        if pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        return pd.to_datetime(v).strftime("%Y-%m")
    except Exception:
        return str(v)


def project_value(cur: Optional[float], py_ytd: Optional[float], py_total: Optional[float]) -> Optional[float]:
    if cur is None:
        return None
    if py_ytd is not None and py_total is not None and py_ytd > 0:
        return cur * (py_total / py_ytd)
    return cur


def safe_rate(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    return numerator / denominator * 100.0


def sql_numeric_expr(colmap: Dict[str, str], key: str) -> str:
    col = colmap.get(key)
    if col:
        return f"SAFE_CAST({col} AS FLOAT64)"
    return "CAST(NULL AS FLOAT64)"


def sql_int_expr(colmap: Dict[str, str], key: str) -> str:
    col = colmap.get(key)
    if col:
        return f"SAFE_CAST({col} AS INT64)"
    return "CAST(NULL AS INT64)"


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


def _build_query_parameter(key: str, value: Any) -> bigquery.QueryParameter:
    if isinstance(value, tuple) and len(value) == 2:
        p_type, p_value = value
        p_type = str(p_type).upper()
        if p_type.startswith("ARRAY<") and isinstance(p_value, (list, tuple)):
            return bigquery.ArrayQueryParameter(key, "STRING", [None if v is None else str(v) for v in p_value])
        return bigquery.ScalarQueryParameter(key, p_type, p_value)

    if isinstance(value, (list, tuple)):
        return bigquery.ArrayQueryParameter(key, "STRING", [None if v is None else str(v) for v in value])

    if value is None:
        return bigquery.ScalarQueryParameter(key, "STRING", None)
    if isinstance(value, bool):
        return bigquery.ScalarQueryParameter(key, "BOOL", value)
    if isinstance(value, int):
        return bigquery.ScalarQueryParameter(key, "INT64", value)
    if isinstance(value, float):
        return bigquery.ScalarQueryParameter(key, "FLOAT64", value)
    if isinstance(value, pd.Timestamp):
        return bigquery.ScalarQueryParameter(key, "TIMESTAMP", value.to_pydatetime())

    return bigquery.ScalarQueryParameter(key, "STRING", str(value))


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
            job_config.query_parameters = [_build_query_parameter(k, v) for k, v in params.items()]

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


# -----------------------------
# ★ ColMap汎用（任意VIEWの列名揺れ吸収）
# -----------------------------
@st.cache_data(ttl=3600)
def get_view_columns(_client: bigquery.Client, view_fqn: str) -> set[str]:
    project_id, dataset_id, table_name = _split_table_fqn(view_fqn)
    sql = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
    """
    df = query_df_safe(_client, sql, {"table_name": table_name}, f"Schema Check: {view_fqn}")
    if df.empty or "column_name" not in df.columns:
        return set()
    return {str(c).lower() for c in df["column_name"].dropna().tolist()}


def _pick_from(cols: set[str], *cands: str) -> Optional[str]:
    for c_ in cands:
        if c_ and c_.lower() in cols:
            return c_.lower()
    return None


def resolve_view_colmap(
    _client: bigquery.Client,
    view_fqn: str,
    mapping: Dict[str, Iterable[str]],
    required_keys: Iterable[str],
    optional_mapping: Optional[Dict[str, Iterable[str]]] = None,
) -> Dict[str, str]:
    cols = get_view_columns(_client, view_fqn)
    colmap: Dict[str, Optional[str]] = {}

    for logical_key, cands in mapping.items():
        colmap[logical_key] = _pick_from(cols, *list(cands))

    if optional_mapping:
        for logical_key, cands in optional_mapping.items():
            v = _pick_from(cols, *list(cands))
            if v:
                colmap[logical_key] = v

    missing = [k for k in required_keys if not colmap.get(k)]
    if missing:
        colmap["_missing_required"] = ",".join(missing)

    return {k: v for k, v in colmap.items() if v is not None}


def c(colmap: Dict[str, str], key: str) -> str:
    return colmap.get(key, key)


# -----------------------------
# VIEW_UNIFIED系
# -----------------------------
@st.cache_data(ttl=3600)
def get_unified_columns(_client: bigquery.Client) -> set[str]:
    return get_view_columns(_client, VIEW_UNIFIED)


def get_available_customer_group_columns(_client: bigquery.Client) -> list[str]:
    columns = get_unified_columns(_client)
    return [col for col in CUSTOMER_GROUP_COLUMN_CANDIDATES if col in columns]


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
# ★ v1.4.9 ColMap（列名吸収）: VIEW_UNIFIED
# -----------------------------
@st.cache_data(ttl=3600)
def resolve_unified_colmap(_client: bigquery.Client) -> Dict[str, str]:
    mapping = {
        "customer_code": ("customer_code", "得意先コード", "得意先CD"),
        "customer_name": ("customer_name", "得意先名"),
        "login_email": ("login_email", "email", "担当者メール", "担当メール", "login"),
        "sales_date": ("sales_date", "販売日", "date"),
        "fiscal_year": ("fiscal_year", "年度", "fy"),
        "sales_amount": ("sales_amount", "売上", "合計価格", "sales"),
        "gross_profit": ("gross_profit", "粗利", "gp"),
        "product_name": ("product_name", "商品名", "商品名称", "item_name", "品目名"),
        "yj_code": ("yj_code", "yjcode", "yj", "YJCode"),
        "jan_code": ("jan_code", "jan", "JAN"),
        "package_unit": ("package_unit", "pack_unit", "包装単位", "包装"),
        "manufacturer": ("manufacturer", "maker_name", "maker", "メーカー", "製造元", "製造販売元", "manufacturer_name"),
        "total_drug_price": ("total_drug_price", "総薬価", "薬価金額"),
    }
    optional = {
        "staff_name": ("staff_name", "担当者名", "担当社員名", "担当社員氏", "担当"),
    }
    required = (
        "customer_code",
        "customer_name",
        "sales_date",
        "fiscal_year",
        "sales_amount",
        "gross_profit",
        "product_name",
    )
    return resolve_view_colmap(_client, VIEW_UNIFIED, mapping, required, optional)


# -----------------------------
# ★ v1.4.9 ColMap: VIEW_NEW_DELIVERY
# -----------------------------
@st.cache_data(ttl=3600)
def resolve_new_delivery_colmap(_client: bigquery.Client) -> Dict[str, str]:
    mapping = {
        "first_sales_date": ("first_sales_date", "初回納品日", "first_date", "date"),
        "customer_code": ("customer_code", "得意先コード", "得意先CD"),
        "customer_name": ("customer_name", "得意先名", "cust_name", "customer"),
        "jan_code": ("jan_code", "jan", "JAN"),
        "product_name": ("product_name", "item_name", "商品名", "商品名称", "品目名", "drug_name"),
        "sales_amount": ("sales_amount", "売上", "sales"),
        "gross_profit": ("gross_profit", "粗利", "gp"),
        "login_email": ("login_email", "email", "担当者メール", "担当メール"),
        "staff_name": ("staff_name", "担当者名", "担当社員名", "担当"),
    }
    required = ("first_sales_date", "customer_code", "jan_code", "sales_amount", "gross_profit")
    optional = {
        "customer_name": ("customer_name", "得意先名", "cust_name", "customer"),
        "product_name": ("product_name", "item_name", "商品名", "商品名称", "品目名", "drug_name"),
        "login_email": ("login_email", "email", "担当者メール", "担当メール"),
        "staff_name": ("staff_name", "担当者名", "担当社員名", "担当"),
    }
    return resolve_view_colmap(_client, VIEW_NEW_DELIVERY, mapping, required, optional)


# -----------------------------
# スコープ設定
# -----------------------------
def render_scope_filters(client: bigquery.Client, role: RoleInfo, colmap: Dict[str, str]) -> ScopeFilter:
    st.markdown("### 🔍 分析スコープ設定")
    predicates: list[str] = []
    params: Dict[str, Any] = {}

    with st.expander("詳細絞り込み（得意先グループ・得意先名）", expanded=False):
        c1_, c2_ = st.columns(2)

        group_expr, group_src = resolve_customer_group_sql_expr(client)
        if group_expr:
            role_where = ""
            role_params: Dict[str, Any] = {}
            if not role.role_admin_view:
                role_where = f"WHERE {c(colmap,'login_email')} = @login_email"
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
            selected_group = c1_.selectbox("得意先グループ", options=group_opts)
            if selected_group != "指定なし":
                predicates.append(f"{group_expr} = @scope_group")
                params["scope_group"] = selected_group

            if group_src:
                c1_.caption(f"抽出元: `{group_src}`")
        else:
            c1_.caption("グループ列なし（VIEW_UNIFIEDに該当列が存在しません）")

        keyword = c2_.text_input("得意先名（部分一致）", placeholder="例：古賀病院")
        if keyword.strip():
            predicates.append(f"{c(colmap,'customer_name')} LIKE @scope_customer_name")
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
# 4. Summary Query Builder
# -----------------------------
def build_summary_sql(colmap: Dict[str, str], scoped_by_login: bool = False) -> str:
    sales_date_col = c(colmap, "sales_date")
    fiscal_year_expr = sql_int_expr(colmap, "fiscal_year")
    sales_expr = sql_numeric_expr(colmap, "sales_amount")
    gp_expr = sql_numeric_expr(colmap, "gross_profit")
    dp_expr = sql_numeric_expr(colmap, "total_drug_price")
    where_sql = f"WHERE {c(colmap,'login_email')} = @login_email" if scoped_by_login else ""

    return f"""
        WITH base AS (
          SELECT
            CAST({sales_date_col} AS DATE) AS sales_date,
            {fiscal_year_expr} AS fiscal_year,
            {sales_expr} AS sales_amount,
            {gp_expr} AS gross_profit,
            {dp_expr} AS drug_price
          FROM `{VIEW_UNIFIED}`
          {where_sql}
        ),
        meta AS (
          SELECT
            MAX(sales_date) AS max_sales_date,
            DATE_TRUNC(MAX(sales_date), MONTH) AS latest_loaded_month,
            DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH) AS calendar_month,
            DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR), MONTH) AS py_calendar_month,
            DATE_SUB(MAX(sales_date), INTERVAL 1 YEAR) AS py_same_day,
            (
              EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
              - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
            ) AS current_fy,
            CASE
              WHEN MAX(sales_date) IS NULL THEN NULL
              WHEN MAX(sales_date) = DATE_SUB(DATE_ADD(DATE_TRUNC(MAX(sales_date), MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)
                THEN DATE_TRUNC(MAX(sales_date), MONTH)
              ELSE DATE_SUB(DATE_TRUNC(MAX(sales_date), MONTH), INTERVAL 1 MONTH)
            END AS latest_closed_month
          FROM base
        ),
        agg AS (
          SELECT
            COUNTIF(DATE_TRUNC(b.sales_date, MONTH) = m.calendar_month) AS calendar_month_rows,

            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.calendar_month, b.sales_amount, NULL)) AS calendar_month_sales,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.calendar_month, b.gross_profit, NULL)) AS calendar_month_profit,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.calendar_month, b.drug_price, NULL)) AS calendar_month_drug_price,

            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.py_calendar_month, b.sales_amount, NULL)) AS calendar_month_sales_py,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.py_calendar_month, b.gross_profit, NULL)) AS calendar_month_profit_py,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.py_calendar_month, b.drug_price, NULL)) AS calendar_month_drug_price_py,

            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_loaded_month, b.sales_amount, NULL)) AS latest_loaded_month_sales,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_loaded_month, b.gross_profit, NULL)) AS latest_loaded_month_profit,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_loaded_month, b.drug_price, NULL)) AS latest_loaded_month_drug_price,

            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_closed_month, b.sales_amount, NULL)) AS latest_closed_month_sales,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_closed_month, b.gross_profit, NULL)) AS latest_closed_month_profit,
            SUM(IF(DATE_TRUNC(b.sales_date, MONTH) = m.latest_closed_month, b.drug_price, NULL)) AS latest_closed_month_drug_price,

            SUM(IF(b.fiscal_year = m.current_fy, b.sales_amount, 0)) AS sales_amount_fytd,
            SUM(IF(b.fiscal_year = m.current_fy, b.gross_profit, 0)) AS gross_profit_fytd,
            SUM(IF(b.fiscal_year = m.current_fy, b.drug_price, 0)) AS drug_price_fytd,

            SUM(IF(b.fiscal_year = m.current_fy - 1 AND b.sales_date <= m.py_same_day, b.sales_amount, 0)) AS sales_amount_py_ytd,
            SUM(IF(b.fiscal_year = m.current_fy - 1 AND b.sales_date <= m.py_same_day, b.gross_profit, 0)) AS gross_profit_py_ytd,
            SUM(IF(b.fiscal_year = m.current_fy - 1 AND b.sales_date <= m.py_same_day, b.drug_price, 0)) AS drug_price_py_ytd,

            SUM(IF(b.fiscal_year = m.current_fy - 1, b.sales_amount, 0)) AS sales_amount_py_total,
            SUM(IF(b.fiscal_year = m.current_fy - 1, b.gross_profit, 0)) AS gross_profit_py_total,
            SUM(IF(b.fiscal_year = m.current_fy - 1, b.drug_price, 0)) AS drug_price_py_total
          FROM base b
          CROSS JOIN meta m
        )
        SELECT
          IFNULL(a.sales_amount_fytd, 0) AS sales_amount_fytd,
          IFNULL(a.gross_profit_fytd, 0) AS gross_profit_fytd,
          a.drug_price_fytd AS drug_price_fytd,

          IFNULL(a.sales_amount_py_ytd, 0) AS sales_amount_py_ytd,
          IFNULL(a.gross_profit_py_ytd, 0) AS gross_profit_py_ytd,
          a.drug_price_py_ytd AS drug_price_py_ytd,

          IFNULL(a.sales_amount_py_total, 0) AS sales_amount_py_total,
          IFNULL(a.gross_profit_py_total, 0) AS gross_profit_py_total,
          a.drug_price_py_total AS drug_price_py_total,

          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_sales ELSE NULL END AS display_current_month_sales,
          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_profit ELSE NULL END AS display_current_month_profit,
          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_drug_price ELSE NULL END AS display_current_month_drug_price,

          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_sales_py ELSE NULL END AS display_current_month_sales_py,
          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_profit_py ELSE NULL END AS display_current_month_profit_py,
          CASE WHEN a.calendar_month_rows > 0 THEN a.calendar_month_drug_price_py ELSE NULL END AS display_current_month_drug_price_py,

          a.latest_loaded_month_sales,
          a.latest_loaded_month_profit,
          a.latest_loaded_month_drug_price,

          a.latest_closed_month_sales,
          a.latest_closed_month_profit,
          a.latest_closed_month_drug_price,

          a.calendar_month_rows,
          m.max_sales_date,
          m.latest_loaded_month,
          m.latest_closed_month,
          m.calendar_month,
          CASE
            WHEN m.max_sales_date IS NULL THEN 'NO_DATA'
            WHEN m.latest_loaded_month = m.calendar_month THEN 'CURRENT_MONTH_AVAILABLE'
            ELSE 'CURRENT_MONTH_MISSING'
          END AS refresh_status,
          CASE
            WHEN m.max_sales_date IS NULL THEN NULL
            ELSE DATE_DIFF(CURRENT_DATE('Asia/Tokyo'), m.max_sales_date, DAY)
          END AS lag_days
        FROM meta m
        CROSS JOIN agg a
    """


# -----------------------------
# 5. UI Sections
# -----------------------------
def render_summary_metrics(row: pd.Series) -> None:
    # 累計系
    s_cur = get_safe_float(row, "sales_amount_fytd")
    s_py_ytd = get_safe_float(row, "sales_amount_py_ytd")
    s_py_total = get_safe_float(row, "sales_amount_py_total")
    s_fc = project_value(s_cur, s_py_ytd, s_py_total)

    gp_cur = get_safe_float(row, "gross_profit_fytd")
    gp_py_ytd = get_safe_float(row, "gross_profit_py_ytd")
    gp_py_total = get_safe_float(row, "gross_profit_py_total")
    gp_fc = project_value(gp_cur, gp_py_ytd, gp_py_total)

    dp_cur = get_nullable_float(row, "drug_price_fytd")
    dp_py_ytd = get_nullable_float(row, "drug_price_py_ytd")
    dp_py_total = get_nullable_float(row, "drug_price_py_total")
    dp_fc = project_value(dp_cur, dp_py_ytd, dp_py_total)

    # 当月（未反映時は NULL を維持）
    s_cm = get_nullable_float(row, "display_current_month_sales")
    s_py_cm = get_nullable_float(row, "display_current_month_sales_py")
    gp_cm = get_nullable_float(row, "display_current_month_profit")
    gp_py_cm = get_nullable_float(row, "display_current_month_profit_py")
    dp_cm = get_nullable_float(row, "display_current_month_drug_price")
    dp_py_cm = get_nullable_float(row, "display_current_month_drug_price_py")

    # 参考情報
    latest_loaded_month = row.get("latest_loaded_month")
    latest_closed_month = row.get("latest_closed_month")
    max_sales_date = row.get("max_sales_date")
    refresh_status = normalize_text(row.get("refresh_status"))
    lag_days_raw = row.get("lag_days")
    lag_days: Optional[int] = None
    if lag_days_raw is not None:
        try:
            if not pd.isna(lag_days_raw):
                lag_days = int(lag_days_raw)
        except Exception:
            lag_days = None

    latest_loaded_month_sales = get_nullable_float(row, "latest_loaded_month_sales")
    latest_loaded_month_profit = get_nullable_float(row, "latest_loaded_month_profit")
    latest_closed_month_sales = get_nullable_float(row, "latest_closed_month_sales")
    latest_closed_month_profit = get_nullable_float(row, "latest_closed_month_profit")

    # 率
    rate_cm = safe_rate(s_cm, dp_cm)
    rate_py_cm = safe_rate(s_py_cm, dp_py_cm)
    rate_cur = safe_rate(s_cur, dp_cur)
    rate_py_ytd = safe_rate(s_py_ytd, dp_py_ytd)
    rate_py_total = safe_rate(s_py_total, dp_py_total)
    rate_fc = safe_rate(s_fc, dp_fc)

    current_month_label = "⭐ 当月実績"
    if refresh_status == "CURRENT_MONTH_MISSING":
        current_month_label = "⭐ 当月実績（未反映）"
    elif refresh_status == "NO_DATA":
        current_month_label = "⭐ 当月実績（データなし）"

    st.markdown("##### ■ データ反映状況")
    d1_, d2_, d3_, d4_ = st.columns(4)
    d1_.metric("最新反映日", fmt_date_or_dash(max_sales_date))
    d2_.metric("最新取込月", fmt_month_or_dash(latest_loaded_month))
    d3_.metric("最新確定月", fmt_month_or_dash(latest_closed_month))
    d4_.metric("反映遅延", f"{lag_days}日" if lag_days is not None else "—")

    if refresh_status == "CURRENT_MONTH_MISSING":
        st.warning("当月データは未反映です。当月実績は「—」表示とし、参考として最新取込月・最新確定月を併記しています。")
    elif refresh_status == "NO_DATA":
        st.warning("売上データが見つかりません。VIEW_UNIFIED の中身をご確認ください。")

    st.markdown("##### ■ 参考（月次実績）")
    r1_, r2_, r3_, r4_ = st.columns(4)
    r1_.metric(f"最新取込月売上（{fmt_month_or_dash(latest_loaded_month)}）", fmt_yen_or_dash(latest_loaded_month_sales))
    r2_.metric(f"最新取込月粗利（{fmt_month_or_dash(latest_loaded_month)}）", fmt_yen_or_dash(latest_loaded_month_profit))
    r3_.metric(f"最新確定月売上（{fmt_month_or_dash(latest_closed_month)}）", fmt_yen_or_dash(latest_closed_month_sales))
    r4_.metric(f"最新確定月粗利（{fmt_month_or_dash(latest_closed_month)}）", fmt_yen_or_dash(latest_closed_month_profit))

    st.caption(
        "※ 【今期予測】はAI予測ではなく、「今期実績 × (昨年度着地 ÷ 前年同期)」"
        "による季節変動を加味した推移ペース（着地見込）です。"
    )

    st.markdown("##### ■ 売上")
    c1_, c2_, c3_, c4_, c5_, c6_ = st.columns(6)
    c1_.metric(current_month_label, fmt_yen_or_dash(s_cm), delta=fmt_delta_yen(s_cm, s_py_cm))
    c2_.metric("① 今期累計", fmt_yen_or_dash(s_cur))
    c3_.metric("② 前年同期", fmt_yen_or_dash(s_py_ytd), delta=f"{int(s_cur - s_py_ytd):,}" if s_py_ytd > 0 else None)
    c4_.metric("③ 昨年度着地", fmt_yen_or_dash(s_py_total))
    c5_.metric("④ 今期予測", fmt_yen_or_dash(s_fc))
    c6_.metric("⑤ 着地GAP", fmt_yen_or_dash((s_fc - s_py_total) if s_fc is not None else None),
               delta=f"{int((s_fc - s_py_total)):,.0f}" if s_fc is not None else None)

    st.markdown("##### ■ 粗利")
    c1_, c2_, c3_, c4_, c5_, c6_ = st.columns(6)
    c1_.metric(current_month_label, fmt_yen_or_dash(gp_cm), delta=fmt_delta_yen(gp_cm, gp_py_cm))
    c2_.metric("① 今期累計", fmt_yen_or_dash(gp_cur))
    c3_.metric("② 前年同期", fmt_yen_or_dash(gp_py_ytd), delta=f"{int(gp_cur - gp_py_ytd):,}" if gp_py_ytd > 0 else None)
    c4_.metric("③ 昨年度着地", fmt_yen_or_dash(gp_py_total))
    c5_.metric("④ 今期予測", fmt_yen_or_dash(gp_fc))
    c6_.metric("⑤ 着地GAP", fmt_yen_or_dash((gp_fc - gp_py_total) if gp_fc is not None else None),
               delta=f"{int((gp_fc - gp_py_total)):,.0f}" if gp_fc is not None else None)

    st.markdown("##### ■ 総納入薬価")
    c1_, c2_, c3_, c4_, c5_, c6_ = st.columns(6)
    c1_.metric(current_month_label, fmt_yen_or_dash(dp_cm), delta=fmt_delta_yen(dp_cm, dp_py_cm))
    c2_.metric("① 今期累計", fmt_yen_or_dash(dp_cur))
    c3_.metric("② 前年同期", fmt_yen_or_dash(dp_py_ytd), delta=fmt_delta_yen(dp_cur, dp_py_ytd))
    c4_.metric("③ 昨年度着地", fmt_yen_or_dash(dp_py_total))
    c5_.metric("④ 今期予測", fmt_yen_or_dash(dp_fc))
    c6_.metric("⑤ 着地GAP", fmt_yen_or_dash((dp_fc - dp_py_total) if dp_fc is not None and dp_py_total is not None else None),
               delta=fmt_delta_yen(dp_fc, dp_py_total))

    st.markdown("##### ■ 加重平均 (納入価率)")
    c1_, c2_, c3_, c4_, c5_, c6_ = st.columns(6)
    c1_.metric(current_month_label, fmt_pct_or_dash(rate_cm), delta=fmt_delta_pct(rate_cm, rate_py_cm))
    c2_.metric("① 今期累計", fmt_pct_or_dash(rate_cur))
    c3_.metric("② 前年同期", fmt_pct_or_dash(rate_py_ytd), delta=fmt_delta_pct(rate_cur, rate_py_ytd))
    c4_.metric("③ 昨年度着地", fmt_pct_or_dash(rate_py_total))
    c5_.metric("④ 今期予測", fmt_pct_or_dash(rate_fc))
    c6_.metric("⑤ 着地GAP", fmt_pct_or_dash((rate_fc - rate_py_total) if rate_fc is not None and rate_py_total is not None else None),
               delta=fmt_delta_pct(rate_fc, rate_py_total))

    if dp_cur is None:
        st.caption("※ 総納入薬価列が VIEW_UNIFIED に存在しない、または数値化できないため、総納入薬価 / 納入価率は「—」表示です。")


def render_fytd_org_section(client: bigquery.Client, colmap: Dict[str, str]) -> None:
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")

    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    if st.button("全社データを読み込む", key="btn_org_load"):
        st.session_state.org_data_loaded = True

    if st.session_state.get("org_data_loaded"):
        sql = build_summary_sql(colmap, scoped_by_login=False)
        df_org = query_df_safe(client, sql, None, "Org Summary")
        if not df_org.empty:
            render_summary_metrics(df_org.iloc[0])
        else:
            st.info("全社サマリーのデータが取得できませんでした。")


def render_fytd_me_section(client: bigquery.Client, login_email: str, colmap: Dict[str, str]) -> None:
    st.subheader("👤 年度累計（FYTD）｜個人サマリー")
    if st.button("自分の成績を読み込む", key="btn_me_load"):
        sql = build_summary_sql(colmap, scoped_by_login=True)
        df_me = query_df_safe(client, sql, {"login_email": login_email}, "Me Summary")
        if not df_me.empty:
            render_summary_metrics(df_me.iloc[0])
        else:
            st.info("個人サマリーのデータが取得できませんでした。")


# -----------------------------
# ★追加：メーカー別パフォーマンス（前期/今期 売上・粗利）
# -----------------------------
def render_manufacturer_performance_section(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("🏭 メーカー別パフォーマンス（前期 / 今期：売上・粗利・加重平均）")

    manu_col = colmap.get("manufacturer")
    dp_col = colmap.get("total_drug_price")
    if not manu_col or not dp_col:
        st.info("VIEW_UNIFIED にメーカー列または総薬価列が見つからないため、このセクションは表示できません。")
        return

    role_filter = "" if role.role_admin_view else f"{c(colmap,'login_email')} = @login_email"
    scope_filter_clause = scope.where_clause()
    where_sql = _compose_where(role_filter, scope_filter_clause)

    params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        params["login_email"] = role.login_email

    sql = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
            - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy,
          DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today
      )
      SELECT
        COALESCE(NULLIF(TRIM(CAST({manu_col} AS STRING)), ''), '未設定') AS manufacturer,
        SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
        SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales,
        SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'gross_profit')} ELSE 0 END) AS ty_gp,
        SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 AND {c(colmap,'sales_date')} <= py_today THEN {c(colmap,'gross_profit')} ELSE 0 END) AS py_gp,
        SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {dp_col} ELSE 0 END) AS ty_dp
      FROM `{VIEW_UNIFIED}`
      CROSS JOIN fy
      {where_sql}
      GROUP BY manufacturer
      HAVING ty_sales != 0 OR py_sales != 0
      ORDER BY ty_sales DESC
      LIMIT 200
    """
    df = query_df_safe(client, sql, params, "Manufacturer Perf")

    if df.empty:
        st.info("該当データがありません。")
        return

    df = df.copy()
    df["売上差額"] = df["ty_sales"] - df["py_sales"]
    df["粗利差額"] = df["ty_gp"] - df["py_gp"]
    df["売上成長率"] = df.apply(lambda r: ((r["ty_sales"] / r["py_sales"] - 1) * 100) if r["py_sales"] else 0.0, axis=1)
    df["粗利成長率"] = df.apply(lambda r: ((r["ty_gp"] / r["py_gp"] - 1) * 100) if r["py_gp"] else 0.0, axis=1)
    df["納入価率(加重平均)"] = df.apply(lambda r: (r["ty_sales"] / r["ty_dp"] * 100) if r["ty_dp"] and r["ty_dp"] > 0 else 0.0, axis=1)

    c1_, c2_, c3_ = st.columns(3)
    sort_key = c1_.selectbox(
        "並び替え",
        ["今期売上（大→小）", "売上差額（小→大）", "売上差額（大→小）", "粗利差額（小→大）", "粗利差額（大→小）"],
        index=0,
    )
    topn = c2_.slider("表示件数", 20, 200, 80, 10)
    only_negative = c3_.checkbox("下落のみ（売上差額<0）", value=False)

    if only_negative:
        df = df[df["売上差額"] < 0]

    if sort_key == "売上差額（小→大）":
        df = df.sort_values("売上差額", ascending=True)
    elif sort_key == "売上差額（大→小）":
        df = df.sort_values("売上差額", ascending=False)
    elif sort_key == "粗利差額（小→大）":
        df = df.sort_values("粗利差額", ascending=True)
    elif sort_key == "粗利差額（大→小）":
        df = df.sort_values("粗利差額", ascending=False)
    else:
        df = df.sort_values("ty_sales", ascending=False)

    df = df.head(int(topn))

    df_disp = df.rename(
        columns={
            "manufacturer": "メーカー",
            "ty_sales": "今期売上",
            "py_sales": "前年同期売上",
            "ty_gp": "今期粗利",
            "py_gp": "前年同期粗利",
            "ty_dp": "今期総薬価",
        }
    )

    st.dataframe(
        df_disp[
            ["メーカー", "今期売上", "前年同期売上", "売上差額", "売上成長率", "今期粗利", "前年同期粗利", "粗利差額", "今期総薬価", "納入価率(加重平均)"]
        ].style.format(
            {
                "今期売上": "¥{:,.0f}",
                "前年同期売上": "¥{:,.0f}",
                "売上差額": "¥{:,.0f}",
                "売上成長率": "{:,.1f}%",
                "今期粗利": "¥{:,.0f}",
                "前年同期粗利": "¥{:,.0f}",
                "粗利差額": "¥{:,.0f}",
                "今期総薬価": "¥{:,.0f}",
                "納入価率(加重平均)": "{:,.1f}%",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------
# 得意先・グループ別パフォーマンス & 要因分析（管理者向け）
# -----------------------------
def render_group_underperformance_section(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("🏢 得意先・グループ別パフォーマンス ＆ 要因分析")

    if "perf_view" not in st.session_state:
        st.session_state.perf_view = "🏢 グループ別"
    if "perf_mode" not in st.session_state:
        st.session_state.perf_mode = "📉 下落幅ワースト"

    c1_, c2_ = st.columns(2)
    view_choice = c1_.radio("📊 分析の単位", ["🏢 グループ別", "🏥 得意先単体"], horizontal=True, key="perf_view")
    mode_choice = c2_.radio("🏆 ランキング基準", ["📉 下落幅ワースト", "📈 上昇幅ベスト"], horizontal=True, key="perf_mode")

    perf_view = "グループ別" if "グループ別" in view_choice else "得意先別"
    perf_mode = "ワースト" if "ワースト" in mode_choice else "ベスト"
    sort_order = "ASC" if perf_mode == "ワースト" else "DESC"

    group_expr, group_src = resolve_customer_group_sql_expr(client)
    if perf_view == "グループ別" and not group_expr:
        st.info("グループ分析に利用できる列が見つかりません（VIEW_UNIFIEDにグループ列がありません）。")
        return

    role_filter = "" if role.role_admin_view else f"{c(colmap,'login_email')} = @login_email"
    scope_filter_clause = scope.where_clause()
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
        parent_key_col = "名称"
    else:
        sql_parent = f"""
            WITH fy AS (
              SELECT
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                 - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today
            )
            SELECT
              CAST({c(colmap,'customer_code')} AS STRING) AS `コード`,
              ANY_VALUE(CAST({c(colmap,'customer_name')} AS STRING)) AS `名称`,
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
        parent_key_col = "コード"

    df_parent = query_df_safe(client, sql_parent, params, f"Parent Perf {perf_view}")
    if df_parent.empty:
        st.info("表示できるデータがありません。")
        return

    df_parent = df_parent.copy()
    df_parent["売上差額"] = df_parent["今期売上"] - df_parent["前年同期売上"]
    df_parent["売上成長率"] = df_parent.apply(
        lambda r: ((r["今期売上"] / r["前年同期売上"] - 1) * 100) if r["前年同期売上"] else 0,
        axis=1,
    )
    df_parent["粗利差額"] = df_parent["今期粗利"] - df_parent["前年同期粗利"]

    def rank_icon(rank: int, mode: str) -> str:
        if mode == "ベスト":
            return "🥇 1位" if rank == 1 else ("🥈 2位" if rank == 2 else ("🥉 3位" if rank == 3 else f"🌟 {rank}位"))
        return "🚨 1位" if rank == 1 else ("⚠️ 2位" if rank == 2 else ("⚡ 3位" if rank == 3 else f"📉 {rank}位"))

    df_parent.insert(0, "順位", [rank_icon(i + 1, perf_mode) for i in range(len(df_parent))])

    if perf_view == "グループ別" and group_src:
        st.caption(f"抽出元グループ列: `{group_src}`")

    show_cols = ["順位"]
    if perf_view != "グループ別":
        show_cols += ["コード"]
    show_cols += ["名称", "今期売上", "前年同期売上", "売上差額", "売上成長率", "今期粗利", "前年同期粗利", "粗利差額"]

    st.markdown("👇 **表の行をクリックすると、下の要因分析（商品ドリルダウン）が切り替わります**")
    event = st.dataframe(
        df_parent[show_cols].style.format(
            {
                "今期売上": "¥{:,.0f}",
                "前年同期売上": "¥{:,.0f}",
                "売上差額": "¥{:,.0f}",
                "売上成長率": "{:,.1f}%",
                "今期粗利": "¥{:,.0f}",
                "前年同期粗利": "¥{:,.0f}",
                "粗利差額": "¥{:,.0f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
        column_config=create_default_column_config(df_parent[show_cols]),
        selection_mode="single-row",
        on_select="rerun",
        key=f"grid_parent_{perf_view}_{perf_mode}",
    )

    try:
        sel_rows = event.selection.rows if hasattr(event, "selection") else []
        if sel_rows:
            idx = sel_rows[0]
            selected_parent_id = str(df_parent.iloc[idx][parent_key_col])
            selected_parent_name = str(df_parent.iloc[idx]["名称"])
        else:
            selected_parent_id = str(df_parent.iloc[0][parent_key_col])
            selected_parent_name = str(df_parent.iloc[0]["名称"])
    except Exception:
        selected_parent_id = str(df_parent.iloc[0][parent_key_col])
        selected_parent_name = str(df_parent.iloc[0]["名称"])

    st.session_state.selected_parent_id = selected_parent_id

    st.divider()
    st.markdown(f"#### 🔎 【{selected_parent_name}】要因（商品）ドリルダウン（全件表示）")

    drill_role_filter = "" if role.role_admin_view else f"{c(colmap,'login_email')} = @login_email"
    drill_scope_clause = scope.where_clause()
    drill_params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        drill_params["login_email"] = role.login_email

    if perf_view == "グループ別":
        if not group_expr:
            st.info("グループ列が無いため要因分析できません。")
            return
        drill_filter_sql = _compose_where(drill_role_filter, drill_scope_clause, f"{group_expr} = @parent_id")
        drill_params["parent_id"] = selected_parent_id
    else:
        drill_filter_sql = _compose_where(
            drill_role_filter, drill_scope_clause, f"CAST({c(colmap,'customer_code')} AS STRING) = @parent_id"
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
              TRIM(CAST({c(colmap,'product_name')} AS STRING))
            ) AS yj_key,
            CAST({c(colmap,'product_name')} AS STRING) AS product_base,
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
    if df_drill.empty:
        st.info("要因データが見つかりません。")
        return

    df_drill = df_drill.copy()
    df_drill["product_name"] = df_drill["product_name"].apply(normalize_product_display_name)
    df_drill.insert(0, "要因順位", [rank_icon(i + 1, perf_mode) for i in range(len(df_drill))])

    st.dataframe(
        df_drill[["要因順位", "product_name", "sales_amount", "py_sales_amount", "sales_diff_yoy"]]
        .rename(
            columns={
                "product_name": "代表商品名(成分)",
                "sales_amount": "今期売上",
                "py_sales_amount": "前年同期売上",
                "sales_diff_yoy": "前年比差額",
            }
        )
        .style.format({"今期売上": "¥{:,.0f}", "前年同期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------
# YoY
# -----------------------------
def render_yoy_section(
    client: bigquery.Client,
    login_email: str,
    is_admin: bool,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJベース）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = "ワースト"
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()

    c1_, c2_, c3_ = st.columns(3)

    def load_yj_data(mode_name: str) -> None:
        st.session_state.yoy_mode = mode_name
        role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
        scope_where = scope.where_clause()
        combined_where = _compose_where(role_filter, scope_where)

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
              SELECT (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            ),
            base_raw AS (
              SELECT
                COALESCE(
                  NULLIF(NULLIF(TRIM(CAST({c(colmap,'yj_code')} AS STRING)), ''), '0'),
                  TRIM(CAST({c(colmap,'product_name')} AS STRING))
                ) AS yj_key,
                CAST({c(colmap,'product_name')} AS STRING) AS original_name,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {combined_where}
              GROUP BY yj_key, original_name
            ),
            base AS (
              SELECT
                yj_key AS yj_code,
                ARRAY_AGG(original_name ORDER BY ty_sales DESC LIMIT 1)[OFFSET(0)] AS product_name,
                SUM(ty_sales) AS ty_sales,
                SUM(py_sales) AS py_sales
              FROM base_raw
              GROUP BY yj_code
            )
            SELECT *, (ty_sales - py_sales) AS sales_diff_yoy
            FROM base
            WHERE {diff_filter}
            ORDER BY {order_by}
            LIMIT 100
        """
        st.session_state.yoy_df = query_df_safe(client, sql, params, mode_name)

    with c1_:
        if st.button("📉 下落幅ワースト", use_container_width=True):
            load_yj_data("ワースト")
    with c2_:
        if st.button("📈 上昇幅ベスト", use_container_width=True):
            load_yj_data("ベスト")
    with c3_:
        if st.button("🆕 新規/比較不能", use_container_width=True):
            load_yj_data("新規")

    if st.session_state.yoy_df.empty:
        st.info("ランキングを読み込むにはボタンを押してください。")
        return

    df_disp = st.session_state.yoy_df.copy()
    df_disp["product_name"] = df_disp["product_name"].apply(normalize_product_display_name)

    st.markdown(f"#### 🏆 第一階層：成分（YJ）ベース {st.session_state.yoy_mode} ランキング")
    event = st.dataframe(
        df_disp[["product_name", "ty_sales", "py_sales", "sales_diff_yoy"]].rename(
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

    selected_yj_default = "全成分を表示"
    try:
        sel_rows = event.selection.rows if hasattr(event, "selection") else []
        if sel_rows:
            selected_yj_default = str(df_disp.iloc[sel_rows[0]]["yj_code"])
    except Exception:
        pass

    st.divider()
    st.header("🔍 第二階層：詳細分析（スコープ内全量）")

    yj_opts = ["全成分を表示"] + list(df_disp["yj_code"].astype(str).unique())
    yj_display_map = {"全成分を表示": "🚩 スコープ内の全成分を合計して表示"}
    for _, r in df_disp.iterrows():
        yj_display_map[str(r["yj_code"])] = f"{normalize_product_display_name(r['product_name'])} (差額: ¥{r['sales_diff_yoy']:,.0f})"

    current_index = yj_opts.index(selected_yj_default) if selected_yj_default in yj_opts else 0

    selected_yj = st.selectbox(
        "詳細を見たい成分を選択してください（[全成分を表示] ですべて表示）",
        options=yj_opts,
        index=current_index,
        format_func=lambda x: yj_display_map.get(x, x),
    )

    role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
    scope_where = scope.where_clause()

    drill_params = dict(scope.params or {})
    if not is_admin:
        drill_params["login_email"] = login_email

    yj_filter = ""
    if selected_yj != "全成分を表示":
        yj_filter = f"""
            COALESCE(
              NULLIF(NULLIF(TRIM(CAST({c(colmap,'yj_code')} AS STRING)), ''), '0'),
              TRIM(CAST({c(colmap,'product_name')} AS STRING))
            ) = @target_yj
        """
        drill_params["target_yj"] = selected_yj

    final_where = _compose_where(role_filter, scope_where, yj_filter)
    sort_order = "ASC" if st.session_state.yoy_mode == "ワースト" else "DESC"

    fy_cte = f"""
        WITH fy AS (
          SELECT (
            EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
            - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
          ) AS current_fy
        )
    """

    st.markdown("#### 🧾 得意先別内訳（前年差額）")
    sql_cust = f"""
        {fy_cte}
        SELECT
          {c(colmap,'customer_name')} AS `得意先名`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前期売上`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {final_where}
        GROUP BY 1
        HAVING `今期売上`!=0 OR `前期売上`!=0
        ORDER BY (`今期売上`-`前期売上`) {sort_order}
        LIMIT 50
    """
    df_cust = query_df_safe(client, sql_cust, drill_params, "YoY Cust Detail")
    if not df_cust.empty:
        df_cust["前年差額"] = df_cust["今期売上"] - df_cust["前期売上"]
        st.dataframe(
            df_cust.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### 🧪 原因追及：JAN・商品別（前年差額寄与）")
    sql_jan = f"""
        {fy_cte}
        SELECT
          CAST({c(colmap,'jan_code')} AS STRING) AS `JAN`,
          CAST({c(colmap,'product_name')} AS STRING) AS `商品名`,
          CAST({c(colmap,'package_unit')} AS STRING) AS `包装`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前期売上`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {final_where}
        GROUP BY 1,2,3
        ORDER BY (`今期売上`-`前期売上`) {sort_order}
    """
    df_jan = query_df_safe(client, sql_jan, drill_params, "YoY JAN Detail")
    if not df_jan.empty:
        df_jan["前年差額"] = df_jan["今期売上"] - df_jan["前期売上"]
        st.dataframe(
            df_jan.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### 📅 原因追及：月次推移（前年差額）")
    sql_month = f"""
        {fy_cte}
        SELECT
          FORMAT_DATE('%Y-%m', {c(colmap,'sales_date')}) AS `年月`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前期売上`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {final_where}
        GROUP BY 1
        ORDER BY 1
    """
    df_month = query_df_safe(client, sql_month, drill_params, "YoY Month Trend")
    if not df_month.empty:
        df_month["前年差額"] = df_month["今期売上"] - df_month["前期売上"]
        st.dataframe(
            df_month.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )


# -----------------------------
# 新規納品（Realized）
# -----------------------------
def render_new_delivery_trends(
    client: bigquery.Client,
    login_email: str,
    is_admin: bool,
    nd_colmap: Dict[str, str],
    unified_colmap: Dict[str, str],
) -> None:
    st.markdown("##### 📈 新規納品トレンド（グループ / 得意先 / 商品）")

    missing_required = nd_colmap.get("_missing_required")
    if missing_required:
        st.error("VIEW_NEW_DELIVERY の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing_required}")
        st.stop()

    if (not is_admin) and (c(nd_colmap, "login_email") == "login_email"):
        st.error("VIEW_NEW_DELIVERY に login_email 列が無いため、担当者スコープ絞り込みができません。")
        st.stop()

    if "nd_trend_days" not in st.session_state:
        st.session_state.nd_trend_days = 60
    if "nd_trend_mode" not in st.session_state:
        st.session_state.nd_trend_mode = "🏢 グループ"

    days = st.slider("対象期間（日）", 7, 180, st.session_state.nd_trend_days, 1, key="nd_trend_days")
    mode = st.radio("表示単位", ["🏢 グループ", "🏥 得意先", "💊 商品"], horizontal=True, key="nd_trend_mode")

    where_staff = "" if is_admin else f"AND nd.{c(nd_colmap,'login_email')} = @login_email"
    base_params = {} if is_admin else {"login_email": login_email}

    group_expr, _ = resolve_customer_group_sql_expr(client)
    if group_expr:
        cust_dim_sql = f"""
          SELECT
            CAST({c(unified_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(CAST({c(unified_colmap,'customer_name')} AS STRING)) AS customer_name,
            ANY_VALUE({group_expr}) AS group_name
          FROM `{VIEW_UNIFIED}`
          GROUP BY customer_code
        """
    else:
        cust_dim_sql = f"""
          SELECT
            CAST({c(unified_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(CAST({c(unified_colmap,'customer_name')} AS STRING)) AS customer_name,
            '未設定' AS group_name
          FROM `{VIEW_UNIFIED}`
          GROUP BY customer_code
        """

    nd_prod_col = nd_colmap.get("product_name")
    if nd_prod_col:
        prod_expr = f"CAST(nd.{nd_prod_col} AS STRING)"
    else:
        prod_expr = f"CONCAT('商品名不明（JAN）:', CAST(nd.{c(nd_colmap,'jan_code')} AS STRING))"

    if mode.startswith("🏢"):
        sql_parent = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql})
          SELECT
            COALESCE(cd.group_name, '未設定') AS group_name,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'customer_code')} AS STRING)) AS customer_cnt,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS item_cnt,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
          GROUP BY group_name
          ORDER BY sales_amount DESC
          LIMIT 300
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Groups")
        key_col = "group_name"
        title = "🏢 グループトレンド（新規納品）"

    elif mode.startswith("🏥"):
        sql_parent = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            ANY_VALUE(COALESCE(cd.group_name, '未設定')) AS group_name,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS item_cnt,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
          GROUP BY customer_code
          ORDER BY sales_amount DESC
          LIMIT 300
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Customers")
        key_col = "customer_code"
        title = "🏥 得意先トレンド（新規納品）"

    else:
        sql_parent = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
          SELECT
            {prod_expr} AS prod_key,
            ANY_VALUE({prod_expr}) AS product_name,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'customer_code')} AS STRING)) AS customer_cnt,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS jan_cnt,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
          GROUP BY prod_key
          ORDER BY sales_amount DESC
          LIMIT 500
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Items")
        key_col = "prod_key"
        title = "💊 商品トレンド（新規納品）"

    st.markdown(f"**{title}**")
    if df_parent.empty:
        st.info("該当期間のトレンドがありません。")
        return

    df_parent = df_parent.copy()
    df_parent.insert(0, "☑", False)

    if key_col == "group_name":
        df_parent = df_parent.rename(
            columns={
                "group_name": "グループ",
                "customer_cnt": "得意先数",
                "item_cnt": "品目数",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )
        display_cols = ["☑", "グループ", "得意先数", "品目数", "売上", "粗利"]
        pick_col = "グループ"

    elif key_col == "customer_code":
        df_parent = df_parent.rename(
            columns={
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "group_name": "グループ",
                "item_cnt": "品目数",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )
        display_cols = ["☑", "得意先コード", "得意先名", "グループ", "品目数", "売上", "粗利"]
        pick_col = "得意先コード"

    else:
        df_parent = df_parent.rename(
            columns={
                "prod_key": "商品キー",
                "product_name": "商品名",
                "customer_cnt": "得意先数",
                "jan_cnt": "JAN数",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )
        display_cols = ["☑", "商品キー", "商品名", "得意先数", "JAN数", "売上", "粗利"]
        pick_col = "商品キー"

    df_view = df_parent[display_cols].copy()
    for colx in df_view.columns:
        if colx != "☑":
            df_view[colx] = df_view[colx].fillna("")

    column_config = {
        "☑": st.column_config.CheckboxColumn("選択", help="明細を表示したい行にチェック（複数可）"),
    }
    if "商品キー" in df_view.columns:
        column_config["商品キー"] = st.column_config.TextColumn("商品キー", width="small", help="内部キー（選択連動用）")

    edited = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        disabled=[c_ for c_ in display_cols if c_ != "☑"],
        column_config=column_config,
        key=f"nd_trend_editor_{key_col}_v2",
    )

    sel_df = edited[edited["☑"].astype(bool) == True]  # noqa: E712
    if sel_df.empty:
        st.caption("☑にチェックすると下に明細が出ます（複数選択可）。")
        return

    selected_keys = sel_df[pick_col].astype(str).tolist()

    st.divider()
    st.markdown("#### 🧾 明細（ドリルダウン）")

    if key_col == "group_name":
        params2 = dict(base_params)
        params2["group_keys"] = selected_keys

        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE) AS first_sales_date,
            COALESCE(cd.group_name, '未設定') AS group_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            {prod_expr} AS product_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND COALESCE(cd.group_name, '未設定') IN UNNEST(@group_keys)
          GROUP BY first_sales_date, group_name, customer_code, product_name
          ORDER BY first_sales_date DESC, sales_amount DESC
          LIMIT 5000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Group Details")
        df_detail = df_detail.rename(
            columns={
                "first_sales_date": "初回納品日",
                "group_name": "グループ",
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "product_name": "商品名",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )

    elif key_col == "customer_code":
        params2 = dict(base_params)
        params2["customer_keys"] = selected_keys

        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE) AS first_sales_date,
            COALESCE(cd.group_name, '未設定') AS group_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            {prod_expr} AS product_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) IN UNNEST(@customer_keys)
          GROUP BY first_sales_date, group_name, customer_code, product_name
          ORDER BY first_sales_date DESC, sales_amount DESC
          LIMIT 5000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Customer Details")
        df_detail = df_detail.rename(
            columns={
                "first_sales_date": "初回納品日",
                "group_name": "グループ",
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "product_name": "商品名",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )

    else:
        params2 = dict(base_params)
        params2["prod_keys"] = selected_keys

        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql})
          SELECT
            {prod_expr} AS product_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            ANY_VALUE(COALESCE(cd.group_name, '未設定')) AS group_name,
            MIN(CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE)) AS first_sales_date_min,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND {prod_expr} IN UNNEST(@prod_keys)
          GROUP BY product_name, customer_code
          ORDER BY sales_amount DESC
          LIMIT 5000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Item -> Customers")
        df_detail = df_detail.rename(
            columns={
                "product_name": "商品名",
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "group_name": "グループ",
                "first_sales_date_min": "初回納品日（最小）",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )

    if df_detail.empty:
        st.info("明細がありません。")
        return

    st.dataframe(
        df_detail.fillna("").style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
    )


def render_new_deliveries_section(
    client: bigquery.Client,
    login_email: str,
    is_admin: bool,
    nd_colmap: Dict[str, str],
    unified_colmap: Dict[str, str],
) -> None:
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")

    missing = nd_colmap.get("_missing_required")
    if missing:
        st.error("VIEW_NEW_DELIVERY の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing}")
        st.stop()

    if (not is_admin) and (c(nd_colmap, "login_email") == "login_email"):
        st.error("VIEW_NEW_DELIVERY に login_email 列が無いため、担当者スコープ絞り込みができません。")
        st.code("対処: VIEW_NEW_DELIVERY に login_email を追加するか、nd_colmap mapping に実列名を追加してください。")
        st.stop()

    if "nd_summary_loaded" not in st.session_state:
        st.session_state.nd_summary_loaded = False
    if "nd_summary_df" not in st.session_state:
        st.session_state.nd_summary_df = pd.DataFrame()

    if st.button("新規納品実績を読み込む", key="btn_new_deliv"):
        where_ext = "" if is_admin else f"AND {c(nd_colmap,'login_email')} = @login_email"
        params = None if is_admin else {"login_email": login_email}

        sql = f"""
        WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
        SELECT
          '① 昨日' AS `期間`,
          COUNT(DISTINCT CAST({c(nd_colmap,'customer_code')} AS STRING)) AS `得意先数`,
          COUNT(DISTINCT CAST({c(nd_colmap,'jan_code')} AS STRING)) AS `品目数`,
          SUM({c(nd_colmap,'sales_amount')}) AS `売上`,
          SUM({c(nd_colmap,'gross_profit')}) AS `粗利`
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE {c(nd_colmap,'first_sales_date')} = DATE_SUB(today, INTERVAL 1 DAY) {where_ext}
        UNION ALL
        SELECT
          '② 直近7日',
          COUNT(DISTINCT CAST({c(nd_colmap,'customer_code')} AS STRING)),
          COUNT(DISTINCT CAST({c(nd_colmap,'jan_code')} AS STRING)),
          SUM({c(nd_colmap,'sales_amount')}),
          SUM({c(nd_colmap,'gross_profit')})
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE {c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL 7 DAY) {where_ext}
        UNION ALL
        SELECT
          '③ 当月',
          COUNT(DISTINCT CAST({c(nd_colmap,'customer_code')} AS STRING)),
          COUNT(DISTINCT CAST({c(nd_colmap,'jan_code')} AS STRING)),
          SUM({c(nd_colmap,'sales_amount')}),
          SUM({c(nd_colmap,'gross_profit')})
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE DATE_TRUNC({c(nd_colmap,'first_sales_date')}, MONTH) = DATE_TRUNC(today, MONTH) {where_ext}
        ORDER BY `期間`
        """

        df_new = query_df_safe(client, sql, params, label="New Deliveries")
        st.session_state.nd_summary_df = df_new.copy()
        st.session_state.nd_summary_loaded = True

    if not st.session_state.nd_summary_loaded:
        st.info("上のボタンで新規納品実績を読み込みます。")
        return

    df_new = st.session_state.nd_summary_df
    if df_new is None or df_new.empty:
        st.info("新規納品データがありません。")
    else:
        df_new = df_new.copy()
        for coln in ["売上", "粗利"]:
            if coln in df_new.columns:
                df_new[coln] = pd.to_numeric(df_new[coln], errors="coerce").fillna(0)

        st.dataframe(
            df_new.style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    render_new_delivery_trends(client, login_email, is_admin, nd_colmap, unified_colmap)


# -----------------------------
# 採用・失注アラート
# -----------------------------
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


# -----------------------------
# 得意先ドリルダウン & Reco
# -----------------------------
def render_customer_drilldown(
    client: bigquery.Client,
    login_email: str,
    is_admin: bool,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")

    role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
    scope_filter = scope.where_clause()
    customer_where = _compose_where(role_filter, scope_filter, f"{c(colmap,'customer_name')} IS NOT NULL")

    customer_params: Dict[str, Any] = dict(scope.params or {})
    if not is_admin:
        customer_params["login_email"] = login_email

    sql_cust = f"""
        SELECT DISTINCT
          CAST({c(colmap,'customer_code')} AS STRING) AS customer_code,
          {c(colmap,'customer_name')} AS customer_name
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

    opts = {str(row["customer_code"]): f"{row['customer_code']} : {row['customer_name']}" for _, row in filtered_df.iterrows()}
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
        WHERE CAST(customer_code AS STRING) = @c
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
        SELECT *
        FROM `{VIEW_RECOMMEND}`
        WHERE CAST(customer_code AS STRING) = @c
        ORDER BY priority_rank ASC
        LIMIT 10
    """
    df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
    if not df_rec.empty:
        df_disp = df_rec[["priority_rank", "recommend_product", "manufacturer"]].rename(
            columns={"priority_rank": "順位", "recommend_product": "推奨商品", "manufacturer": "メーカー"}
        )
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
    else:
        st.info("現在、この得意先への推奨商品はありません。")


# -----------------------------
# 6. Main Loop
# -----------------------------
def main() -> None:
    set_page()

    if "use_bqstorage" not in st.session_state:
        st.session_state["use_bqstorage"] = True

    client = setup_bigquery_client()

    unified_colmap = resolve_unified_colmap(client)
    missing = unified_colmap.get("_missing_required")
    if missing:
        st.error(f"VIEW_UNIFIED の必須列が見つかりません。不足キー: {missing}")
        st.stop()

    nd_colmap = resolve_new_delivery_colmap(client)

    with st.sidebar:
        st.header("🔑 ログイン")
        login_id = st.text_input("ログインID (メールアドレス)")
        login_pw = st.text_input("パスコード (携帯下4桁)", type="password")

        st.divider()
        st.checkbox("高速読込 (Storage API)", key="use_bqstorage")

        if st.button("📡 通信ヘルスチェック"):
            try:
                client.query("SELECT 1").result(timeout=10)
                st.success("BigQuery 接続正常")
            except Exception as e:
                st.error(f"接続エラー: {e}")

        if st.button("🧹 キャッシュクリア"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("キャッシュをクリアしました（再読み込みしてください）")

    if not login_id or not login_pw:
        st.info("👈 サイドバーからログインしてください。")
        return

    role = resolve_role(client, login_id.strip(), login_pw.strip())
    if not role.is_authenticated:
        st.error("❌ ログイン情報が正しくありません。")
        return

    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    c1_, c2_, c3_ = st.columns(3)
    c1_.metric("👤 担当", role.staff_name)
    c2_.metric("🛡️ 権限", role.role_key)
    c3_.metric("📞 電話", role.phone)
    st.divider()

    if role.role_admin_view:
        render_fytd_org_section(client, unified_colmap)
    else:
        render_fytd_me_section(client, role.login_email, unified_colmap)

    st.divider()

    scope = render_scope_filters(client, role, unified_colmap)
    st.divider()

    if role.role_admin_view:
        render_group_underperformance_section(client, role, scope, unified_colmap)
        st.divider()

    render_manufacturer_performance_section(client, role, scope, unified_colmap)
    st.divider()

    is_admin = role.role_admin_view
    render_yoy_section(client, role.login_email, is_admin, scope, unified_colmap)
    st.divider()

    render_new_deliveries_section(client, role.login_email, is_admin, nd_colmap, unified_colmap)
    st.divider()

    render_adoption_alerts_section(client, role.login_email, is_admin)
    st.divider()

    render_customer_drilldown(client, role.login_email, is_admin, scope, unified_colmap)


if __name__ == "__main__":
    main()
