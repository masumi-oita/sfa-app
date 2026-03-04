# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.9 (Final Complete Edition)

【v1.4.8 踏襲機能】
- YoY：VIEW_UNIFIED から動的集計に統一（YJ同一で商品名が2行問題を抑止）
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定
- Group Display: official先頭 + raw併記
- 新機能：得意先グループ / 得意先単体の切替 ＆ 商品要因ドリルダウン（全件表示）
- 新機能：順位アイコンの追加と、不要なYJコード列の非表示
- 修正：WHERE二重エラー解消 ＆ 選択状態の消失バグ解消 ＆ 表示順序の最適化
- 修正：Reco（VIEW_RECOMMEND）の customer_code が INT64 のため CAST 対応

【v1.4.9 強化機能（事故の根治）】
- ColMap（列名吸収）導入：jan/jan_code、pack_unit/package_unit 等の差異を自動解決
- 全SQLで colmap を貫通：列名揺れ起因の "Unrecognized name" を根絶
- 必須列が見つからない場合は、起動直後に「不足列一覧」を明示して停止
- VIEW_NEW_DELIVERY に customer_name/product_name が無くても VIEW_UNIFIED から補完
- YoY等での年度判定を MAX(fiscal_year) ではなく CURRENT_DATE ベースに完全統一（未来データ混入対策）
- YoY第二階層を「全成分を表示」デフォルト対応（ドリルダウンしやすく修正）

【★最新追加機能】
- サマリー（最上部）に「当月実績」および「前年同月差額」の表示を追加。
- 「得意先・グループ別パフォーマンス」の冗長なラジオボタンを撤廃し、データフレーム（表）のクリック連動にUIを洗練化。
"""

from __future__ import annotations

from dataclasses import dataclass
import re
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
    st.caption("OS v1.4.9｜Final Complete Edition (UI洗練 + ColMap + 当月実績)")


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


def _build_query_parameter(key: str, value: Any) -> bigquery.QueryParameter:
    if isinstance(value, tuple) and len(value) == 2:
        p_type, p_value = value
        p_type = str(p_type).upper()
        if p_type.startswith("ARRAY<") and isinstance(p_value, (list, tuple)):
            return bigquery.ArrayQueryParameter(key, "STRING", list(p_value))
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
    for c in cands:
        if c and c.lower() in cols:
            return c.lower()
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
    """SQL内で使う列名解決。"""
    return colmap.get(key, key)


# -----------------------------
# VIEW_UNIFIED系（互換のため関数名は維持）
# -----------------------------
@st.cache_data(ttl=3600)
def get_unified_columns(_client: bigquery.Client) -> set[str]:
    return get_view_columns(_client, VIEW_UNIFIED)


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
        "product_name": ("product_name", "商品名", "商品名称", "item_name", "商品名称"),
        "yj_code": ("yj_code", "yjcode", "yj", "YJCode"),
        "jan_code": ("jan_code", "jan", "JAN"),
        "package_unit": ("package_unit", "pack_unit", "包装単位", "包装"),
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
        c1, c2 = st.columns(2)

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
# 4. UI Sections
# -----------------------------
def render_summary_metrics(row: pd.Series) -> None:
    # 累計データ
    s_cur = get_safe_float(row, "sales_amount_fytd")
    s_py_ytd = get_safe_float(row, "sales_amount_py_ytd")
    s_py_total = get_safe_float(row, "sales_amount_py_total")
    s_fc = s_cur * (s_py_total / s_py_ytd) if s_py_ytd > 0 else s_cur

    gp_cur = get_safe_float(row, "gross_profit_fytd")
    gp_py_ytd = get_safe_float(row, "gross_profit_py_ytd")
    gp_py_total = get_safe_float(row, "gross_profit_py_total")
    gp_fc = gp_cur * (gp_py_total / gp_py_ytd) if gp_py_ytd > 0 else gp_cur

    # 当月データ
    s_cm = get_safe_float(row, "sales_amount_cm")
    s_py_cm = get_safe_float(row, "sales_amount_py_cm")
    gp_cm = get_safe_float(row, "gross_profit_cm")
    gp_py_cm = get_safe_float(row, "gross_profit_py_cm")

    st.caption(
        "※ 【今期予測】はAI予測ではなく、「今期実績 × (昨年度着地 ÷ 前年同期)」"
        "による季節変動を加味した推移ペース（着地見込）です。"
    )

    st.markdown("##### ■ 売上")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("⭐ 当月実績", f"¥{s_cm:,.0f}", delta=f"{int(s_cm - s_py_cm):,}" if s_py_cm > 0 else None)
    c2.metric("① 今期累計", f"¥{s_cur:,.0f}")
    c3.metric("② 前年同期", f"¥{s_py_ytd:,.0f}", delta=f"{int(s_cur - s_py_ytd):,}" if s_py_ytd > 0 else None)
    c4.metric("③ 昨年度着地", f"¥{s_py_total:,.0f}")
    c5.metric("④ 今期予測", f"¥{s_fc:,.0f}")
    c6.metric("⑤ 着地GAP", f"¥{s_fc - s_py_total:,.0f}", delta=f"{int(s_fc - s_py_total):,}")

    st.markdown("##### ■ 粗利")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("⭐ 当月実績", f"¥{gp_cm:,.0f}", delta=f"{int(gp_cm - gp_py_cm):,}" if gp_py_cm > 0 else None)
    c2.metric("① 今期累計", f"¥{gp_cur:,.0f}")
    c3.metric("② 前年同期", f"¥{gp_py_ytd:,.0f}", delta=f"{int(gp_cur - gp_py_ytd):,}" if gp_py_ytd > 0 else None)
    c4.metric("③ 昨年度着地", f"¥{gp_py_total:,.0f}")
    c5.metric("④ 今期予測", f"¥{gp_fc:,.0f}")
    c6.metric("⑤ 着地GAP", f"¥{gp_fc - gp_py_total:,.0f}", delta=f"{int(gp_fc - gp_py_total):,}")


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
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(today, MONTH) THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(today, MONTH) THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(py_today, MONTH) THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(py_today, MONTH) THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_cm,
              
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
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(today, MONTH) THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(today, MONTH) THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(py_today, MONTH) THEN {c(colmap,'sales_amount')} ELSE 0 END) AS sales_amount_py_cm,
              SUM(CASE WHEN DATE_TRUNC({c(colmap,'sales_date')}, MONTH) = DATE_TRUNC(py_today, MONTH) THEN {c(colmap,'gross_profit')} ELSE 0 END) AS gross_profit_py_cm,
              
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

    if "perf_view" not in st.session_state:
        st.session_state.perf_view = "🏢 グループ別"
    if "perf_mode" not in st.session_state:
        st.session_state.perf_mode = "📉 下落幅ワースト"
    if "selected_parent_id" not in st.session_state:
        st.session_state.selected_parent_id = ""

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

    # ★ 冗長なラジオボタンを撤廃し、データフレームのクリックと連動させる
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
        key=f"grid_parent_{perf_view}_{perf_mode}"
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
    except:
        selected_parent_id = str(df_parent.iloc[0][parent_key_col])
        selected_parent_name = str(df_parent.iloc[0]["名称"])

    st.session_state.selected_parent_id = selected_parent_id

    st.divider()
    st.markdown(f"#### 🔎 【{selected_parent_name}】要因（商品）ドリルダウン（全件表示）")

    drill_role_filter = "" if role.role_admin_view else f"{c(colmap,'login_email')} = @login_email"
    drill_scope_clause = scope.where_clause()
    drill_filter_sql = _compose_where(drill_role_filter, drill_scope_clause)

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
        drill_filter_sql = _compose_where(drill_role_filter, drill_scope_clause, f"CAST({c(colmap,'customer_code')} AS STRING) = @parent_id")
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
    if df_drill.empty:
        st.info("要因データが見つかりません。")
        return

    df_drill = df_drill.copy()
    df_drill["product_name"] = df_drill["product_name"].apply(normalize_product_display_name)
    df_drill = df_drill.fillna(0)
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


def render_yoy_section(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter, colmap: Dict[str, str]) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJベース）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = "ワースト"
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()
    if "selected_yj" not in st.session_state:
        st.session_state.selected_yj = "全成分を表示"

    c1, c2, c3 = st.columns(3)

    def load_yj_data(mode_name: str) -> None:
        st.session_state.yoy_mode = mode_name
        role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
        scope_where = scope.where_clause()
        combined_where = _compose_where(role_filter, scope_where)
        params: Dict[str, Any] = dict(scope.params or {})
        if not is_admin: params["login_email"] = login_email

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
            base AS (
              SELECT
                {c(colmap,'yj_code')} AS yj_code,
                ANY_VALUE({c(colmap,'product_name')}) AS product_name,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS py_sales
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {combined_where}
              GROUP BY yj_code
            )
            SELECT *, (ty_sales - py_sales) AS sales_diff_yoy FROM base
            WHERE {diff_filter}
            ORDER BY {order_by} LIMIT 100
        """
        st.session_state.yoy_df = query_df_safe(client, sql, params, mode_name)

    with c1:
        if st.button("📉 下落幅ワースト", use_container_width=True): load_yj_data("ワースト")
    with c2:
        if st.button("📈 上昇幅ベスト", use_container_width=True): load_yj_data("ベスト")
    with c3:
        if st.button("🆕 新規/比較不能", use_container_width=True): load_yj_data("新規")

    if st.session_state.yoy_df.empty:
        st.info("ランキングを読み込むにはボタンを押してください。")
        return

    df_disp = st.session_state.yoy_df.copy()
    df_disp["product_name"] = df_disp["product_name"].apply(normalize_product_display_name)
    
    st.markdown(f"#### 🏆 第一階層：成分（YJ）ベース {st.session_state.yoy_mode} ランキング")
    event = st.dataframe(
        df_disp[["product_name", "ty_sales", "py_sales", "sales_diff_yoy"]].rename(
            columns={"product_name": "代表商品名(成分)", "ty_sales": "今期売上", "py_sales": "前期売上", "sales_diff_yoy": "前年比差額"}
        ).style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}),
        use_container_width=True, hide_index=True, selection_mode="single-row", on_select="rerun", key=f"grid_yoy_{st.session_state.yoy_mode}"
    )

    try:
        sel_rows = event.selection.rows if hasattr(event, "selection") else []
        if sel_rows:
            st.session_state.selected_yj = str(df_disp.iloc[sel_rows[0]]["yj_code"])
        else:
            st.session_state.selected_yj = "全成分を表示"
    except: pass

    st.divider()
    
    st.header("🔍 第二階層：詳細分析（スコープ内全量）")

    yj_opts = ["全成分を表示"] + list(df_disp["yj_code"].unique())
    yj_display_map = {"全成分を表示": "🚩 スコープ内の全成分を合計して表示"}
    for _, r in df_disp.iterrows():
        yj_display_map[str(r["yj_code"])] = f"{normalize_product_display_name(r['product_name'])} (差額: ¥{r['sales_diff_yoy']:,.0f})"

    current_index = 0
    if st.session_state.selected_yj in yj_opts:
        current_index = yj_opts.index(st.session_state.selected_yj)

    selected_yj = st.selectbox(
        "詳細を見たい成分を選択してください（[全成分を表示] ですべて表示）", 
        options=yj_opts, 
        index=current_index,
        format_func=lambda x: yj_display_map.get(x, x)
    )

    role_filter = "" if is_admin else f"{c(colmap,'login_email')} = @login_email"
    scope_where = scope.where_clause()
    yj_filter = ""
    drill_params = dict(scope.params or {})
    if not is_admin: drill_params["login_email"] = login_email

    if selected_yj != "全成分を表示":
        yj_filter = f"{c(colmap,'yj_code')} = @target_yj"
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
        ORDER BY (`今期売上`-`前期売上`) {sort_order} LIMIT 50
    """
    df_cust = query_df_safe(client, sql_cust, drill_params)
    if not df_cust.empty:
        df_cust["前年差額"] = df_cust["今期売上"] - df_cust["前期売上"]
        st.dataframe(df_cust.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

    st.markdown("#### 🧪 原因追及：JAN・商品別（前年差額寄与）")
    sql_jan = f"""
        {fy_cte}
        SELECT 
          CAST({c(colmap,'jan_code')} AS STRING) AS `JAN`, 
          CAST({c(colmap,'product_name')} AS STRING) AS `代表商品名`, 
          CAST({c(colmap,'package_unit')} AS STRING) AS `包装`, 
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `今期売上`, 
          SUM(CASE WHEN {c(colmap,'fiscal_year')} = current_fy - 1 THEN {c(colmap,'sales_amount')} ELSE 0 END) AS `前期売上` 
        FROM `{VIEW_UNIFIED}` 
        CROSS JOIN fy
        {final_where} 
        GROUP BY 1,2,3 
        ORDER BY (`今期売上`-`前期売上`) {sort_order}
    """
    df_jan = query_df_safe(client, sql_jan, drill_params)
    if not df_jan.empty:
        df_jan["前年差額"] = df_jan["今期売上"] - df_jan["前期売上"]
        st.dataframe(df_jan.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

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
    df_month = query_df_safe(client, sql_month, drill_params)
    if not df_month.empty:
        df_month["前年差額"] = df_month["今期売上"] - df_month["前期売上"]
        st.dataframe(df_month.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)


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
    mode = st.radio("表示単位", ["🏢 グループ", "🏥 得意先", "💊 商品（YJ=成分規格）"], horizontal=True, key="nd_trend_mode")

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

    unified_has_jan = (c(unified_colmap, "jan_code") != "jan_code")
    unified_has_yj = (c(unified_colmap, "yj_code") != "yj_code")

    if unified_has_jan:
        yj_expr = f"CAST({c(unified_colmap,'yj_code')} AS STRING)" if unified_has_yj else "NULL"
        item_dim_sql = f"""
          SELECT
            CAST({c(unified_colmap,'jan_code')} AS STRING) AS jan_code,
            {yj_expr} AS yj_code,
            ANY_VALUE(REGEXP_REPLACE(CAST({c(unified_colmap,'product_name')} AS STRING), r"[/／].*$", "")) AS ingredient_spec_name
          FROM `{VIEW_UNIFIED}`
          GROUP BY jan_code, yj_code
        """
    else:
        if c(nd_colmap, "product_name") != "product_name":
            item_dim_sql = f"""
              SELECT
                CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
                NULL AS yj_code,
                ANY_VALUE(REGEXP_REPLACE(CAST(nd.{c(nd_colmap,'product_name')} AS STRING), r"[/／].*$", "")) AS ingredient_spec_name
              FROM `{VIEW_NEW_DELIVERY}` nd
              GROUP BY jan_code
            """
        else:
            item_dim_sql = f"""
              SELECT
                CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
                NULL AS yj_code,
                '不明' AS ingredient_spec_name
              FROM `{VIEW_NEW_DELIVERY}` nd
              GROUP BY jan_code
            """

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
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          item_dim AS ({item_dim_sql})
          SELECT
            COALESCE(NULLIF(id.yj_code, ''), CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS yj_key,
            ANY_VALUE(id.ingredient_spec_name) AS ingredient_spec_name,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'customer_code')} AS STRING)) AS customer_cnt,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS jan_cnt,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
          GROUP BY yj_key
          ORDER BY sales_amount DESC
          LIMIT 500
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Items(YJ)")
        key_col = "yj_key"
        title = "💊 商品トレンド（YJ=成分規格集約）"

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
                "yj_key": "YJキー",
                "ingredient_spec_name": "成分規格（代表）",
                "customer_cnt": "得意先数",
                "jan_cnt": "JAN数",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )
        display_cols = ["☑", "YJキー", "成分規格（代表）", "得意先数", "JAN数", "売上", "粗利"]
        pick_col = "YJキー"

    df_view = df_parent[display_cols].copy()
    for colx in df_view.columns:
        if colx != "☑":
            df_view[colx] = df_view[colx].fillna("")

    edited = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        disabled=[c_ for c_ in display_cols if c_ != "☑"],
        column_config={
            "☑": st.column_config.CheckboxColumn("選択", help="明細を表示したい行にチェック（複数可）"),
        },
        key=f"nd_trend_editor_{key_col}_v2",
    )

    sel_df = edited[edited["☑"].astype(bool) == True]
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
          cust_dim AS ({cust_dim_sql}),
          item_dim AS ({item_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE) AS first_sales_date,
            COALESCE(cd.group_name, '未設定') AS group_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            COALESCE(NULLIF(id.yj_code, ''), CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS yj_key,
            ANY_VALUE(id.ingredient_spec_name) AS ingredient_spec_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND COALESCE(cd.group_name, '未設定') IN UNNEST(@group_keys)
          GROUP BY first_sales_date, group_name, customer_code, yj_key
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
                "yj_key": "YJキー",
                "ingredient_spec_name": "成分規格（代表）",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )

    elif key_col == "customer_code":
        params2 = dict(base_params)
        params2["customer_keys"] = selected_keys

        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql}),
          item_dim AS ({item_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE) AS first_sales_date,
            COALESCE(cd.group_name, '未設定') AS group_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            COALESCE(NULLIF(id.yj_code, ''), CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS yj_key,
            ANY_VALUE(id.ingredient_spec_name) AS ingredient_spec_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) IN UNNEST(@customer_keys)
          GROUP BY first_sales_date, group_name, customer_code, yj_key
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
                "yj_key": "YJキー",
                "ingredient_spec_name": "成分規格（代表）",
                "sales_amount": "売上",
                "gross_profit": "粗利",
            }
        )

    else:
        params2 = dict(base_params)
        params2["yj_keys"] = selected_keys

        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql}),
          item_dim AS ({item_dim_sql})
          SELECT
            COALESCE(NULLIF(id.yj_code, ''), CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) AS yj_key,
            ANY_VALUE(id.ingredient_spec_name) AS ingredient_spec_name,
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
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_staff}
            AND COALESCE(NULLIF(id.yj_code, ''), CAST(nd.{c(nd_colmap,'jan_code')} AS STRING)) IN UNNEST(@yj_keys)
          GROUP BY yj_key, customer_code
          ORDER BY sales_amount DESC
          LIMIT 5000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Item(YJ) -> Customers")

        df_detail = df_detail.rename(
            columns={
                "yj_key": "YJキー",
                "ingredient_spec_name": "成分規格（代表）",
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
# 5. Main Loop
# -----------------------------
def main() -> None:
    set_page()

    if "use_bqstorage" not in st.session_state:
        st.session_state["use_bqstorage"] = True

    client = setup_bigquery_client()

    colmap = resolve_unified_colmap(client)
    missing = colmap.get("_missing_required")
    if missing:
        st.error("VIEW_UNIFIED の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing}")
        st.stop()

    nd_map = resolve_new_delivery_colmap(client)

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

        with st.expander("🔧 VIEW_UNIFIED 列マップ（自動解決結果）", expanded=False):
            st.json(colmap)

        with st.expander("🔧 VIEW_NEW_DELIVERY 列マップ（自動解決結果）", expanded=False):
            st.json(nd_map)

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

    scope = render_scope_filters(client, role, colmap)
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
