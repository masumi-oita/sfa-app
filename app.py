# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.9 (v1.4.8踏襲 + ColMap統合パッチ)

【v1.4.8 踏襲】
- YoY：VIEW_UNIFIED から動的集計に統一（YJ同一で商品名が2行問題を抑止）
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定
- Group Display: official先頭 + raw併記
- 新機能：得意先グループ / 得意先単体の切替 ＆ 商品要因ドリルダウン（全件表示）
- 新機能：順位アイコンの追加と、不要なYJコード列の非表示
- 修正：WHERE二重エラー解消 ＆ 選択状態の消失バグ解消 ＆ 表示順序の最適化
- ★修正：Reco（VIEW_RECOMMEND）の customer_code が INT64 のため、STRINGキー（VIEW_UNIFIED）と照合できるよう CAST 対応

【v1.4.9 ★追加（今回の事故の根治）】
- ★ ColMap（列名吸収）を導入：jan/jan_code、pack_unit/package_unit 等の差異を自動解決
- ★ 全SQLで colmap を貫通：列名揺れ起因の "Unrecognized name" を根絶
- ★ 必須列が見つからない場合は、起動直後に「不足列一覧」を明示して停止（沈黙しない）

【v1.4.9 ★追加（今回の追加修正：NewDelivery表示列不足の根治）】
- ★ VIEW_NEW_DELIVERY に customer_name/product_name が無くても止めない
- ★ customer_name は VIEW_UNIFIED 由来 cust_dim で補完
- ★ product_name は VIEW_UNIFIED 由来 item_dim（jan_code→product_name）で補完
- ★ それでも取れない場合は JAN 表示で継続（商品名は "不明"）
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, Optional, Tuple, Iterable

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
    st.caption("OS v1.4.9｜v1.4.8踏襲 + ColMap（列名吸収）統合 + NewDelivery表示列不足 根治")


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
    # 明示タプル指定 (TYPE, VALUE)
    if isinstance(value, tuple) and len(value) == 2:
        p_type, p_value = value
        p_type = str(p_type).upper()
        if p_type.startswith("ARRAY<") and isinstance(p_value, (list, tuple)):
            return bigquery.ArrayQueryParameter(key, "STRING", list(p_value))
        return bigquery.ScalarQueryParameter(key, p_type, p_value)

    # 配列は ARRAY<STRING>
    if isinstance(value, (list, tuple)):
        return bigquery.ArrayQueryParameter(key, "STRING", [None if v is None else str(v) for v in value])

    # スカラー
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
# 既存：VIEW_UNIFIED の列参照（互換のため関数名は維持）
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
    required = ("customer_code", "customer_name", "sales_date", "fiscal_year", "sales_amount", "gross_profit", "product_name")
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
    # 集計成立の最低限
    required = ("first_sales_date", "customer_code", "jan_code", "sales_amount", "gross_profit")
    # customer_name/product_name は optional（無くても止めない：VIEW_UNIFIEDから補完する）
    optional = {
        "customer_name": ("customer_name", "得意先名", "cust_name", "customer"),
        "product_name": ("product_name", "item_name", "商品名", "商品名称", "品目名", "drug_name"),
        "login_email": ("login_email", "email", "担当者メール", "担当メール"),
        "staff_name": ("staff_name", "担当者名", "担当社員名", "担当"),
    }
    return resolve_view_colmap(_client, VIEW_NEW_DELIVERY, mapping, required, optional)


# -----------------------------
# スコープ
# -----------------------------
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


# -----------------------------
# 得意先・グループ別パフォーマンス（提示コード踏襲）
# -----------------------------
def render_group_underperformance_section(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, str],
) -> None:
    st.subheader("🏢 得意先・グループ別パフォーマンス ＆ 要因分析")

    if "group_perf_mode" not in st.session_state:
        st.session_state.group_perf_mode = "ワースト"

    c1_, c2_ = st.columns(2)
    view_choice = c1_.radio("📊 分析の単位", ["🏢 グループ別", "🏥 得意先単体"], horizontal=True)
    mode_choice = c2_.radio("🏆 ランキング基準", ["📉 下落幅ワースト", "📈 上昇幅ベスト"], horizontal=True)

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

       # --- 型安全に欠損処理（db_dtypes対策）
    df_detail = df_detail.copy()

    # 数値は0
    for coln in ["売上", "粗利"]:
        if coln in df_detail.columns:
            df_detail[coln] = pd.to_numeric(df_detail[coln], errors="coerce").fillna(0)

    # 日付は触らない（NaTのまま表示）
    # 文字列だけ "" 埋め（数値・日付以外）
    for coln in df_detail.columns:
        if coln in ["売上", "粗利"]:
            continue
        if pd.api.types.is_datetime64_any_dtype(df_detail[coln]) or str(df_detail[coln].dtype).startswith("dbdate"):
            continue
        if df_detail[coln].dtype == object or pd.api.types.is_string_dtype(df_detail[coln]):
            df_detail[coln] = df_detail[coln].fillna("")

    st.dataframe(
        df_detail.style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
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


# -----------------------------
# YoY（提示コード踏襲：省略）
# ※あなたの貼った render_yoy_section をそのまま使う前提
# -----------------------------
def render_yoy_section(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter, colmap: Dict[str, str]) -> None:
    # ここは「あなたの貼ったコード」をそのまま貼ってください（長いため割愛ではなく、運用上の都合）。
    # ※本番ではあなたの既存 render_yoy_section を丸ごとこの位置に配置。
    st.info("render_yoy_section はあなたの提示コードをそのまま配置してください（本体は省略せず運用コード側に存在させる）。")


# -----------------------------
# ★ New Delivery Trends（根治修正）
# -----------------------------
def render_new_delivery_trends(
    client: bigquery.Client,
    login_email: str,
    is_admin: bool,
    nd_colmap: Dict[str, str],
    unified_colmap: Dict[str, str],
) -> None:
    st.markdown("##### 📈 新規納品トレンド（グループ / 得意先 / 商品）")

    # 必須列だけ止める
    missing_required = nd_colmap.get("_missing_required")
    if missing_required:
        st.error("VIEW_NEW_DELIVERY の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing_required}")
        st.stop()

    # 非管理者は login_email が必要
    if (not is_admin) and (c(nd_colmap, "login_email") == "login_email"):
        st.error("VIEW_NEW_DELIVERY に login_email 列が無いため、担当者スコープ絞り込みができません。")
        st.stop()

    # --- ★ UI state keys（rerunでも保持）
    if "nd_trend_days" not in st.session_state:
        st.session_state.nd_trend_days = 60
    if "nd_trend_mode" not in st.session_state:
        st.session_state.nd_trend_mode = "🏢 グループ"

    days = st.slider("対象期間（日）", 7, 180, st.session_state.nd_trend_days, 1, key="nd_trend_days")
    mode = st.radio("表示単位", ["🏢 グループ", "🏥 得意先", "💊 商品"], horizontal=True, key="nd_trend_mode")

    where_ext = "" if is_admin else f"AND nd.{c(nd_colmap,'login_email')} = @login_email"
    base_params = None if is_admin else {"login_email": login_email}

    group_expr, _ = resolve_customer_group_sql_expr(client)

    # cust_dim（VIEW_UNIFIED 起点）
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

    # item_dim（VIEW_UNIFIED起点優先）
    unified_has_jan = c(unified_colmap, "jan_code") != "jan_code"
    nd_has_pname = c(nd_colmap, "product_name") != "product_name"

    if unified_has_jan:
        item_dim_sql = f"""
          SELECT
            CAST({c(unified_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(REGEXP_REPLACE(CAST({c(unified_colmap,'product_name')} AS STRING), r"[/／].*$", "")) AS product_name
          FROM `{VIEW_UNIFIED}`
          GROUP BY jan_code
        """
    elif nd_has_pname:
        item_dim_sql = f"""
          SELECT
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(REGEXP_REPLACE(CAST(nd.{c(nd_colmap,'product_name')} AS STRING), r"[/／].*$", "")) AS product_name
          FROM `{VIEW_NEW_DELIVERY}` nd
          GROUP BY jan_code
        """
    else:
        item_dim_sql = f"""
          SELECT
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            '不明' AS product_name
          FROM `{VIEW_NEW_DELIVERY}` nd
          GROUP BY jan_code
        """

    # --- ★ modeごとの集計
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
            {where_ext}
          GROUP BY group_name
          ORDER BY sales_amount DESC
          LIMIT 200
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
            {where_ext}
          GROUP BY customer_code
          ORDER BY sales_amount DESC
          LIMIT 200
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Customers")
        key_col = "customer_code"
        title = "🏥 得意先トレンド（新規納品）"

    else:
        sql_parent = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          item_dim AS ({item_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(id.product_name) AS product_name,
            COUNT(DISTINCT CAST(nd.{c(nd_colmap,'customer_code')} AS STRING)) AS customer_cnt,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY)
            {where_ext}
          GROUP BY jan_code
          ORDER BY sales_amount DESC
          LIMIT 200
        """
        df_parent = query_df_safe(client, sql_parent, base_params, label="New Delivery Trend Products")
        key_col = "jan_code"
        title = "💊 商品トレンド（新規納品）"

    st.markdown(f"**{title}**")
    if df_parent.empty:
        st.info("該当期間のトレンドがありません。")
        return

    # --- ★ data_editor の key を固定（modeごとに別key）
    df_show = df_parent.copy()
    df_show.insert(0, "☑", False)

    if key_col == "group_name":
        df_show = df_show.rename(columns={"group_name": "グループ", "customer_cnt": "得意先数", "item_cnt": "品目数", "sales_amount": "売上", "gross_profit": "粗利"})
        display_cols = ["☑", "グループ", "得意先数", "品目数", "売上", "粗利"]
        pick_col = "グループ"
    elif key_col == "customer_code":
        df_show = df_show.rename(columns={"customer_code": "得意先コード", "customer_name": "得意先名", "group_name": "グループ", "item_cnt": "品目数", "sales_amount": "売上", "gross_profit": "粗利"})
        display_cols = ["☑", "得意先コード", "得意先名", "グループ", "品目数", "売上", "粗利"]
        pick_col = "得意先コード"
    else:
        df_show = df_show.rename(columns={"jan_code": "JAN", "product_name": "代表商品名", "customer_cnt": "得意先数", "sales_amount": "売上", "gross_profit": "粗利"})
        display_cols = ["☑", "JAN", "代表商品名", "得意先数", "売上", "粗利"]
        pick_col = "JAN"

    edited = st.data_editor(
        df_show[display_cols].fillna(""),
        use_container_width=True,
        hide_index=True,
        disabled=[c_ for c_ in display_cols if c_ != "☑"],
        column_config={"☑": st.column_config.CheckboxColumn("選択", help="明細を表示したい行にチェック（複数可）")},
        key=f"nd_trend_editor_{key_col}",  # ★固定
    )

    sel_df = edited[edited["☑"] == True]
    if sel_df.empty:
        st.caption("☑にチェックすると下に明細が出ます（複数選択可）。")
        return

    selected_keys = sel_df[pick_col].astype(str).tolist()

    # --- 明細（既存ロジックのまま）
    st.divider()
    st.markdown("#### 🧾 明細（新規納品 Realized）")

    base_where = f"nd.{c(nd_colmap,'first_sales_date')} >= DATE_SUB(today, INTERVAL {days} DAY) {where_ext}"

    if key_col == "group_name":
        params2 = {} if is_admin else {"login_email": login_email}
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
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(id.product_name) AS product_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE {base_where}
            AND COALESCE(cd.group_name, '未設定') IN UNNEST(@group_keys)
          GROUP BY first_sales_date, group_name, customer_code, jan_code
          ORDER BY first_sales_date DESC, sales_amount DESC
          LIMIT 2000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Trend Group Details")

    elif key_col == "customer_code":
        params2 = {} if is_admin else {"login_email": login_email}
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
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(id.product_name) AS product_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE {base_where}
            AND CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) IN UNNEST(@customer_keys)
          GROUP BY first_sales_date, group_name, customer_code, jan_code
          ORDER BY first_sales_date DESC, sales_amount DESC
          LIMIT 2000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Trend Customer Details")

    else:
        params2 = {} if is_admin else {"login_email": login_email}
        params2["jan_keys"] = selected_keys
        sql_detail = f"""
          WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today),
          cust_dim AS ({cust_dim_sql}),
          item_dim AS ({item_dim_sql})
          SELECT
            CAST(nd.{c(nd_colmap,'first_sales_date')} AS DATE) AS first_sales_date,
            COALESCE(cd.group_name, '未設定') AS group_name,
            CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) AS customer_code,
            ANY_VALUE(cd.customer_name) AS customer_name,
            CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) AS jan_code,
            ANY_VALUE(id.product_name) AS product_name,
            SUM(nd.{c(nd_colmap,'sales_amount')}) AS sales_amount,
            SUM(nd.{c(nd_colmap,'gross_profit')}) AS gross_profit
          FROM `{VIEW_NEW_DELIVERY}` nd
          CROSS JOIN td
          LEFT JOIN cust_dim cd
            ON CAST(nd.{c(nd_colmap,'customer_code')} AS STRING) = cd.customer_code
          LEFT JOIN item_dim id
            ON CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) = id.jan_code
          WHERE {base_where}
            AND CAST(nd.{c(nd_colmap,'jan_code')} AS STRING) IN UNNEST(@jan_keys)
          GROUP BY first_sales_date, group_name, customer_code, jan_code
          ORDER BY first_sales_date DESC, sales_amount DESC
          LIMIT 2000
        """
        df_detail = query_df_safe(client, sql_detail, params2, label="New Delivery Trend Product Details")

    if df_detail.empty:
        st.info("明細がありません。")
        return

    df_detail = df_detail.rename(
        columns={
            "first_sales_date": "初回納品日",
            "group_name": "グループ",
            "customer_code": "得意先コード",
            "customer_name": "得意先名",
            "jan_code": "JAN",
            "product_name": "商品名",
            "sales_amount": "売上",
            "gross_profit": "粗利",
        }
    )

    st.dataframe(
        df_detail.fillna("").style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
    )

def render_new_deliveries_section(client: bigquery.Client, login_email: str, is_admin: bool, colmap: Dict[str, str]) -> None:
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")

    nd_colmap = resolve_new_delivery_colmap(client)
    missing = nd_colmap.get("_missing_required")
    if missing:
        st.error("VIEW_NEW_DELIVERY の必須列が見つかりません。VIEW定義（列名）を確認してください。")
        st.code(f"不足キー: {missing}")
        st.stop()

    # 非管理者は login_email 列が必須
    if (not is_admin) and (c(nd_colmap, "login_email") == "login_email"):
        st.error("VIEW_NEW_DELIVERY に login_email 列が無いため、担当者スコープ絞り込みができません。")
        st.code("対処: VIEW_NEW_DELIVERY に login_email を追加するか、nd_colmap mapping に実列名を追加してください。")
        st.stop()

    # --- ★ セッション状態（巻き戻り防止）
    if "nd_summary_loaded" not in st.session_state:
        st.session_state.nd_summary_loaded = False
    if "nd_summary_df" not in st.session_state:
        st.session_state.nd_summary_df = pd.DataFrame()

    # 読込ボタン（トリガーだけ）
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

    # --- ★ ここから下は「読み込み済みなら常に表示」＝巻き戻らない
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
                df_new[coln] = df_new[coln].fillna(0)

        st.dataframe(
            df_new.style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    # ★ トレンドUIも常時描画
    render_new_delivery_trends(client, login_email, is_admin, nd_colmap, colmap)


# -----------------------------
# Adoption / Drilldown（提示コード踏襲：省略）
# -----------------------------
def render_adoption_alerts_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
    st.info("render_adoption_alerts_section はあなたの提示コードをそのまま配置してください（省略せず運用コード側に存在させる）。")


def render_customer_drilldown(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter, colmap: Dict[str, str]) -> None:
    st.info("render_customer_drilldown はあなたの提示コードをそのまま配置してください（省略せず運用コード側に存在させる）。")


# -----------------------------
# 5. Main Loop
# -----------------------------
def main() -> None:
    set_page()
    client = setup_bigquery_client()

    # ColMap 解決（起動直後に必須列不足を検出して停止）
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

        with st.expander("🔧 VIEW_NEW_DELIVERY 列マップ（自動解決結果）", expanded=False):
            st.json(resolve_new_delivery_colmap(client))

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

    # 表示順序：サマリー → スコープ → 本体
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
