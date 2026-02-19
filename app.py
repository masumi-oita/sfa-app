# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.8
(Integrated Update / Auth Hardening & Typed Params)
- YoY：VIEW_UNIFIED から動的集計に統一（YJ同一で商品名が2行問題を抑止）
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定（存在しない場合は非表示）
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
    "customer_group_name",
    "customer_group",
    "customer_segment",
    "channel_group",
    "chain_name",
    "sales_group_name"
)


# -----------------------------
# 2. Helpers (表示用)
# -----------------------------
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.8｜実態解明モード（YJ=0はJANキーで追跡）")


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
    """表示用: 包装・入数表記（例: /B500T, /1V, /10テスト用）を除去。"""
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
    """
    query_df_safe() 用の型推定ヘルパー
    - tuple("TYPE", value) の場合は明示型を優先
    - それ以外は値型から推定
    """
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


def resolve_customer_group_column(_client: bigquery.Client) -> Optional[str]:
    profiles = get_customer_group_column_profiles(_client)
    if not profiles.empty and "column_name" in profiles.columns:
        return str(profiles.iloc[0]["column_name"])

    available_cols = get_available_customer_group_columns(_client)
    return available_cols[0] if available_cols else None


def render_scope_filters(client: bigquery.Client, role: RoleInfo) -> ScopeFilter:
    st.markdown("### 🔍 分析スコープ設定")
    predicates: list[str] = []
    params: Dict[str, Any] = {}

    with st.expander("詳細絞り込み（得意先グループ・得意先名）", expanded=False):
        c1, c2 = st.columns(2)

        group_col = resolve_customer_group_column(client)
        if group_col:
            role_where = ""
            role_params: Dict[str, Any] = {}
            if not role.role_admin_view:
                role_where = "WHERE login_email = @login_email"
                role_params["login_email"] = role.login_email

            sql_group = f"""
                SELECT DISTINCT COALESCE(NULLIF(CAST({group_col} AS STRING), ''), '未設定') AS group_name
                FROM `{VIEW_UNIFIED}`
                {role_where}
                ORDER BY group_name
                LIMIT 500
            """
            df_group = query_df_safe(client, sql_group, role_params, "Scope Group Options")
            group_opts = ["指定なし"] + (df_group["group_name"].tolist() if not df_group.empty else [])
            selected_group = c1.selectbox("得意先グループ", options=group_opts)
            if selected_group != "指定なし":
                predicates.append(
                    f"COALESCE(NULLIF(CAST({group_col} AS STRING), ''), '未設定') = @scope_group"
                )
                params["scope_group"] = selected_group
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


def render_fytd_org_section(client: bigquery.Client) -> None:
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")
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
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN gross_profit ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
        """
        df_org = query_df_safe(client, sql, None, "Org Summary")
        if not df_org.empty:
            render_summary_metrics(df_org.iloc[0])


def render_fytd_me_section(client: bigquery.Client, login_email: str) -> None:
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
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN gross_profit ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
            WHERE login_email = @login_email
        """
        df_me = query_df_safe(client, sql, {"login_email": login_email}, "Me Summary")
        if not df_me.empty:
            render_summary_metrics(df_me.iloc[0])


def render_group_underperformance_section(client: bigquery.Client, role: RoleInfo, scope: ScopeFilter) -> None:
    st.subheader("🏢 得意先グループ別パフォーマンス（前年比ワースト順）")

    group_col = resolve_customer_group_column(client)
    if not group_col:
        st.info("グループ分析に利用できる列が見つかりません（VIEW_UNIFIEDにグループ列がありません）。")
        return

    role_filter = "" if role.role_admin_view else "login_email = @login_email"
    scope_filter = scope.where_clause()
    filter_sql = _compose_where(role_filter, scope_filter)

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
          COALESCE(NULLIF(CAST({group_col} AS STRING), ''), '未設定') AS `得意先グループ`,
          SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS `前年同期売上`,
          SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS `今期粗利`,
          SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS `前年同期粗利`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {filter_sql}
        GROUP BY `得意先グループ`
        HAVING `前年同期売上` > 0 OR `今期売上` > 0
        ORDER BY (`今期売上` - `前年同期売上`) ASC
        LIMIT 50
    """
    df = query_df_safe(client, sql, params, "Group Performance")
    if df.empty:
        st.info("表示できるグループデータがありません。")
        return

    df["売上差額"] = df["今期売上"] - df["前年同期売上"]
    df["売上成長率"] = df.apply(
        lambda r: ((r["今期売上"] / r["前年同期売上"] - 1) * 100) if r["前年同期売上"] else 0,
        axis=1,
    )
    df["粗利差額"] = df["今期粗利"] - df["前年同期粗利"]

    st.caption(f"抽出元グループ列: `{VIEW_UNIFIED}.{group_col}`")
    st.dataframe(
        df.style.format(
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
    )


def render_yoy_section(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJベース）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = None
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()

    # ★クリック選択保持
    if "selected_yj" not in st.session_state:
        st.session_state.selected_yj = None

    c1, c2, c3 = st.columns(3)

    def load_yj_data(mode_name: str) -> None:
        """
        スコープ有無に関係なく VIEW_UNIFIED から動的集計に統一。
        - yj_code が 0/NULL/空なら jan_code にフォールバック
        - yj_key×product_base の粒度で一旦作り、最後に yj_key に集約し「代表商品名」を決める
        """
        st.session_state.yoy_mode = mode_name

        role_filter = "" if is_admin else "login_email = @login_email"
        scope_filter = scope.where_clause()
        combined_where = _compose_where(role_filter, scope_filter)

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
              SELECT (
                EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
              ) AS current_fy
            ),
            base_raw AS (
              SELECT
                COALESCE(
                  NULLIF(NULLIF(TRIM(CAST(yj_code AS STRING)), ''), '0'),
                  TRIM(CAST(jan_code AS STRING))
                ) AS yj_key,
                REGEXP_REPLACE(CAST(product_name AS STRING), r"[/／].*$", "") AS product_base,
                SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS py_sales
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {combined_where}
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
            WHERE {diff_filter}
            ORDER BY {order_by}
            LIMIT 100
        """
        st.session_state.yoy_df = query_df_safe(client, sql, params, mode_name)

        # ★モードを変えた瞬間に「選択がテーブルから消える」事故を防ぐため、選択は維持するが
        #   新しいランキングに存在しない場合のみリセットする（後で df_disp を作って判定）
        #   ここでは何もしない（df_disp 作成後に判定）

    with c1:
        if st.button("📉 下落幅ワースト", use_container_width=True):
            load_yj_data("ワースト")
    with c2:
        if st.button("📈 上昇幅ベスト", use_container_width=True):
            load_yj_data("ベスト")
    with c3:
        if st.button("🆕 新規/比較不能", use_container_width=True):
            load_yj_data("新規")

    if st.session_state.yoy_df.empty:
        return

    df = st.session_state.yoy_df.copy()
    df["product_name"] = df["product_name"].apply(normalize_product_display_name)
    df = df.fillna(0)

    df_disp = (
        df.groupby(["yj_code"], as_index=False)
        .agg(
            product_name=("product_name", "first"),
            sales_amount=("sales_amount", "sum"),
            py_sales_amount=("py_sales_amount", "sum"),
            sales_diff_yoy=("sales_diff_yoy", "sum"),
        )
        .rename(
            columns={
                "yj_code": "YJコード",
                "product_name": "代表商品名(成分)",
                "sales_amount": "今期売上",
                "py_sales_amount": "前期売上",
                "sales_diff_yoy": "前年比差額",
            }
        )
    )

    # ★現在のランキングに存在しない selected_yj はリセット
    if st.session_state.selected_yj and st.session_state.selected_yj not in set(df_disp["YJコード"].astype(str)):
        st.session_state.selected_yj = None

    # -------------------------
    # 第一階層：クリック選択
    # -------------------------
    st.markdown(f"#### 🏆 第一階層：成分（YJ）{st.session_state.yoy_mode} ランキング")

    event = st.dataframe(
        df_disp[["YJコード", "代表商品名(成分)", "今期売上", "前期売上", "前年比差額"]].style.format(
            {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}
        ),
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun",
    )

    # クリックされた行を取得 → selected_yj に保存
    try:
        sel_rows = event.selection.rows if hasattr(event, "selection") else []
        if sel_rows:
            idx = sel_rows[0]
            st.session_state.selected_yj = str(df_disp.iloc[idx]["YJコード"])
    except Exception:
        pass

    st.divider()
    st.markdown("#### 🔍 第二階層：得意先別 / グループ別 / 原因追及（JAN・月次）")

    # 保険：クリックが無い場合は従来 selectbox
    selected_yj = st.session_state.selected_yj
    if not selected_yj:
        yj_options = {
            str(row["YJコード"]): f"{row['代表商品名(成分)']} (差額: ¥{row['前年比差額']:,.0f})"
            for _, row in df_disp.iterrows()
        }
        selected_yj = st.selectbox(
            "詳細を見たい成分（YJ）を選択してください",
            options=list(yj_options.keys()),
            format_func=lambda x: yj_options[x],
        )
        st.session_state.selected_yj = selected_yj

    if not selected_yj:
        return

    role_filter = "" if is_admin else "login_email = @login_email"
    scope_filter = scope.where_clause()
    filter_sql = _compose_where(
        'COALESCE(NULLIF(NULLIF(TRIM(CAST(yj_code AS STRING)), ""), "0"), TRIM(CAST(jan_code AS STRING))) = @yj',
        role_filter,
        scope_filter,
    )
    params: Dict[str, Any] = {"yj": selected_yj, **(scope.params or {})}
    if not is_admin:
        params["login_email"] = login_email

    sort_order = "ASC" if st.session_state.yoy_mode == "ワースト" else "DESC"

    # --- 得意先別 ---
    st.markdown("#### 🧾 得意先別内訳（前年差額）")
    sql_drill = f"""
        WITH fy AS (
          SELECT (
            EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
            - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
          ) AS current_fy
        ),
        base AS (
          SELECT
            customer_name,
            SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS ty_sales,
            SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS py_sales
          FROM `{VIEW_UNIFIED}`
          CROSS JOIN fy
          {filter_sql}
          GROUP BY customer_name
        )
        SELECT
          customer_name AS `得意先名`,
          ty_sales AS `今期売上`,
          py_sales AS `前期売上`,
          (ty_sales - py_sales) AS `前年差額`
        FROM base
        WHERE ty_sales > 0 OR py_sales > 0
        ORDER BY `前年差額` {sort_order}
        LIMIT 50
    """
    df_drill = query_df_safe(client, sql_drill, params, "YJ Drilldown Customers")
    if not df_drill.empty:
        st.dataframe(
            df_drill.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("この成分の得意先内訳データが見つかりません。")

    # --- グループ別 ---
    st.markdown("#### 🏷️ 得意先グループ別内訳（前年差額）")
    group_col = resolve_customer_group_column(client)
    if group_col:
        sql_group = f"""
            WITH fy AS (
              SELECT (
                EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
              ) AS current_fy
            ),
            base AS (
              SELECT
                COALESCE(NULLIF(CAST({group_col} AS STRING), ''), '未設定') AS customer_group,
                SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS py_sales,
                COUNT(DISTINCT CASE WHEN fiscal_year = current_fy THEN customer_code END) AS ty_customers,
                COUNT(DISTINCT CASE WHEN fiscal_year = current_fy - 1 THEN customer_code END) AS py_customers
              FROM `{VIEW_UNIFIED}`
              CROSS JOIN fy
              {filter_sql}
              GROUP BY customer_group
            )
            SELECT
              customer_group AS `得意先グループ`,
              ty_sales AS `今期売上`,
              py_sales AS `前期売上`,
              (ty_sales - py_sales) AS `前年差額`,
              ty_customers AS `今期得意先数`,
              py_customers AS `前期得意先数`
            FROM base
            WHERE ty_sales > 0 OR py_sales > 0
            ORDER BY `前年差額` {sort_order}
            LIMIT 30
        """
        df_group = query_df_safe(client, sql_group, params, "YJ Drilldown Groups")
        if not df_group.empty:
            st.caption(f"抽出元グループ列: `{VIEW_UNIFIED}.{group_col}`")
            st.dataframe(
                df_group.fillna(0).style.format(
                    {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("この成分の得意先グループ内訳データが見つかりません。")
    else:
        st.info("得意先グループ列がデータソースに存在しないため、グループ内訳は表示できません。")

    # --- 原因追及：JAN別 ---
    st.markdown("#### 🧪 原因追及：JAN別（前年差額寄与）")
    sql_root_jan = f"""
        WITH fy AS (
          SELECT (
            EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
            - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
          ) AS current_fy
        )
        SELECT
          jan_code AS `JAN`,
          ANY_VALUE(REGEXP_REPLACE(CAST(product_name AS STRING), r"[/／].*$", "")) AS `代表商品名`,
          SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS `前期売上`,
          SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END)
            - SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS `前年差額`,
          COUNT(DISTINCT CASE WHEN fiscal_year = current_fy THEN customer_code END) AS `今期得意先数`,
          COUNT(DISTINCT CASE WHEN fiscal_year = current_fy - 1 THEN customer_code END) AS `前期得意先数`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {filter_sql}
        GROUP BY jan_code
        ORDER BY `前年差額` ASC
        LIMIT 30
    """
    df_root_jan = query_df_safe(client, sql_root_jan, params, "YJ Root Cause JAN")
    if not df_root_jan.empty:
        st.dataframe(
            df_root_jan.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )

    # --- 原因追及：月次 ---
    st.markdown("#### 📅 原因追及：月次推移（前年差額）")
    sql_root_month = f"""
        WITH fy AS (
          SELECT (
            EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
            - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END
          ) AS current_fy
        )
        SELECT
          FORMAT_DATE('%Y-%m', sales_date) AS `年月`,
          SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS `今期売上`,
          SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS `前期売上`,
          SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END)
            - SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS `前年差額`
        FROM `{VIEW_UNIFIED}`
        CROSS JOIN fy
        {filter_sql}
        GROUP BY `年月`
        ORDER BY `年月`
    """
    df_root_month = query_df_safe(client, sql_root_month, params, "YJ Root Cause Month")
    if not df_root_month.empty:
        st.dataframe(
            df_root_month.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_new_deliveries_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")
    if st.button("新規納品実績を読み込む", key="btn_new_deliv"):
        where_ext = "" if is_admin else "AND login_email = @login_email"
        params = None if is_admin else {"login_email": login_email}

        sql = f"""
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
        df_new = query_df_safe(client, sql, params, label="New Deliveries")

        if not df_new.empty:
            df_new[["売上", "粗利"]] = df_new[["売上", "粗利"]].fillna(0)
            st.dataframe(
                df_new.style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("新規納品データがありません。")


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


def render_customer_drilldown(client: bigquery.Client, login_email: str, is_admin: bool, scope: ScopeFilter) -> None:
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")

    role_filter = "" if is_admin else "login_email = @login_email"
    scope_filter = scope.where_clause()
    customer_where = _compose_where(role_filter, scope_filter, "customer_name IS NOT NULL")

    customer_params: Dict[str, Any] = dict(scope.params or {})
    if not is_admin:
        customer_params["login_email"] = login_email

    sql_cust = f"""
        SELECT DISTINCT customer_code, customer_name
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
        SELECT *
        FROM `{VIEW_RECOMMEND}`
        WHERE customer_code = @c
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
    client = setup_bigquery_client()

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

    scope = render_scope_filters(client, role)
    st.divider()

    if role.role_admin_view:
        render_fytd_org_section(client)
        st.divider()
        render_group_underperformance_section(client, role, scope)
        st.divider()
        render_yoy_section(client, role.login_email, is_admin=True, scope=scope)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=True)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=True)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=True, scope=scope)
    else:
        render_fytd_me_section(client, role.login_email)
        st.divider()
        render_yoy_section(client, role.login_email, is_admin=False, scope=scope)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=False)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=False)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=False, scope=scope)


if __name__ == "__main__":
    main()
