# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.9 (True "踏襲＋追加" / Backward Compatible)

【踏襲（v1.4.8の既存機能は維持）】
- YoY：VIEW_UNIFIED から動的集計に統一（YJ同一で商品名が2行問題を抑止）
- YoY：第一階層を「クリック選択」対応（モード切替でも選択保持）
- スコープ：得意先グループ列候補を VIEW_UNIFIED のスキーマから自動判定
- Group Display: official先頭 + raw併記
- 得意先グループ / 得意先単体の切替 ＆ 商品要因ドリルダウン（全件表示）
- 順位アイコン、WHERE二重対策、選択消失バグ対策、表示順序最適化
- ★Reco（VIEW_RECOMMEND）の customer_code が INT64 のため CAST対応（v1.4.8踏襲）

【追加（必須要件）】
- ★fact列名の自動解決（jan/pack/yj 等）：schemaを見て自動吸収（後方互換）
- ★管理者スコープ必須：スコープ無しは原則ブロック（例外許可を明示）
- ★全件表示の段階UI：推定（DryRun）→ 同意 → 実行（maximumBytesBilled付き）
- ★FYTDサマリーに「当月（MTD）の売上・粗利」を追加（今が2月なら2/1〜今日）
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, Optional, Tuple, List

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

DEFAULT_USD_PER_TB = 5.0
DEFAULT_USDJPY = 150.0
BYTES_PER_TB = 1024 ** 4


# -----------------------------
# 2. Helpers (表示用)
# -----------------------------
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.9｜実働維持 + 列名差異吸収 + 管理者フルスキャン統制（推定→同意→実行） + 当月(MTD)表示")


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


def bq_ident(name: str) -> str:
    s = str(name).strip()
    if not s:
        raise ValueError("Empty identifier")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
        raise ValueError(f"Unsafe identifier: {s}")
    return f"`{s}`"


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
    maximum_bytes_billed: Optional[int] = None,
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

        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = int(maximum_bytes_billed)

        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except Exception as e:
        st.error(f"クエリエラー ({label}):\n{e}")
        return pd.DataFrame()


def estimate_query_bytes(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "Estimate",
) -> Optional[int]:
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        if params:
            query_params = []
            for key, raw_value in params.items():
                p_type, p_value = _normalize_param(raw_value)
                query_params.append(bigquery.ScalarQueryParameter(key, p_type, p_value))
            job_config.query_parameters = query_params

        job = client.query(sql, job_config=job_config)
        return int(job.total_bytes_processed or 0)
    except Exception as e:
        st.warning(f"推定に失敗しました（{label}）: {e}")
        return None


def bytes_to_human(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    v = float(n)
    for u in units:
        v /= 1024.0
        if v < 1024.0:
            return f"{v:.2f} {u}"
    return f"{v:.2f} EB"


def estimate_cost_jpy(bytes_processed: int, usd_per_tb: float, usd_jpy: float) -> float:
    tb = bytes_processed / BYTES_PER_TB
    return tb * float(usd_per_tb) * float(usd_jpy)


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

    def is_empty(self) -> bool:
        return not self.predicates


@dataclass(frozen=True)
class AdminScanPolicy:
    require_scope: bool = True
    allow_full_scan: bool = False
    max_bytes_billed: int = 0
    must_estimate_before_run: bool = True


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


# -----------------------------
# 3.1 Schema utilities (列名差異吸収の基盤)
# -----------------------------
@st.cache_data(ttl=3600)
def get_table_columns_lower(_client: bigquery.Client, table_fqn: str) -> set[str]:
    project_id, dataset_id, table_name = _split_table_fqn(table_fqn)
    sql = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
    """
    df = query_df_safe(_client, sql, {"table_name": table_name}, f"Schema Check: {table_name}")
    if df.empty or "column_name" not in df.columns:
        return set()
    return {str(c).lower() for c in df["column_name"].dropna().tolist()}


def resolve_col(table_fqn: str, cols_lower: set[str], candidates: List[str], required: bool = True) -> Optional[str]:
    for c in candidates:
        if c.lower() in cols_lower:
            return c
    if required:
        return None
    return None


@st.cache_data(ttl=3600)
def resolve_view_columns_map(_client: bigquery.Client) -> Dict[str, Dict[str, str]]:
    m: Dict[str, Dict[str, str]] = {}

    # --- VIEW_UNIFIED（fact）
    cols_u = get_table_columns_lower(_client, VIEW_UNIFIED)
    unified: Dict[str, str] = {}

    unified["customer_code"] = resolve_col(VIEW_UNIFIED, cols_u, ["customer_code", "customer_cd", "cust_code"], True) or ""
    unified["customer_name"] = resolve_col(VIEW_UNIFIED, cols_u, ["customer_name", "customer_nm", "cust_name"], True) or ""
    unified["login_email"] = resolve_col(VIEW_UNIFIED, cols_u, ["login_email", "email", "user_email"], True) or ""
    unified["staff_name"] = resolve_col(VIEW_UNIFIED, cols_u, ["staff_name", "担当社員名", "担当者名", "sales_name"], False) or "staff_name"

    unified["yj_code"] = resolve_col(VIEW_UNIFIED, cols_u, ["yj_code", "yj", "YJCode", "YJ_CODE"], False) or "yj_code"
    unified["jan_code"] = resolve_col(VIEW_UNIFIED, cols_u, ["jan_code", "jan", "JAN", "JAN_CODE"], False) or "jan_code"
    unified["product_name"] = resolve_col(VIEW_UNIFIED, cols_u, ["product_name", "item_name", "商品名称", "item_nm"], True) or ""
    unified["package_unit"] = resolve_col(VIEW_UNIFIED, cols_u, ["package_unit", "pack_unit", "pack", "包装単位", "packaging"], False) or "package_unit"

    unified["sales_date"] = resolve_col(VIEW_UNIFIED, cols_u, ["sales_date", "date", "販売日"], True) or ""
    unified["fiscal_year"] = resolve_col(VIEW_UNIFIED, cols_u, ["fiscal_year", "FY", "会計年度"], True) or ""
    unified["sales_amount"] = resolve_col(VIEW_UNIFIED, cols_u, ["sales_amount", "sales", "売上", "合計価格"], True) or ""
    unified["gross_profit"] = resolve_col(VIEW_UNIFIED, cols_u, ["gross_profit", "gp", "粗利"], True) or ""

    m["unified"] = unified

    # --- VIEW_NEW_DELIVERY（新規納品）
    cols_nd = get_table_columns_lower(_client, VIEW_NEW_DELIVERY)
    nd: Dict[str, str] = {}
    nd["first_sales_date"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["first_sales_date", "first_sale_date", "first_date"], True) or ""
    nd["customer_code"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["customer_code", "customer_cd"], True) or ""
    nd["jan_code"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["jan_code", "jan", "JAN"], True) or ""
    nd["sales_amount"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["sales_amount", "sales", "売上"], True) or ""
    nd["gross_profit"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["gross_profit", "gp", "粗利"], True) or ""
    nd["login_email"] = resolve_col(VIEW_NEW_DELIVERY, cols_nd, ["login_email", "email", "user_email"], False) or ""
    m["new_delivery"] = nd

    # --- VIEW_ADOPTION（採用・失注アラート）
    cols_ad = get_table_columns_lower(_client, VIEW_ADOPTION)
    ad: Dict[str, str] = {}
    ad["login_email"] = resolve_col(VIEW_ADOPTION, cols_ad, ["login_email", "email", "user_email"], False) or ""
    ad["staff_name"] = resolve_col(VIEW_ADOPTION, cols_ad, ["staff_name", "担当者名"], False) or ""
    ad["customer_code"] = resolve_col(VIEW_ADOPTION, cols_ad, ["customer_code", "customer_cd"], True) or ""
    ad["customer_name"] = resolve_col(VIEW_ADOPTION, cols_ad, ["customer_name", "customer_nm"], True) or ""
    ad["product_name"] = resolve_col(VIEW_ADOPTION, cols_ad, ["product_name", "item_name", "商品名"], True) or ""
    ad["last_purchase_date"] = resolve_col(VIEW_ADOPTION, cols_ad, ["last_purchase_date", "last_date", "最終購入日"], True) or ""
    ad["adoption_status"] = resolve_col(VIEW_ADOPTION, cols_ad, ["adoption_status", "status", "ステータス"], True) or ""
    ad["current_fy_sales"] = resolve_col(VIEW_ADOPTION, cols_ad, ["current_fy_sales", "cur_sales", "今期売上"], True) or ""
    ad["previous_fy_sales"] = resolve_col(VIEW_ADOPTION, cols_ad, ["previous_fy_sales", "py_sales", "前期売上"], True) or ""
    m["adoption"] = ad

    # --- VIEW_RECOMMEND（Reco）
    cols_rc = get_table_columns_lower(_client, VIEW_RECOMMEND)
    rc: Dict[str, str] = {}
    rc["customer_code"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["customer_code", "customer_cd"], True) or ""
    rc["customer_name"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["customer_name", "customer_nm"], False) or ""
    rc["strong_category"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["strong_category", "category", "強み分類"], False) or ""
    rc["priority_rank"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["priority_rank", "rank", "順位"], True) or ""
    rc["recommend_jan"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["recommend_jan", "jan", "JAN"], False) or ""
    rc["recommend_product"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["recommend_product", "product_name", "推奨商品"], True) or ""
    rc["manufacturer"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["manufacturer", "maker", "メーカー"], False) or ""
    rc["market_scale"] = resolve_col(VIEW_RECOMMEND, cols_rc, ["market_scale", "scale", "市場規模"], False) or ""
    m["recommend"] = rc

    return m


def require_colmap_ok(colmap: Dict[str, Dict[str, str]]) -> bool:
    # 必須（unified）
    u = colmap.get("unified", {})
    required_unified = ["customer_code", "customer_name", "login_email", "product_name", "sales_date", "fiscal_year", "sales_amount", "gross_profit"]
    missing = [k for k in required_unified if not u.get(k)]
    if missing:
        st.error(f"VIEW_UNIFIED 必須列が解決できません: {missing}\n（列名差異吸収に失敗。VIEWのスキーマを確認してください）")
        return False
    return True


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
# 3.2 Customer Group resolution (踏襲)
# -----------------------------
@st.cache_data(ttl=3600)
def get_unified_columns(_client: bigquery.Client) -> set[str]:
    return get_table_columns_lower(_client, VIEW_UNIFIED)


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
# 3.3 Admin full scan policy (段階UI)
# -----------------------------
def get_admin_scan_policy(role: RoleInfo) -> AdminScanPolicy:
    if not role.role_admin_view:
        return AdminScanPolicy(require_scope=False, allow_full_scan=False, max_bytes_billed=0, must_estimate_before_run=False)

    with st.sidebar.expander("🛡️ 管理者フルスキャン統制（必読）", expanded=False):
        st.caption("スコープ未指定＝全社フルスキャンです。実働保護のため推定→上限→実行の順で統制します。")
        allow_full_scan = st.checkbox("スコープ未指定のフルスキャンを許可する（例外）", value=False, key="admin_allow_full_scan")
        must_estimate = st.checkbox("フルスキャン時は推定（DryRun）を必須にする", value=True, key="admin_must_estimate")

        usd_per_tb = st.number_input("推定単価（USD / TB）", min_value=0.0, max_value=50.0, value=float(st.session_state.get("usd_per_tb", DEFAULT_USD_PER_TB)), step=0.5)
        usd_jpy = st.number_input("為替レート（USDJPY）", min_value=50.0, max_value=300.0, value=float(st.session_state.get("usd_jpy", DEFAULT_USDJPY)), step=1.0)
        st.session_state["usd_per_tb"] = float(usd_per_tb)
        st.session_state["usd_jpy"] = float(usd_jpy)

        max_gb = st.number_input("maximumBytesBilled 上限（GB）", min_value=0, max_value=5000, value=int(st.session_state.get("admin_max_gb", 200)), step=50)
        st.session_state["admin_max_gb"] = int(max_gb)

        if max_gb == 0:
            st.warning("上限0GB＝上限なし（非推奨）。実働保護の観点では上限設定を推奨。")

    max_bytes = int(st.session_state.get("admin_max_gb", 200)) * (1024 ** 3)
    if int(st.session_state.get("admin_max_gb", 200)) == 0:
        max_bytes = 0

    return AdminScanPolicy(
        require_scope=True,
        allow_full_scan=bool(st.session_state.get("admin_allow_full_scan", False)),
        max_bytes_billed=max_bytes,
        must_estimate_before_run=bool(st.session_state.get("admin_must_estimate", True)),
    )


def guard_and_run_query_ui(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    sql: str,
    params: Optional[Dict[str, Any]],
    label: str,
    *,
    risky_if_no_scope: bool = True,
    force_estimate: bool = False,
    timeout_sec: int = 120,
) -> pd.DataFrame:
    policy = get_admin_scan_policy(role)

    is_admin = role.role_admin_view
    scope_empty = scope.is_empty()

    if is_admin and policy.require_scope and scope_empty and risky_if_no_scope and (not policy.allow_full_scan):
        st.warning("⚠️ 管理者はスコープ必須です。スコープを指定するか、サイドバーで『フルスキャン例外許可』をONにしてください。")
        return pd.DataFrame()

    max_bytes = None
    if is_admin and scope_empty and policy.allow_full_scan and risky_if_no_scope:
        if policy.max_bytes_billed and policy.max_bytes_billed > 0:
            max_bytes = policy.max_bytes_billed
        else:
            max_bytes = None

    need_estimate = force_estimate or (is_admin and scope_empty and policy.allow_full_scan and risky_if_no_scope)
    if need_estimate:
        st.markdown("##### 🧮 推定（Dry Run）")
        colA, colB, colC = st.columns([1, 1, 2])

        estimate_key = f"estimate_bytes::{label}"
        ran_key = f"estimate_ran::{label}"

        if colA.button("推定する（DryRun）", key=f"btn_est_{label}", use_container_width=True):
            b = estimate_query_bytes(client, sql, params, label=label)
            st.session_state[estimate_key] = b if b is not None else 0
            st.session_state[ran_key] = True

        b_est = int(st.session_state.get(estimate_key, 0) or 0)
        ran = bool(st.session_state.get(ran_key, False))

        if ran:
            usd_per_tb = float(st.session_state.get("usd_per_tb", DEFAULT_USD_PER_TB))
            usd_jpy = float(st.session_state.get("usd_jpy", DEFAULT_USDJPY))
            jpy = estimate_cost_jpy(b_est, usd_per_tb, usd_jpy)

            colB.metric("推定処理量", bytes_to_human(b_est))
            colC.metric("推定コスト（目安）", f"¥{jpy:,.0f}")

            if max_bytes is not None:
                st.info(f"maximumBytesBilled: {bytes_to_human(max_bytes)}（管理者フルスキャン統制）")

            if is_admin and scope_empty and policy.allow_full_scan and policy.must_estimate_before_run:
                st.success("推定が完了しました。実行できます。")
        else:
            st.caption("フルスキャンや全件表示では、まず推定を実行してください。")

        if is_admin and scope_empty and policy.allow_full_scan and policy.must_estimate_before_run:
            if not ran:
                st.error("推定が必須です（未実行のため、実行ボタンは無効）。")
                return pd.DataFrame()

    st.markdown("##### ▶ 実行")
    run_disabled = False
    if is_admin and scope_empty and policy.allow_full_scan and policy.must_estimate_before_run and need_estimate:
        ran = bool(st.session_state.get(f"estimate_ran::{label}", False))
        if not ran:
            run_disabled = True

    if st.button(f"実行：{label}", key=f"btn_run_{label}", use_container_width=True, disabled=run_disabled):
        return query_df_safe(
            client,
            sql,
            params=params,
            label=label,
            timeout_sec=timeout_sec,
            maximum_bytes_billed=max_bytes,
        )

    return pd.DataFrame()


# -----------------------------
# 3.4 Scope filters (踏襲＋安全補強)
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
    raw_role = str(row.get("role_tier", "")).strip().upper()
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

    # ★追加：当月（MTD）
    s_mtd = get_safe_float(row, "sales_amount_mtd")
    gp_mtd = get_safe_float(row, "gross_profit_mtd")

    st.caption(
        "※ 【今期予測】はAI予測ではなく、「今期実績 × (昨年度着地 ÷ 前年同期)」"
        "による季節変動を加味した推移ペース（着地見込）です。"
    )

    st.markdown("##### ■ 売上")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("① 今期累計(FYTD)", f"¥{s_cur:,.0f}")
    c2.metric("② 前年同期(YTD)", f"¥{s_py_ytd:,.0f}", delta=f"{int(s_cur - s_py_ytd):,}" if s_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{s_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{s_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{s_fc - s_py_total:,.0f}", delta=f"{int(s_fc - s_py_total):,}")
    c6.metric("★ 当月(MTD)", f"¥{s_mtd:,.0f}")

    st.markdown("##### ■ 粗利")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("① 今期累計(FYTD)", f"¥{gp_cur:,.0f}")
    c2.metric("② 前年同期(YTD)", f"¥{gp_py_ytd:,.0f}", delta=f"{int(gp_cur - gp_py_ytd):,}" if gp_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{gp_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{gp_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{gp_fc - gp_py_total:,.0f}", delta=f"{int(gp_fc - gp_py_total):,}")
    c6.metric("★ 当月(MTD)", f"¥{gp_mtd:,.0f}")


def render_fytd_org_section(client: bigquery.Client, colmap: Dict[str, Dict[str, str]]) -> None:
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")

    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    if st.button("全社データを読み込む", key="btn_org_load"):
        st.session_state.org_data_loaded = True

    if st.session_state.get("org_data_loaded"):
        u = colmap["unified"]
        sales_date = bq_ident(u["sales_date"])
        fiscal_year = bq_ident(u["fiscal_year"])
        sales_amount = bq_ident(u["sales_amount"])
        gross_profit = bq_ident(u["gross_profit"])

        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH) AS month_start,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN {fiscal_year} = current_fy THEN {sales_amount} ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN {fiscal_year} = current_fy THEN {gross_profit} ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 AND {sales_date} <= py_today THEN {sales_amount} ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 AND {sales_date} <= py_today THEN {gross_profit} ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 THEN {sales_amount} ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 THEN {gross_profit} ELSE 0 END) AS gross_profit_py_total,
              SUM(CASE WHEN {sales_date} >= month_start AND {sales_date} <= today THEN {sales_amount} ELSE 0 END) AS sales_amount_mtd,
              SUM(CASE WHEN {sales_date} >= month_start AND {sales_date} <= today THEN {gross_profit} ELSE 0 END) AS gross_profit_mtd
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
        """
        df_org = query_df_safe(client, sql, None, "Org Summary")
        if not df_org.empty:
            render_summary_metrics(df_org.iloc[0])


def render_fytd_me_section(client: bigquery.Client, login_email: str, colmap: Dict[str, Dict[str, str]]) -> None:
    st.subheader("👤 年度累計（FYTD）｜個人サマリー")
    if st.button("自分の成績を読み込む", key="btn_me_load"):
        u = colmap["unified"]
        sales_date = bq_ident(u["sales_date"])
        fiscal_year = bq_ident(u["fiscal_year"])
        sales_amount = bq_ident(u["sales_amount"])
        gross_profit = bq_ident(u["gross_profit"])
        login_col = bq_ident(u["login_email"])

        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH) AS month_start,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN {fiscal_year} = current_fy THEN {sales_amount} ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN {fiscal_year} = current_fy THEN {gross_profit} ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 AND {sales_date} <= py_today THEN {sales_amount} ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 AND {sales_date} <= py_today THEN {gross_profit} ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 THEN {sales_amount} ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN {fiscal_year} = current_fy - 1 THEN {gross_profit} ELSE 0 END) AS gross_profit_py_total,
              SUM(CASE WHEN {sales_date} >= month_start AND {sales_date} <= today THEN {sales_amount} ELSE 0 END) AS sales_amount_mtd,
              SUM(CASE WHEN {sales_date} >= month_start AND {sales_date} <= today THEN {gross_profit} ELSE 0 END) AS gross_profit_mtd
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
            WHERE {login_col} = @login_email
        """
        df_me = query_df_safe(client, sql, {"login_email": login_email}, "Me Summary")
        if not df_me.empty:
            render_summary_metrics(df_me.iloc[0])


def render_group_underperformance_section(client: bigquery.Client, role: RoleInfo, scope: ScopeFilter) -> None:
    st.subheader("🏢 得意先・グループ別パフォーマンス ＆ 要因分析")

    # --- UI（親：グループ/得意先、ベスト/ワースト） ---
    c1, c2 = st.columns(2)
    view_choice = c1.radio("📊 分析の単位", ["🏢 グループ別", "🏥 得意先単体"], horizontal=True, key="grpperf_view_choice")
    mode_choice = c2.radio("🏆 ランキング基準", ["📉 下落幅ワースト", "📈 上昇幅ベスト"], horizontal=True, key="grpperf_mode_choice")

    perf_view = "グループ別" if "グループ別" in view_choice else "得意先別"
    perf_mode = "ワースト" if "ワースト" in mode_choice else "ベスト"
    sort_order = "ASC" if perf_mode == "ワースト" else "DESC"

    group_expr, group_src = resolve_customer_group_sql_expr(client)
    if perf_view == "グループ別" and not group_expr:
        st.info("グループ分析に利用できる列が見つかりません（VIEW_UNIFIEDにグループ列がありません）。")
        return

    # --- フィルタ組立 ---
    role_filter = "" if role.role_admin_view else "login_email = @login_email"
    scope_filter_clause = scope.where_clause()
    filter_sql = _compose_where(role_filter, scope_filter_clause)

    params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        params["login_email"] = role.login_email

    # --- 親ランキングSQL ---
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
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS `今期売上`,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS `前年同期売上`,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS `今期粗利`,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS `前年同期粗利`
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
              customer_code AS `コード`,
              ANY_VALUE(customer_name) AS `名称`,
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS `今期売上`,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS `前年同期売上`,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS `今期粗利`,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS `前年同期粗利`
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

    # --- 表示用の派生列 ---
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

    # ★重要：選択状態を永続化（☑などでrerunしてもドリルダウンが消えない）
    sel_state_key = f"grpperf_selected_id::{perf_view}::{perf_mode}"
    sel_name_key = f"grpperf_selected_name::{perf_view}::{perf_mode}"

    if st.button("選択解除（ドリルダウンを閉じる）", key=f"grpperf_clear::{perf_view}::{perf_mode}"):
        st.session_state.pop(sel_state_key, None)
        st.session_state.pop(sel_name_key, None)
        st.rerun()

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
        key=f"grid_parent::{perf_view}::{perf_mode}",
    )

    # --- 選択の取得（取れたら保存、取れないrerunでは保存値を使う） ---
    selected_parent_id = st.session_state.get(sel_state_key)
    selected_parent_name = st.session_state.get(sel_name_key)

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

            st.session_state[sel_state_key] = selected_parent_id
            st.session_state[sel_name_key] = selected_parent_name
    except Exception:
        pass

    # --- ドリルダウン（要因＝品目） ---
    if not selected_parent_id:
        st.info("上の表からグループ/得意先を1件クリックすると、品目要因（ドリルダウン）が表示されます。")
        return

    st.markdown(f"#### 🔍 要因分析（商品ベース {perf_mode}・全件一覧）")

    drill_params = dict(params)
    drill_params["parent_id"] = selected_parent_id

    if perf_view == "グループ別":
        drill_filter_sql = _compose_where(role_filter, scope_filter_clause, f"{group_expr} = @parent_id")
    else:
        drill_filter_sql = _compose_where(role_filter, scope_filter_clause, "customer_code = @parent_id")

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
              NULLIF(NULLIF(TRIM(CAST(yj_code AS STRING)), ''), '0'),
              NULLIF(NULLIF(TRIM(CAST(jan_code AS STRING)), ''), '0'),
              TRIM(CAST(product_name AS STRING))
            ) AS yj_key,
            REGEXP_REPLACE(CAST(product_name AS STRING), r"[/／].*$", "") AS product_base,
            SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS ty_sales,
            SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS py_sales
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
        ).style.format({"今期売上": "¥{:,.0f}", "前年同期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
    )


def render_yoy_section(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, Dict[str, str]],
) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJ優先｜YJ=0/nullはJANキーで追跡）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = "ワースト"  # ワースト / ベスト / 新規
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()
    if "selected_yoy_key" not in st.session_state:
        st.session_state.selected_yoy_key = "全成分を表示"

    u = colmap["unified"]
    login_col = bq_ident(u["login_email"])
    sales_date = bq_ident(u["sales_date"])
    fiscal_year = bq_ident(u["fiscal_year"])
    sales_amount = bq_ident(u["sales_amount"])
    jan_col = bq_ident(u["jan_code"])
    yj_col = bq_ident(u["yj_code"])
    prod_col = bq_ident(u["product_name"])
    pack_col = bq_ident(u["package_unit"])
    customer_name = bq_ident(u["customer_name"])

    c1, c2, c3 = st.columns(3)

    def load_yoy(mode_name: str) -> None:
        st.session_state.yoy_mode = mode_name

        role_filter = "" if role.role_admin_view else f"{login_col} = @login_email"
        scope_where = scope.where_clause()
        where_sql = _compose_where(role_filter, scope_where)

        params: Dict[str, Any] = dict(scope.params or {})
        if not role.role_admin_view:
            params["login_email"] = role.login_email

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
                  NULLIF(NULLIF(TRIM(CAST({yj_col} AS STRING)), ''), '0'),
                  NULLIF(NULLIF(TRIM(CAST({jan_col} AS STRING)), ''), '0'),
                  REGEXP_REPLACE(CAST({prod_col} AS STRING), r"[/／].*$", "")
                ) AS yj_key,
                REGEXP_REPLACE(CAST({prod_col} AS STRING), r"[/／].*$", "") AS product_base,
                SUM(CASE WHEN {fiscal_year} = fy.current_fy THEN {sales_amount} ELSE 0 END) AS ty_sales,
                SUM(CASE WHEN {fiscal_year} = fy.current_fy - 1 THEN {sales_amount} ELSE 0 END) AS py_sales
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

    role_filter = "" if role.role_admin_view else f"{login_col} = @login_email"
    scope_where = scope.where_clause()

    params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        params["login_email"] = role.login_email

    key_filter = ""
    if selected_key != "全成分を表示":
        key_expr = f"""
          COALESCE(
            NULLIF(NULLIF(TRIM(CAST({yj_col} AS STRING)), ''), '0'),
            NULLIF(NULLIF(TRIM(CAST({jan_col} AS STRING)), ''), '0'),
            REGEXP_REPLACE(CAST({prod_col} AS STRING), r"[/／].*$", "")
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
          CAST({customer_name} AS STRING) AS customer_name,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy THEN {sales_amount} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy - 1 THEN {sales_amount} ELSE 0 END) AS py_sales
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

    st.markdown("#### 🧪 原因追及：JAN・商品別（前年差額寄与）｜全件/段階UI対応")
    with st.expander("JAN・商品別の全件表示設定（重い場合は制限推奨）", expanded=False):
        jan_limit_mode = st.radio("表示件数", ["上位/下位のみ（推奨）", "全件（危険）"], horizontal=True, key="yoy_jan_limit_mode")
        if jan_limit_mode == "上位/下位のみ（推奨）":
            jan_limit = st.slider("取得件数（JAN別）", min_value=100, max_value=5000, value=800, step=100, key="yoy_jan_limit")
        else:
            jan_limit = 0
    jan_limit_sql = f"LIMIT {int(jan_limit)}" if jan_limit and jan_limit > 0 else ""

    sql_jan = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
           - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
      ),
      base AS (
        SELECT
          CAST({jan_col} AS STRING) AS jan,
          REGEXP_REPLACE(CAST({prod_col} AS STRING), r"[/／].*$", "") AS product_base,
          CAST({pack_col} AS STRING) AS package_unit,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy THEN {sales_amount} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy - 1 THEN {sales_amount} ELSE 0 END) AS py_sales
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
      {jan_limit_sql}
    """

    df_jan = guard_and_run_query_ui(
        client,
        role,
        scope,
        sql_jan,
        params,
        label="YoY JAN・商品別（全件/段階UI）",
        risky_if_no_scope=True,
        force_estimate=(jan_limit == 0),
        timeout_sec=180,
    )
    if not df_jan.empty:
        st.dataframe(
            df_jan.fillna(0).style.format(
                {"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年差額": "¥{:,.0f}"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("JAN別内訳がありません（または未実行）。")

    st.markdown("#### 📅 原因追及：月次推移（前年差額）")
    sql_month = f"""
      WITH fy AS (
        SELECT
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
           - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
      ),
      base AS (
        SELECT
          FORMAT_DATE('%Y-%m', {sales_date}) AS ym,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy THEN {sales_amount} ELSE 0 END) AS ty_sales,
          SUM(CASE WHEN {fiscal_year} = fy.current_fy - 1 THEN {sales_amount} ELSE 0 END) AS py_sales
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


def render_new_deliveries_section(
    client: bigquery.Client,
    role: RoleInfo,
    colmap: Dict[str, Dict[str, str]],
) -> None:
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")
    if st.button("新規納品実績を読み込む", key="btn_new_deliv"):
        nd = colmap["new_delivery"]
        first_date = bq_ident(nd["first_sales_date"])
        customer_code = bq_ident(nd["customer_code"])
        jan_code = bq_ident(nd["jan_code"])
        sales_amount = bq_ident(nd["sales_amount"])
        gross_profit = bq_ident(nd["gross_profit"])
        login_col = bq_ident(nd["login_email"]) if nd.get("login_email") else None

        where_ext = ""
        params = None
        if (not role.role_admin_view) and login_col is not None:
            where_ext = f"AND {login_col} = @login_email"
            params = {"login_email": role.login_email}
        elif not role.role_admin_view and login_col is None:
            st.warning("新規納品VIEWに login_email が無いため、個人絞り込みはできません（全体を表示します）。")

        sql = f"""
        WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
        SELECT
          '① 昨日' AS `期間`,
          COUNT(DISTINCT CAST({customer_code} AS STRING)) AS `得意先数`,
          COUNT(DISTINCT CAST({jan_code} AS STRING)) AS `品目数`,
          SUM({sales_amount}) AS `売上`,
          SUM({gross_profit}) AS `粗利`
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE {first_date} = DATE_SUB(today, INTERVAL 1 DAY) {where_ext}
        UNION ALL
        SELECT
          '② 直近7日',
          COUNT(DISTINCT CAST({customer_code} AS STRING)),
          COUNT(DISTINCT CAST({jan_code} AS STRING)),
          SUM({sales_amount}),
          SUM({gross_profit})
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE {first_date} >= DATE_SUB(today, INTERVAL 7 DAY) {where_ext}
        UNION ALL
        SELECT
          '③ 当月',
          COUNT(DISTINCT CAST({customer_code} AS STRING)),
          COUNT(DISTINCT CAST({jan_code} AS STRING)),
          SUM({sales_amount}),
          SUM({gross_profit})
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td
        WHERE DATE_TRUNC({first_date}, MONTH) = DATE_TRUNC(today, MONTH) {where_ext}
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


def render_adoption_alerts_section(
    client: bigquery.Client,
    role: RoleInfo,
    colmap: Dict[str, Dict[str, str]],
) -> None:
    st.subheader("🚨 採用アイテム・失注アラート")

    ad = colmap["adoption"]
    login_col = bq_ident(ad["login_email"]) if ad.get("login_email") else None

    staff_name = bq_ident(ad["staff_name"]) if ad.get("staff_name") else None
    customer_name = bq_ident(ad["customer_name"])
    product_name = bq_ident(ad["product_name"])
    last_purchase_date = bq_ident(ad["last_purchase_date"])
    adoption_status = bq_ident(ad["adoption_status"])
    current_fy_sales = bq_ident(ad["current_fy_sales"])
    previous_fy_sales = bq_ident(ad["previous_fy_sales"])

    where_clause = ""
    params = None
    if (not role.role_admin_view) and login_col is not None:
        where_clause = f"WHERE {login_col} = @login_email"
        params = {"login_email": role.login_email}
    elif not role.role_admin_view and login_col is None:
        st.warning("採用アラートVIEWに login_email が無いため、個人絞り込みはできません（全体を表示します）。")

    staff_select = f"CAST({staff_name} AS STRING) AS `担当者名`," if staff_name else "'-' AS `担当者名`,"

    sql = f"""
        SELECT
            {staff_select}
            CAST({customer_name} AS STRING) AS `得意先名`,
            CAST({product_name} AS STRING) AS `商品名`,
            {last_purchase_date} AS `最終購入日`,
            CAST({adoption_status} AS STRING) AS `ステータス`,
            {current_fy_sales} AS `今期売上`,
            {previous_fy_sales} AS `前期売上`,
            ({current_fy_sales} - {previous_fy_sales}) AS `売上差額`
        FROM `{VIEW_ADOPTION}`
        {where_clause}
        ORDER BY
            CASE
                WHEN CAST({adoption_status} AS STRING) LIKE '%🔴%' THEN 1
                WHEN CAST({adoption_status} AS STRING) LIKE '%🟡%' THEN 2
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


def render_customer_drilldown(
    client: bigquery.Client,
    role: RoleInfo,
    scope: ScopeFilter,
    colmap: Dict[str, Dict[str, str]],
) -> None:
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")

    u = colmap["unified"]
    login_col = bq_ident(u["login_email"])
    customer_code = bq_ident(u["customer_code"])
    customer_name = bq_ident(u["customer_name"])

    role_filter = "" if role.role_admin_view else f"{login_col} = @login_email"
    scope_filter = scope.where_clause()
    customer_where = _compose_where(role_filter, scope_filter, f"{customer_name} IS NOT NULL")

    customer_params: Dict[str, Any] = dict(scope.params or {})
    if not role.role_admin_view:
        customer_params["login_email"] = role.login_email

    # 管理者でスコープなしは危険 → 段階UI
    sql_cust = f"""
        SELECT DISTINCT
          CAST({customer_code} AS STRING) AS customer_code,
          CAST({customer_name} AS STRING) AS customer_name
        FROM `{VIEW_UNIFIED}`
        {customer_where}
        ORDER BY customer_name
        LIMIT 3000
    """
    df_cust = guard_and_run_query_ui(
        client,
        role,
        scope,
        sql_cust,
        customer_params,
        label="得意先一覧を取得（ドリルダウン）",
        risky_if_no_scope=True,
        force_estimate=False,
        timeout_sec=120,
    )
    if df_cust.empty:
        st.info("表示できる得意先データがありません（または未実行）。")
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

    ad = colmap["adoption"]
    ad_customer_code = bq_ident(ad["customer_code"])
    ad_product_name = bq_ident(ad["product_name"])
    ad_status = bq_ident(ad["adoption_status"])
    ad_last = bq_ident(ad["last_purchase_date"])
    ad_cur = bq_ident(ad["current_fy_sales"])
    ad_py = bq_ident(ad["previous_fy_sales"])

    sql_adopt = f"""
        SELECT
            CAST({ad_product_name} AS STRING) AS `商品名`,
            CAST({ad_status} AS STRING) AS `ステータス`,
            {ad_last} AS `最終購入日`,
            {ad_cur} AS `今期売上`,
            {ad_py} AS `前期売上`
        FROM `{VIEW_ADOPTION}`
        WHERE CAST({ad_customer_code} AS STRING) = @c
        ORDER BY
            CASE
                WHEN CAST({ad_status} AS STRING) LIKE '%🟢%' THEN 1
                WHEN CAST({ad_status} AS STRING) LIKE '%🟡%' THEN 2
                ELSE 3
            END,
            {ad_cur} DESC
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

    rc = colmap["recommend"]
    rc_customer_code = bq_ident(rc["customer_code"])
    rc_rank = bq_ident(rc["priority_rank"])
    rc_prod = bq_ident(rc["recommend_product"])
    rc_manu = bq_ident(rc["manufacturer"]) if rc.get("manufacturer") else None
    rc_cat = bq_ident(rc["strong_category"]) if rc.get("strong_category") else None
    rc_scale = bq_ident(rc["market_scale"]) if rc.get("market_scale") else None

    select_cols = [
        f"{rc_rank} AS priority_rank",
        f"{rc_prod} AS recommend_product",
    ]
    if rc_manu:
        select_cols.append(f"{rc_manu} AS manufacturer")
    else:
        select_cols.append("NULL AS manufacturer")
    if rc_cat:
        select_cols.append(f"{rc_cat} AS strong_category")
    else:
        select_cols.append("NULL AS strong_category")
    if rc_scale:
        select_cols.append(f"{rc_scale} AS market_scale")
    else:
        select_cols.append("NULL AS market_scale")

    sql_rec = f"""
        SELECT
          {", ".join(select_cols)}
        FROM `{VIEW_RECOMMEND}`
        WHERE CAST({rc_customer_code} AS STRING) = @c
        ORDER BY priority_rank ASC
        LIMIT 10
    """
    df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
    if not df_rec.empty:
        df_disp = df_rec.rename(
            columns={
                "priority_rank": "順位",
                "recommend_product": "推奨商品",
                "manufacturer": "メーカー",
                "strong_category": "強み分類",
                "market_scale": "市場規模",
            }
        )[["順位", "推奨商品", "メーカー", "強み分類", "市場規模"]]
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

        st.divider()
        st.caption("※ 管理者フルスキャン統制は、ログイン後に有効化されます。")

    if not login_id or not login_pw:
        st.info("👈 サイドバーからログインしてください。")
        return

    role = resolve_role(client, login_id.strip(), login_pw.strip())
    if not role.is_authenticated:
        st.error("❌ ログイン情報が正しくありません。")
        return

    # 列名差異吸収（必須）
    colmap = resolve_view_columns_map(client)
    if not require_colmap_ok(colmap):
        return

    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    c1, c2, c3 = st.columns(3)
    c1.metric("👤 担当", role.staff_name)
    c2.metric("🛡️ 権限", role.role_key)
    c3.metric("📞 電話", role.phone)
    st.divider()

    # 表示順序：サマリーを最上部（踏襲 + MTD追加）
    if role.role_admin_view:
        render_fytd_org_section(client, colmap)
    else:
        render_fytd_me_section(client, role.login_email, colmap)

    st.divider()

    # スコープ設定
    scope = render_scope_filters(client, role)
    st.divider()

    # 管理者フルスキャン統制UI（ログイン後に表示）
    _ = get_admin_scan_policy(role)

    if role.role_admin_view:
        render_group_underperformance_section(client, role, scope, colmap)
        st.divider()
        render_yoy_section(client, role, scope, colmap)
        st.divider()
        render_new_deliveries_section(client, role, colmap)
        st.divider()
        render_adoption_alerts_section(client, role, colmap)
        st.divider()
        render_customer_drilldown(client, role, scope, colmap)
    else:
        render_yoy_section(client, role, scope, colmap)
        st.divider()
        render_new_deliveries_section(client, role, colmap)
        st.divider()
        render_adoption_alerts_section(client, role, colmap)
        st.divider()
        render_customer_drilldown(client, role, scope, colmap)


if __name__ == "__main__":
    main()
