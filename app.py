# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v2.3.0 (Restore Features / Unified Bounds / Dynamic SQL)

【踏襲（復元）した機能】
- 入口：管理者 / 担当者 のRBAC（sales_staff_master_native）
- FYTDサマリー（売上/粗利/粗利率 + PYTD比較）
- 当月YoYランキング（得意先）
- 多次元分析：得意先/商品 × 売上/粗利（ランキング＋ドリル）
- 検索UI（得意先/商品/JAN 部分一致）
- 動的SQL：統一FACT VIEWへ一本化（v_sales_fact_unified 優先、無ければ v_sales_details_norm）
- Drive参照ゼロ（403根絶）

【前提】
- Bounds: salesdb-479915.sales_data.v_sys_bounds
- Role:   salesdb-479915.sales_data.sales_staff_master_native
- Fact:   salesdb-479915.sales_data.v_sales_fact_unified  (無ければ v_sales_details_norm)

【必須カラム（FACT側でこの形に正規化されていること）】
customer_code, customer_name, jan_code, aggregation_code, product_name,
staff_name, sales_date, sales_amount, gross_profit
（email/phone/role/fiscal_year はあれば使う）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype

from google.cloud import bigquery
from google.oauth2 import service_account

# =========================================================
# Config
# =========================================================
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

PROJECT = "salesdb-479915"
DATASET = "sales_data"

VIEW_BOUNDS = f"{PROJECT}.{DATASET}.v_sys_bounds"
TABLE_ROLE  = f"{PROJECT}.{DATASET}.sales_staff_master_native"

# 統一FACT（優先順）
FACT_CANDIDATES = [
    f"{PROJECT}.{DATASET}.v_sales_fact_unified",
    f"{PROJECT}.{DATASET}.v_sales_details_norm",
]

# ノイズJAN（必要なら増やす）
NOISE_JAN_LIST = ["0", "22221", "99998", "33334"]

# =========================================================
# UI helpers
# =========================================================
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v2.3.0｜踏襲復元版（RBAC / ランキング / ドリル / 検索 / Dynamic SQL）")

def money(x: float) -> str:
    try:
        return f"¥{float(x):,.0f}"
    except Exception:
        return "¥0"

def pct(x: float) -> str:
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "0.0%"

def column_config(df: pd.DataFrame):
    cfg = {}
    for c in df.columns:
        if any(k in c for k in ["売上", "粗利", "金額", "差", "GAP"]):
            cfg[c] = st.column_config.NumberColumn(c, format="¥%d")
        elif any(k in c for k in ["率", "比", "%"]):
            cfg[c] = st.column_config.NumberColumn(c, format="%.1f%%")
        elif is_numeric_dtype(df[c]):
            cfg[c] = st.column_config.NumberColumn(c, format="%d")
        else:
            cfg[c] = st.column_config.TextColumn(c)
    return cfg

# =========================================================
# BigQuery client
# =========================================================
@st.cache_resource
def bq_client() -> bigquery.Client:
    if "bigquery" not in st.secrets:
        st.error("❌ secrets.bigquery が未設定です")
        st.stop()

    bq = st.secrets["bigquery"]
    creds = service_account.Credentials.from_service_account_info(
        dict(bq["service_account"]),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(
        project=bq["project_id"],
        credentials=creds,
        location=bq.get("location", DEFAULT_LOCATION),
    )

@st.cache_data(ttl=CACHE_TTL_SEC)
def run_query(sql: str) -> pd.DataFrame:
    try:
        return bq_client().query(sql).to_dataframe()
    except Exception as e:
        st.error(f"Query Failed:\n{e}\n\n---\nSQL:\n{sql}")
        return pd.DataFrame()

# =========================================================
# Resolve Fact View (unified)
# =========================================================
@st.cache_data(ttl=3600)
def resolve_fact_view() -> str:
    # INFORMATION_SCHEMAで存在チェック（asia-northeast1前提）
    # ※ VIEWが無い場合に備え、順に試す
    for v in FACT_CANDIDATES:
        proj, ds, name = v.split(".")
        sql = f"""
        SELECT 1 AS ok
        FROM `{proj}.{ds}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{name}'
        LIMIT 1
        """
        df = run_query(sql)
        if not df.empty:
            return v

    st.error("❌ FACT VIEW が見つかりません（v_sales_fact_unified / v_sales_details_norm のいずれも不在）")
    st.stop()

# =========================================================
# RBAC
# =========================================================
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    staff_name: str
    role_key: str
    is_admin: bool

def load_role(email: str) -> RoleInfo:
    email = email.strip().lower()
    sql = f"""
    SELECT email, staff_name, role
    FROM `{TABLE_ROLE}`
    WHERE LOWER(email) = '{email}'
    LIMIT 1
    """
    df = run_query(sql)
    if df.empty:
        return RoleInfo(login_email=email, staff_name="ゲスト", role_key="SALES", is_admin=False)

    r = df.iloc[0]
    role = str(r.get("role", "SALES")).upper()
    is_admin = any(k in role for k in ["ADMIN", "HQ", "MANAGER"])
    return RoleInfo(
        login_email=email,
        staff_name=str(r.get("staff_name", "不明")),
        role_key=role,
        is_admin=is_admin,
    )

def scope_where(role: RoleInfo, selected_staff: Optional[str]) -> str:
    # 管理者：任意のstaff_nameに絞れる（Noneなら全件）
    # 担当：固定（自分）
    if role.is_admin:
        if selected_staff and selected_staff != "（全員）":
            return f"staff_name = '{selected_staff}'"
        return "1=1"
    return f"staff_name = '{role.staff_name}'"

# =========================================================
# Bounds
# =========================================================
@st.cache_data(ttl=3600)
def load_bounds() -> Dict[str, Any]:
    df = run_query(f"SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1")
    if df.empty:
        st.error("❌ v_sys_bounds が空です")
        st.stop()
    r = df.iloc[0].to_dict()
    # 想定列: current_month, fy_start, py_fy_start, py_current_month, fiscal_year_current, fiscal_year_prev
    return r

# =========================================================
# Dynamic SQL builders
# =========================================================
def noise_jan_where() -> str:
    if not NOISE_JAN_LIST:
        return "1=1"
    in_list = ",".join([f"'{x}'" for x in NOISE_JAN_LIST])
    return f"jan_code NOT IN ({in_list})"

def base_fact_cte(fact_view: str) -> str:
    # ここで“正規形”に寄せる（fact側の実列が多少違っても、norm view で合わせる想定）
    return f"""
    base AS (
      SELECT
        CAST(customer_code AS STRING)      AS customer_code,
        CAST(customer_name AS STRING)      AS customer_name,
        CAST(jan_code AS STRING)           AS jan_code,
        CAST(aggregation_code AS STRING)   AS aggregation_code,
        CAST(product_name AS STRING)       AS product_name,
        CAST(staff_name AS STRING)         AS staff_name,
        DATE(sales_date)                   AS sales_date,
        CAST(sales_amount AS FLOAT64)      AS sales_amount,
        CAST(gross_profit AS FLOAT64)      AS gross_profit
      FROM `{fact_view}`
      WHERE {noise_jan_where()}
    )
    """

def sql_fytd_summary(fact_view: str, where_scope: str) -> str:
    return f"""
    WITH
      b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1),
      {base_fact_cte(fact_view)}
    SELECT
      SUM(CASE WHEN sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)
               THEN sales_amount ELSE 0 END) AS sales_fytd,
      SUM(CASE WHEN sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)
               THEN gross_profit ELSE 0 END) AS gp_fytd,
      SAFE_DIVIDE(
        SUM(CASE WHEN sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)
                 THEN gross_profit ELSE 0 END),
        NULLIF(SUM(CASE WHEN sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)
                 THEN sales_amount ELSE 0 END), 0)
      ) * 100 AS gp_rate_fytd,

      SUM(CASE WHEN sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)
               THEN sales_amount ELSE 0 END) AS sales_pytd,
      SUM(CASE WHEN sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)
               THEN gross_profit ELSE 0 END) AS gp_pytd,
      SAFE_DIVIDE(
        SUM(CASE WHEN sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)
                 THEN gross_profit ELSE 0 END),
        NULLIF(SUM(CASE WHEN sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)
                 THEN sales_amount ELSE 0 END), 0)
      ) * 100 AS gp_rate_pytd
    FROM base
    WHERE {where_scope}
    """

def sql_yoy_customer_current_month(fact_view: str, where_scope: str, limit: int = 100) -> str:
    return f"""
    WITH
      b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1),
      {base_fact_cte(fact_view)}
    SELECT
      customer_name AS 得意先名,
      SUM(CASE WHEN DATE_TRUNC(sales_date, MONTH) = (SELECT current_month FROM b) THEN sales_amount ELSE 0 END) AS 売上_当月,
      SUM(CASE WHEN DATE_TRUNC(sales_date, MONTH) = (SELECT py_current_month FROM b) THEN sales_amount ELSE 0 END) AS 売上_前年同月,
      SUM(CASE WHEN DATE_TRUNC(sales_date, MONTH) = (SELECT current_month FROM b) THEN sales_amount ELSE 0 END)
      - SUM(CASE WHEN DATE_TRUNC(sales_date, MONTH) = (SELECT py_current_month FROM b) THEN sales_amount ELSE 0 END) AS 売上差
    FROM base
    WHERE {where_scope}
    GROUP BY 1
    HAVING 売上_当月 > 0 OR 売上_前年同月 > 0
    ORDER BY 売上差 DESC
    LIMIT {int(limit)}
    """

def sql_rank(dim: str, metric: str, fact_view: str, where_scope: str, period: str, limit: int = 100, keyword: str = "") -> str:
    """
    dim: 'customer' | 'product'
    metric: 'sales' | 'gp'
    period: 'FYTD' | 'CURRENT_MONTH' | 'PYTD' | 'PY_CURRENT_MONTH'
    keyword: 部分一致（customer_name/product_name/jan_code へOR）
    """
    dim_expr = "customer_name" if dim == "customer" else "product_name"
    metric_expr = "sales_amount" if metric == "sales" else "gross_profit"

    period_where = {
        "FYTD": "sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)",
        "PYTD": "sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)",
        "CURRENT_MONTH": "DATE_TRUNC(sales_date, MONTH) = (SELECT current_month FROM b)",
        "PY_CURRENT_MONTH": "DATE_TRUNC(sales_date, MONTH) = (SELECT py_current_month FROM b)",
    }[period]

    kw = keyword.strip()
    kw_where = "1=1"
    if kw:
        kw_esc = kw.replace("'", r"\'")
        kw_where = f"""
        (
          LOWER(customer_name) LIKE LOWER('%{kw_esc}%')
          OR LOWER(product_name) LIKE LOWER('%{kw_esc}%')
          OR CAST(jan_code AS STRING) LIKE '%{kw_esc}%'
        )
        """

    label_dim = "得意先名" if dim == "customer" else "商品名"
    label_metric = "売上" if metric == "sales" else "粗利"

    return f"""
    WITH
      b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1),
      {base_fact_cte(fact_view)}
    SELECT
      {dim_expr} AS {label_dim},
      SUM(CASE WHEN {period_where} THEN {metric_expr} ELSE 0 END) AS {label_metric}
    FROM base
    WHERE {where_scope}
      AND {kw_where}
    GROUP BY 1
    HAVING {label_metric} != 0
    ORDER BY {label_metric} DESC
    LIMIT {int(limit)}
    """

def sql_drill_details(fact_view: str, where_scope: str, mode: str, key_value: str, period: str, limit: int = 500) -> str:
    """
    mode: 'customer' | 'product'
    key_value: 選択された customer_name or product_name
    period: same as above
    """
    key_col = "customer_name" if mode == "customer" else "product_name"
    period_where = {
        "FYTD": "sales_date BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b)",
        "PYTD": "sales_date BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b)",
        "CURRENT_MONTH": "DATE_TRUNC(sales_date, MONTH) = (SELECT current_month FROM b)",
        "PY_CURRENT_MONTH": "DATE_TRUNC(sales_date, MONTH) = (SELECT py_current_month FROM b)",
    }[period]

    v = key_value.replace("'", r"\'")

    return f"""
    WITH
      b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1),
      {base_fact_cte(fact_view)}
    SELECT
      sales_date AS 販売日,
      customer_name AS 得意先名,
      product_name AS 商品名,
      jan_code AS JAN,
      sales_amount AS 売上,
      gross_profit AS 粗利,
      SAFE_DIVIDE(gross_profit, NULLIF(sales_amount, 0)) * 100 AS 粗利率,
      staff_name AS 担当
    FROM base
    WHERE {where_scope}
      AND {key_col} = '{v}'
      AND {period_where}
    ORDER BY 販売日 DESC
    LIMIT {int(limit)}
    """

# =========================================================
# Main
# =========================================================
def main() -> None:
    set_page()

    fact_view = resolve_fact_view()
    bounds = load_bounds()

    # -------- Sidebar (Login / Filters) --------
    st.sidebar.header("入口（RBAC）")
    email = st.sidebar.text_input("ログインEmail").strip()
    if not email:
        st.info("👈 Emailを入力してください（RBAC判定）")
        st.stop()

    role = load_role(email)

    st.sidebar.success(f"{role.staff_name} / {role.role_key}")

    # 管理者だけ staff filter を許可
    selected_staff = None
    if role.is_admin:
        df_staff = run_query(f"SELECT DISTINCT staff_name FROM `{TABLE_ROLE}` WHERE staff_name IS NOT NULL ORDER BY staff_name")
        staff_list = ["（全員）"] + (df_staff["staff_name"].dropna().astype(str).tolist() if not df_staff.empty else [])
        selected_staff = st.sidebar.selectbox("スコープ（担当者）", staff_list, index=0)

    where_scope = scope_where(role, selected_staff)

    # 共通フィルタ
    st.sidebar.header("検索")
    keyword = st.sidebar.text_input("得意先 / 商品 / JAN（部分一致）", value="").strip()

    st.sidebar.header("期間")
    period = st.sidebar.selectbox(
        "期間モード",
        ["FYTD", "CURRENT_MONTH", "PYTD", "PY_CURRENT_MONTH"],
        index=0,
        help="FYTD=当期4月〜当月 / CURRENT_MONTH=当月 / PYTD=前年同期 / PY_CURRENT_MONTH=前年同月",
    )

    # 上部：境界表示（明示）
    with st.expander("sys_bounds（確定値）", expanded=False):
        st.write({
            "current_month": str(bounds.get("current_month")),
            "fy_start": str(bounds.get("fy_start")),
            "py_current_month": str(bounds.get("py_current_month")),
            "py_fy_start": str(bounds.get("py_fy_start")),
            "fiscal_year_current": int(bounds.get("fiscal_year_current")),
            "fiscal_year_prev": int(bounds.get("fiscal_year_prev")),
            "fact_view": fact_view,
        })

    # -------- KPI row --------
    df_kpi = run_query(sql_fytd_summary(fact_view, where_scope))
    if not df_kpi.empty:
        r = df_kpi.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("売上 FYTD", money(r["sales_fytd"]))
        c2.metric("売上 PYTD", money(r["sales_pytd"]))
        c3.metric("粗利 FYTD", money(r["gp_fytd"]))
        c4.metric("粗利 PYTD", money(r["gp_pytd"]))
        c5.metric("粗利率 FYTD", pct(r["gp_rate_fytd"]))
        c6.metric("粗利率 PYTD", pct(r["gp_rate_pytd"]))

    st.divider()

    # -------- Tabs（踏襲：ランキング＋ドリル＋検索） --------
    tab1, tab2, tab3, tab4 = st.tabs([
        "① 当月YoY（得意先）",
        "② ランキング（得意先）",
        "③ ランキング（商品）",
        "④ ドリル（明細）",
    ])

    # ① 当月YoY
    with tab1:
        st.subheader("当月YoYランキング（得意先）")
        df = run_query(sql_yoy_customer_current_month(fact_view, where_scope, limit=200))
        if df.empty:
            st.info("データなし")
        else:
            st.dataframe(df, use_container_width=True, column_config=column_config(df))

    # ② 得意先ランキング
    with tab2:
        st.subheader("得意先ランキング（売上 / 粗利）")
        colA, colB = st.columns(2)

        with colA:
            st.markdown("**売上ランキング**")
            df_sales = run_query(sql_rank("customer", "sales", fact_view, where_scope, period, limit=200, keyword=keyword))
            st.dataframe(df_sales, use_container_width=True, column_config=column_config(df_sales))
        with colB:
            st.markdown("**粗利ランキング**")
            df_gp = run_query(sql_rank("customer", "gp", fact_view, where_scope, period, limit=200, keyword=keyword))
            st.dataframe(df_gp, use_container_width=True, column_config=column_config(df_gp))

    # ③ 商品ランキング
    with tab3:
        st.subheader("商品ランキング（売上 / 粗利）")
        colA, colB = st.columns(2)

        with colA:
            st.markdown("**売上ランキング**")
            df_sales = run_query(sql_rank("product", "sales", fact_view, where_scope, period, limit=200, keyword=keyword))
            st.dataframe(df_sales, use_container_width=True, column_config=column_config(df_sales))
        with colB:
            st.markdown("**粗利ランキング**")
            df_gp = run_query(sql_rank("product", "gp", fact_view, where_scope, period, limit=200, keyword=keyword))
            st.dataframe(df_gp, use_container_width=True, column_config=column_config(df_gp))

    # ④ ドリル（明細）
    with tab4:
        st.subheader("ドリルダウン（明細）")
        mode = st.radio("ドリル軸", ["得意先", "商品"], horizontal=True)
        drill_mode = "customer" if mode == "得意先" else "product"

        # 候補の選択肢：期間＋スコープ＋keyword で上位から選ぶ（重くならない）
        if drill_mode == "customer":
            df_pick = run_query(sql_rank("customer", "sales", fact_view, where_scope, period, limit=200, keyword=keyword))
            key_col = "得意先名"
        else:
            df_pick = run_query(sql_rank("product", "sales", fact_view, where_scope, period, limit=200, keyword=keyword))
            key_col = "商品名"

        if df_pick.empty:
            st.info("選択候補がありません（期間/スコープ/検索条件を見直してください）")
        else:
            options = df_pick[key_col].astype(str).tolist()
            selected = st.selectbox("選択", options, index=0)

            df_det = run_query(sql_drill_details(fact_view, where_scope, drill_mode, selected, period, limit=800))
            if df_det.empty:
                st.info("明細なし")
            else:
                st.dataframe(df_det, use_container_width=True, column_config=column_config(df_det))

                # ダウンロード（踏襲：現場で使う）
                csv = df_det.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "CSVダウンロード",
                    data=csv,
                    file_name=f"drill_{drill_mode}_{period}.csv",
                    mime="text/csv",
                )

    st.caption("SFA OS v2.3.0｜踏襲復元（RBAC/ランキング/ドリル/検索/統一VIEW）")

if __name__ == "__main__":
    main()
