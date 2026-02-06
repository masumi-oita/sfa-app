# app.py
# -*- coding: utf-8 -*-
"""
SFA｜入口高速版（判断専用） - OS v1.6.0

【システム構成定義】
- Backend: Google BigQuery (asia-northeast1)
- Frontend: Streamlit
- Logic:
    1. Role Separation: HQ_ADMIN (全社) vs SALES (個人)
    2. Forecasting: Pacing Method (Sales & Gross Profit)
    3. Analysis: Worst Impact Ranking (Direct Raw Access / JAN Strict)
    4. Recommendation: Gap Analysis (JAN Based) ★New!

【参照VIEW一覧】
- v_dim_staff_role_dedup: 権限管理
- v_admin_org_fytd_summary_scoped: 全社KPI・着地予測
- v_admin_product_yoy_worst_ranking: 全社減少要因分析
- v_staff_fytd_summary_scoped: 個人KPI
- v_sales_customer_yoy_*: 得意先別ランキング
- v_sales_fact_login_jan_daily: 日次ファクト（ドリルダウン用）
- v_sales_recommendation_engine: 戦略提案エンジン ★New!
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


# -----------------------------
# Configuration & Constants
# -----------------------------
APP_TITLE = "SFA｜入口高速版（判断専用）"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# BigQuery Views (FQN)
VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_dim_staff_role_dedup"

# Admin / Org Views
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_WORST_RANK = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_product_yoy_worst_ranking"

# Staff / Personal Views
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"

# ★ Recommendation & Fact Views
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"


# -----------------------------
# Display Mappings (Japanese)
# -----------------------------
# KPI Card Columns
JP_COLS_FYTD = {
    "viewer_email": "閲覧者メール",
    "login_email": "ログインメール",
    "display_name": "担当者名",
    "role_tier": "ロール",
    "area_name": "エリア",
    "current_month": "基準日",
    "fy_start": "年度開始",
    "sales_amount_fytd": "売上（FYTD）",
    "gross_profit_fytd": "粗利（FYTD）",
    "gross_profit_rate_fytd": "粗利率（FYTD）",
    "sales_amount_py_fytd": "売上（前年FYTD）",
    "gross_profit_py_fytd": "粗利（前年FYTD）",
    "sales_diff_fytd": "前年差（売上）",
    "gp_diff_fytd": "前年差（粗利）",
    # Forecasts
    "sales_forecast_total": "売上着地予測（年）",
    "pacing_rate": "売上対前年ペース",
    "sales_amount_py_total": "前年売上実績（年）",
    "gp_forecast_total": "粗利着地予測（年）",
    "gp_pacing_rate": "粗利対前年ペース",
    "gross_profit_py_total": "前年粗利実績（年）",
}

# Ranking Columns (Worst Analysis)
JP_COLS_RANK = {
    "jan": "JANコード",
    "product_name": "商品名",
    "maker_name": "メーカー",
    "sales_cur": "売上(今年)",
    "sales_prev": "売上(前年)",
    "sales_diff": "売上差額(Impact)",
    "sales_rate": "前年比",
    "gp_cur": "粗利(今年)",
    "gp_diff": "粗利差額(Impact)"
}

# Customer Ranking Columns
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
}


# -----------------------------
# Utility Functions
# -----------------------------
def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)


# -----------------------------
# Role Management
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
# BigQuery Client & Auth
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
        raise ValueError("JSON format invalid.")
    for k in ["type", "project_id", "private_key", "client_email"]:
        if k not in obj:
            raise ValueError(f"Service Account JSON missing key: {k}")
    return obj


def ensure_credentials_ui() -> Tuple[str, str, Dict[str, Any]]:
    st.sidebar.header("接続設定")

    if _secrets_has_bigquery():
        project_id, location, sa = _get_bq_from_secrets()
        st.sidebar.success("Secrets: OK")
        return project_id, location, sa

    st.sidebar.warning("Secrets 未設定。JSON貼り付けモードで動作します。")

    project_id = st.sidebar.text_input("project_id (Temporary)", value=PROJECT_DEFAULT)
    location = st.sidebar.text_input("location (Temporary)", value=DEFAULT_LOCATION)
    sa_text = st.sidebar.text_area("Service Account JSON", height=100)

    if not sa_text.strip():
        st.info("SA JSONを入力してください。")
        st.stop()

    try:
        sa = _parse_service_account_json(sa_text.strip())
    except Exception as e:
        st.error(f"JSON Parse Error: {e}")
        st.stop()

    sa["project_id"] = project_id.strip() or sa.get("project_id")
    return str(project_id), str(location), sa


@st.cache_resource(show_spinner=False)
def get_bq_client(project_id: str, location: str, sa: Dict[str, Any]) -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)


# -----------------------------
# Query Execution Helpers
# -----------------------------
def _build_query_parameters(params: Optional[Dict[str, Any]]) -> List[bigquery.ScalarQueryParameter]:
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
        elif v is None:
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", ""))
        else:
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return qparams


def _show_bq_error_context(title: str, sql: str, exc: Exception):
    st.error(f"Query Failed: {title}")
    st.write(f"Exception: {exc}")
    # st.code(sql, language="sql") # Debug


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SEC)
def cached_query_df(
    project_id: str, location: str, sa_json: str, sql: str, params_json: str,
    use_bqstorage: bool, timeout_sec: int
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
    client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None,
    label: str = "", use_bqstorage: bool = True, timeout_sec: int = 60,
    cache_key: Optional[Tuple[str, str, str]] = None
) -> pd.DataFrame:
    params_json = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)
    try:
        if cache_key:
            project_id, location, sa_json = cache_key
            return cached_query_df(
                project_id, location, sa_json, sql, params_json, use_bqstorage, timeout_sec
            )
        else:
            job_config = bigquery.QueryJobConfig()
            qparams = _build_query_parameters(params or {})
            if qparams:
                job_config.query_parameters = qparams
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
            return job.to_dataframe(create_bqstorage_client=use_bqstorage)

    except (BadRequest, GoogleAPICallError, Exception) as e:
        _show_bq_error_context(label, sql, e)
        return pd.DataFrame()


# -----------------------------
# Component: User Interface
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.6.0｜戦略提案｜ワースト分析｜着地予測")

def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.header("System Settings")
    use_bqstorage = st.sidebar.toggle("Use Storage API (Fast)", value=True)
    timeout_sec = st.sidebar.slider("Query Timeout (sec)", 10, 300, 60, 10)
    show_sql = st.sidebar.toggle("Show SQL (Debug)", value=False)
    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.sidebar.success("Cache Cleared.")
    return {"use_bqstorage": use_bqstorage, "timeout_sec": timeout_sec, "show_sql": show_sql}

def get_login_email_ui() -> str:
    st.sidebar.header("Login Simulation")
    default_email = st.secrets.get("default_login_email", "") if "default_login_email" in st.secrets else ""
    login_email = st.sidebar.text_input("Login Email", value=default_email).strip()
    if not login_email:
        st.info("Please enter login email.")
        st.stop()
    return login_email

def resolve_role(client, cache_key, login_email, opts) -> RoleInfo:
    sql = f"""
    SELECT login_email, role_tier, role_admin_view, area_name
    FROM `{VIEW_ROLE}` WHERE login_email = @login_email LIMIT 1
    """
    df = query_df_safe(client, sql, {"login_email": login_email}, "Role Check",
                       opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    if df.empty:
        return RoleInfo(login_email=login_email)
    
    r = df.iloc[0]
    return RoleInfo(
        login_email=login_email,
        role_key=normalize_role_key(str(r.get("role_tier"))),
        role_admin_view=bool(r.get("role_admin_view")),
        area_name=str(r.get("area_name", "未設定"))
    )

def run_scoped_query(client, cache_key, sql_template, scope_col, login_email, opts, allow_fallback=False):
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    if opts["show_sql"]: st.code(sql, language="sql")
    df = query_df_safe(client, sql, {"login_email": login_email}, "Scoped Query",
                       opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    if not df.empty: return df

    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        if opts["show_sql"]: st.code(sql_all, language="sql")
        df_all = query_df_safe(client, sql_all, None, "Fallback Query",
                               opts["use_bqstorage"], opts["timeout_sec"], cache_key)
        return df_all
        
    return pd.DataFrame()


# -----------------------------
# Component: Render Sections
# -----------------------------

def render_fytd_org_section(client, cache_key, login_email, opts):
    st.subheader("🏢 年度累計（FYTD）｜全社")
    
    if st.button("全社データを読み込む", key="btn_org", use_container_width=True):
        # KPI Card
        sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
        df_org = run_scoped_query(client, cache_key, sql_kpi, "viewer_email", login_email, opts, allow_fallback=True)
        
        if not df_org.empty:
            row = df_org.iloc[0]
            
            st.markdown("##### ■ 売上予測")
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("売上 着地予測（年）", f"¥{float(row.get('sales_forecast_total', 0)):,.0f}")
            with c2: 
                pace = float(row.get('pacing_rate', 0))
                st.metric("対前年ペース", f"{pace*100:.1f}%", f"{(pace-1.0)*100:+.1f}%")
            with c3: st.metric("昨年度実績（年）", f"¥{float(row.get('sales_amount_py_total', 0)):,.0f}")

            st.markdown("##### ■ 粗利予測")
            c4, c5, c6 = st.columns(3)
            with c4: st.metric("粗利 着地予測（年）", f"¥{float(row.get('gp_forecast_total', 0)):,.0f}")
            with c5:
                pace_gp = float(row.get('gp_pacing_rate', 0))
                st.metric("対前年ペース", f"{pace_gp*100:.1f}%", f"{(pace_gp-1.0)*100:+.1f}%")
            with c6: st.metric("昨年度実績（年）", f"¥{float(row.get('gross_profit_py_total', 0)):,.0f}")
            
            st.divider()

        # Worst Ranking
        st.subheader("📉 売上減少要因（ワースト商品ランキング）")
        st.caption("前年同期と比較して、売上減少額（Impact）が大きい商品トップ50")
        
        sql_rank = f"SELECT * FROM `{VIEW_WORST_RANK}` LIMIT 50"
        df_rank = query_df_safe(client, sql_rank, None, "Worst Ranking",
                                opts["use_bqstorage"], opts["timeout_sec"], cache_key)
        
        if df_rank.empty:
            st.info("減少商品データはありません。")
        else:
            df_disp = rename_columns_for_display(df_rank, JP_COLS_RANK)
            st.dataframe(
                df_disp,
                column_config={
                    "売上差額(Impact)": st.column_config.NumberColumn(format="¥%d"),
                    "売上(今年)": st.column_config.NumberColumn(format="¥%d"),
                    "売上(前年)": st.column_config.NumberColumn(format="¥%d"),
                    "前年比": st.column_config.NumberColumn(format="%.1f%%"),
                },
                use_container_width=True,
                height=400
            )

def render_fytd_me_section(client, cache_key, login_email, opts):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(client, cache_key, sql, "login_email", login_email, opts)
        
        if df_me.empty:
            st.warning("データがありません。")
            return

        row = df_me.iloc[0]
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("着地予測（年）", f"¥{float(row.get('sales_forecast_total', 0)):,.0f}")
        with c2: 
            pace = float(row.get('pacing_rate', 0))
            st.metric("対前年ペース", f"{pace*100:.1f}%", f"{(pace-1.0)*100:+.1f}%")
        with c3: st.metric("前年実績（年）", f"¥{float(row.get('sales_amount_py_total', 0)):,.0f}")
        
        st.divider()
        st.dataframe(rename_columns_for_display(df_me, JP_COLS_FYTD), use_container_width=True)

def render_yoy_section(client, cache_key, login_email, allow_fallback, opts):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)
    
    def _show_table(title, view_name, key):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(client, cache_key, sql, "login_email", login_email, opts, allow_fallback)
            if not df.empty:
                st.dataframe(rename_columns_for_display(df, JP_COLS_YOY), use_container_width=True)
            else:
                st.info("0件です。")

    with c1: _show_table("YoY Top (伸び)", VIEW_YOY_TOP, "btn_top")
    with c2: _show_table("YoY Bottom (落ち)", VIEW_YOY_BOTTOM, "btn_btm")
    with c3: _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")

def render_customer_drilldown(client, cache_key, login_email, opts):
    """
    v1.6.0 New Feature: Customer Gap Analysis & Recommendation (JAN Based)
    """
    st.subheader("🎯 得意先別・戦略提案（AI Gap Analysis）")
    
    # 1. Get Customer List from Fact (Login scoped)
    sql_cust = f"""
    SELECT DISTINCT customer_code, customer_name
    FROM `{VIEW_FACT_DAILY}`
    WHERE login_email = @login_email
    ORDER BY customer_code
    """
    df_cust = query_df_safe(client, sql_cust, {"login_email": login_email}, "Cust List", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    
    if df_cust.empty:
        st.info("担当得意先データがありません（またはログインメール不一致）。")
        return

    # 2. Select Customer
    cust_options = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in df_cust.iterrows()}
    selected_code = st.selectbox("分析する得意先を選択してください", options=cust_options.keys(), format_func=lambda x: cust_options[x])
    
    if not selected_code:
        return

    st.divider()
    
    # 3. Get Recommendation (Query VIEW_RECOMMEND by Customer)
    # Note: Logic relies on JAN, but we display refined info
    sql_rec = f"""
    SELECT * FROM `{VIEW_RECOMMEND}`
    WHERE customer_code = @cust_code
    ORDER BY priority_rank ASC
    """
    df_rec = query_df_safe(client, sql_rec, {"cust_code": selected_code}, "Recommendation", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    
    # 4. Display Logic
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.markdown("#### 🏥 得意先プロファイル")
        if not df_rec.empty:
            strong_cat = df_rec.iloc[0].get("strong_category", "-")
            st.info(f"この得意先の主力領域(メーカー): **{strong_cat}**")
            st.caption("※購入実績シェアNo.1のメーカー")
        else:
            st.warning("プロファイルデータ不足（または主要品完納済み）")
            strong_cat = "(不明)"

    with c2:
        st.markdown("#### 💡 AI提案リスト（未採用のチャンス商品）")
        st.caption(f"全社の **{strong_cat}** 売上TOP10のうち、**未採用**の商品")
        
        if df_rec.empty:
            st.success("🎉 この領域の主要商品はすべて採用済みです。")
        else:
            # Display: Hide JAN, Show Product Name & Scale
            disp_df = df_rec[[
                "priority_rank", "recommend_product", "manufacturer", "market_scale"
            ]].rename(columns={
                "priority_rank": "優先順位",
                "recommend_product": "推奨商品名",
                "manufacturer": "メーカー",
                "market_scale": "全社売上規模"
            })
            
            st.dataframe(
                disp_df,
                column_config={
                    "全社売上規模": st.column_config.NumberColumn(format="¥%d")
                },
                use_container_width=True,
                hide_index=True
            )
            
    # 5. Reference: Adopted List (Raw Fact)
    with st.expander("参考: 現在の採用品リストを見る"):
        sql_adopted = f"""
        SELECT 
            m.product_name, 
            SUM(t.sales_amount) as sales_fytd
        FROM `{VIEW_FACT_DAILY}` t
        LEFT JOIN `{PROJECT_DEFAULT}.{DATASET_DEFAULT}.vw_item_master_norm` m 
            ON CAST(t.jan AS STRING) = CAST(m.jan_code AS STRING)
        WHERE t.customer_code = @cust_code
          AND t.fiscal_year = 2025
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 100
        """
        df_adopted = query_df_safe(client, sql_adopted, {"cust_code": selected_code}, "Adopted List", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
        st.dataframe(df_adopted, use_container_width=True)


# -----------------------------
# Main Execution
# -----------------------------
def main():
    set_page()
    
    # 1. Connection
    project_id, location, sa = ensure_credentials_ui()
    sa_json = json.dumps(sa)
    cache_key = (project_id, location, sa_json)
    client = get_bq_client(project_id, location, sa)
    
    # 2. Controls
    opts = sidebar_controls()
    login_email = get_login_email_ui()
    
    st.divider()

    # 3. Role Check
    role = resolve_role(client, cache_key, login_email, opts)
    st.write(f"**Login:** {role.login_email} / **Role:** {role.role_key} ({role.area_name})")
    
    is_admin = role.role_key in ("HQ_ADMIN", "AREA_MANAGER")
    
    st.divider()
    
    # 4. Routing with New Tabs
    if is_admin:
        t1, t2, t3 = st.tabs(["🏢 全社状況", "👤 エリア/個人", "🎯 戦略提案(Beta)"])
        with t1: render_fytd_org_section(client, cache_key, login_email, opts)
        with t2:
            render_fytd_me_section(client, cache_key, login_email, opts)
            st.divider()
            render_yoy_section(client, cache_key, login_email, is_admin, opts)
        with t3:
            render_customer_drilldown(client, cache_key, login_email, opts)

    else:
        # Sales Role: New Tab Structure
        t1, t2, t3 = st.tabs(["👤 今年の成績", "📊 得意先分析", "🎯 提案を作る"])
        with t1: render_fytd_me_section(client, cache_key, login_email, opts)
        with t2: render_yoy_section(client, cache_key, login_email, is_admin, opts)
        with t3: render_customer_drilldown(client, cache_key, login_email, opts)

    st.caption("Updated: v1.6.0 Strategy & Recommendation (Strict JAN)")

if __name__ == "__main__":
    main()
