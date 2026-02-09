# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.9.3 (Drill-Down Update)

【更新履歴 v1.9.3】
- [New] 商品ランキングからの3段階ドリルダウン分析機能を実装
  (ランキング -> 得意先別増減 -> 得意先別商品構成)
- [Update] 全社状況タブのUIを整理し、分析機能を強化
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype

from google.cloud import bigquery
from google.oauth2 import service_account


# -----------------------------
# 1. Configuration & Constants
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

# ★QRコードの飛び先
APP_URL = "https://sfa-premium-app-2.streamlit.app/"

PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# BigQuery Views (FQN)
VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_dim_staff_role_dedup"
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_WORST_RANK = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_product_yoy_worst_ranking"
VIEW_BEST_RANK = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_product_yoy_best_ranking"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"
# ★Drill-down用 (Raw Fact View)
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"


# -----------------------------
# 2. Display Mappings
# -----------------------------
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
    "sales_forecast_total": "売上着地予測（年）",
    "pacing_rate": "売上対前年ペース",
    "sales_amount_py_total": "前年売上実績（年）",
    "gp_forecast_total": "粗利着地予測（年）",
    "gp_pacing_rate": "粗利対前年ペース",
    "gross_profit_py_total": "前年粗利実績（年）",
}

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
# 3. Helper Functions
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.9.3 (Drill-Down Update)｜戦略提案｜3段階ドリルダウン分析｜着地予測")

def get_qr_code_url(url: str) -> str:
    return f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={url}"

def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)

def append_total_row(df: pd.DataFrame, label_col: str = None) -> pd.DataFrame:
    if df.empty: return df
    num_cols = df.select_dtypes(include=['number']).columns
    total_data = {}
    for col in df.columns:
        if col in num_cols:
            if any(k in col for k in ["率", "比", "ペース", "rate", "pace"]):
                total_data[col] = None
            else:
                total_data[col] = df[col].sum()
        else:
            total_data[col] = ""
    target_label = label_col if label_col and label_col in df.columns else df.columns[0]
    total_data[target_label] = "=== 合計 ==="
    return pd.concat([df, pd.DataFrame([total_data])], ignore_index=True)

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差", "実績", "予測", "GAP", "amount", "profit", "diff", "cur", "prev"]):
            config[col] = st.column_config.NumberColumn(col, format="¥%d")
        elif any(k in col for k in ["率", "比", "ペース", "rate", "pace"]):
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
    return float(val)


# -----------------------------
# 4. Role & Auth
# -----------------------------
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    role_key: str = "SALES"
    role_admin_view: bool = False
    role_admin_edit: bool = False
    role_sales_view: bool = True
    area_name: str = "未設定"

def normalize_role_key(role_key: str) -> str:
    rk = (role_key or "").strip().upper()
    if rk in ("HQ_ADMIN", "AREA_MANAGER", "SALES"):
        return rk
    return "SALES"

def _secrets_has_bigquery() -> bool:
    if "bigquery" not in st.secrets: return False
    bq = st.secrets.get("bigquery", {})
    return bool(bq.get("project_id")) and bool(bq.get("service_account"))

def _get_bq_from_secrets() -> Tuple[str, str, Dict[str, Any]]:
    bq = st.secrets["bigquery"]
    return str(bq.get("project_id")), str(bq.get("location") or DEFAULT_LOCATION), dict(bq.get("service_account"))

def setup_bigquery_client() -> Tuple[bigquery.Client, str, str, str]:
    if not _secrets_has_bigquery():
        st.error("❌ Secrets設定が見つかりません。")
        st.stop()
    project_id, location, sa = _get_bq_from_secrets()
    sa_json = json.dumps(sa)
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    return client, project_id, location, sa_json


# -----------------------------
# 5. Query Execution
# -----------------------------
def _build_query_parameters(params: Optional[Dict[str, Any]]) -> List[bigquery.ScalarQueryParameter]:
    qparams = []
    if not params: return qparams
    for k, v in params.items():
        if isinstance(v, bool): qparams.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
        elif isinstance(v, int): qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        elif isinstance(v, float): qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
        elif v is None: qparams.append(bigquery.ScalarQueryParameter(k, "STRING", ""))
        else: qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return qparams

@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SEC)
def cached_query_df(project_id: str, location: str, sa_json: str, sql: str, params_json: str, use_bqstorage: bool, timeout_sec: int) -> pd.DataFrame:
    sa = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    params = json.loads(params_json) if params_json else {}
    job_config = bigquery.QueryJobConfig()
    qparams = _build_query_parameters(params)
    if qparams: job_config.query_parameters = qparams
    job = client.query(sql, job_config=job_config)
    job.result(timeout=timeout_sec)
    return job.to_dataframe(create_bqstorage_client=use_bqstorage)

def query_df_safe(client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None, label: str = "", use_bqstorage: bool = True, timeout_sec: int = 60, cache_key: Optional[Tuple[str, str, str]] = None) -> pd.DataFrame:
    params_json = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)
    try:
        if cache_key:
            project_id, location, sa_json = cache_key
            return cached_query_df(project_id, location, sa_json, sql, params_json, use_bqstorage, timeout_sec)
        else:
            job_config = bigquery.QueryJobConfig()
            qparams = _build_query_parameters(params or {})
            if qparams: job_config.query_parameters = qparams
            job = client.query(sql, job_config=job_config)
            job.result(timeout=timeout_sec)
            return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except Exception as e:
        st.error(f"Query Failed: {label}\n{e}")
        return pd.DataFrame()

def resolve_role(client, cache_key, login_email, opts) -> RoleInfo:
    sql = f"SELECT login_email, role_tier, role_admin_view, area_name FROM `{VIEW_ROLE}` WHERE login_email = @login_email LIMIT 1"
    df = query_df_safe(client, sql, {"login_email": login_email}, "Role Check", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    if df.empty: return RoleInfo(login_email=login_email)
    r = df.iloc[0]
    return RoleInfo(login_email=login_email, role_key=normalize_role_key(str(r.get("role_tier"))), role_admin_view=bool(r.get("role_admin_view")), area_name=str(r.get("area_name", "未設定")))

def run_scoped_query(client, cache_key, sql_template, scope_col, login_email, opts, allow_fallback=False):
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    if opts["show_sql"]: st.code(sql, language="sql")
    df = query_df_safe(client, sql, {"login_email": login_email}, "Scoped Query", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    if not df.empty: return df
    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        if opts["show_sql"]: st.code(sql_all, language="sql")
        return query_df_safe(client, sql_all, None, "Fallback Query", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    return pd.DataFrame()

# --- Drill-Down Queries ---
def get_product_drilldown(client, cache_key, product_name: str, is_worst: bool, opts) -> pd.DataFrame:
    """商品 -> 得意先別増減内訳"""
    sort_order = "ASC" if is_worst else "DESC"
    sql = f"""
        SELECT 
            customer_name AS `得意先名`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) as `今年売上`,
            SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as `前年売上`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) - 
            SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as `売上差額`,
            SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) as `今年粗利`,
            SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) - 
            SUM(CASE WHEN fiscal_year = 2024 THEN gross_profit ELSE 0 END) as `粗利差額`
        FROM `{VIEW_UNIFIED}`
        WHERE product_name = @product_name
        GROUP BY 1
        ORDER BY `売上差額` {sort_order}
        LIMIT 500
    """
    return query_df_safe(client, sql, {"product_name": product_name}, "Drill: Product->Cust", opts["use_bqstorage"], opts["timeout_sec"], cache_key)

def get_customer_portfolio(client, cache_key, customer_name: str, opts) -> pd.DataFrame:
    """得意先 -> 商品構成"""
    sql = f"""
        SELECT 
            product_name AS `商品名`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) as `今年売上`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) - 
            SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as `売上差額`,
            SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) as `今年粗利`
        FROM `{VIEW_UNIFIED}`
        WHERE customer_name = @customer_name
        GROUP BY 1
        ORDER BY `今年売上` DESC
        LIMIT 500
    """
    return query_df_safe(client, sql, {"customer_name": customer_name}, "Drill: Cust->Portfolio", opts["use_bqstorage"], opts["timeout_sec"], cache_key)


# -----------------------------
# 6. Sidebar
# -----------------------------
def sidebar_controls() -> Dict[str, Any]:
    qr_url = get_qr_code_url(APP_URL)
    st.sidebar.image(qr_url, caption="📱スマホでアクセス", width=150)
    st.sidebar.divider()
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
    if not login_email: st.stop()
    return login_email


# -----------------------------
# 7. Render Functions
# -----------------------------

def render_interactive_ranking_flow(client, cache_key, ranking_type: str, opts):
    """3段階ドリルダウン機能付きランキング表示 (Worst/Best共通)"""
    is_worst = (ranking_type == "worst")
    view_name = VIEW_WORST_RANK if is_worst else VIEW_BEST_RANK
    
    # Session State keys for this specific tab
    key_prefix = f"drill_{ranking_type}"
    
    # 1. ランキングデータの取得 (SQL Viewでノイズ除去・ソート済み)
    sql_rank = f"SELECT * FROM `{view_name}` LIMIT 1000"
    df_rank = query_df_safe(client, sql_rank, None, f"Ranking {ranking_type}", opts["use_bqstorage"], opts["timeout_sec"], cache_key)

    if df_rank.empty:
        st.info("データがありません。")
        return

    # 表示用カラム調整
    df_disp = df_rank.rename(columns={
        "product_name": "商品名",
        "sales_cur": "今年売上",
        "sales_prev": "前年売上",
        "sales_diff": "売上差額",
        "gp_cur": "今年粗利",
        "gp_diff": "粗利差額"
    })
    
    # カラム順序
    cols = ["商品名", "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
    
    st.markdown("##### ① 商品を選択してください (クリックで詳細分析)")
    st.caption("※送料・値引等の管理コードは自動的に除外されています。")

    # Selection API
    event = st.dataframe(
        df_disp[cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "商品名": st.column_config.TextColumn("商品名", width="medium"),
            "売上差額": st.column_config.NumberColumn("売上差額", format="¥%d"),
            "今年売上": st.column_config.NumberColumn("今年売上", format="¥%d"),
            "前年売上": st.column_config.NumberColumn("前年売上", format="¥%d"),
            "粗利差額": st.column_config.NumberColumn("粗利差額", format="¥%d"),
            "今年粗利": st.column_config.NumberColumn("今年粗利", format="¥%d"),
        },
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        key=f"{key_prefix}_table_1"
    )

    # Level 2: Product Selected -> Show Customer Breakdown
    if len(event.selection["rows"]) > 0:
        idx = event.selection["rows"][0]
        selected_product = df_disp.iloc[idx]["商品名"]
        
        st.divider()
        st.subheader(f"🔎 詳細分析: {selected_product}")
        st.info(f"この商品は、どの得意先で数字が変動したのか？ ({'減少' if is_worst else '増加'}要因)")

        df_cust = get_product_drilldown(client, cache_key, selected_product, is_worst, opts)
        
        if df_cust.empty:
            st.warning("詳細データが見つかりませんでした。")
        else:
            event_cust = st.dataframe(
                df_cust,
                use_container_width=True,
                hide_index=True,
                column_config=create_default_column_config(df_cust),
                on_select="rerun",
                selection_mode="single-row",
                key=f"{key_prefix}_table_2"
            )

            # Level 3: Customer Selected -> Show Portfolio
            if len(event_cust.selection["rows"]) > 0:
                c_idx = event_cust.selection["rows"][0]
                selected_customer = df_cust.iloc[c_idx]["得意先名"]
                
                st.divider()
                st.subheader(f"🏥 得意先分析: {selected_customer}")
                st.success(f"{selected_customer} の現在の購入商品一覧")
                
                df_portfolio = get_customer_portfolio(client, cache_key, selected_customer, opts)
                st.dataframe(
                    df_portfolio,
                    use_container_width=True,
                    hide_index=True,
                    column_config=create_default_column_config(df_portfolio),
                    key=f"{key_prefix}_table_3"
                )

def render_fytd_org_section(client, cache_key, login_email, opts):
    st.subheader("🏢 年度累計（FYTD）｜全社")
    
    if st.button("全社データを読み込む", key="btn_org_load", use_container_width=True):
        st.session_state.org_data_loaded = True
    
    if st.session_state.org_data_loaded:
        # KPI Cards
        sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
        df_org = run_scoped_query(client, cache_key, sql_kpi, "viewer_email", login_email, opts, allow_fallback=True)
        
        if not df_org.empty:
            row = df_org.iloc[0]
            s_cur = get_safe_float(row, 'sales_amount_fytd')
            s_py = get_safe_float(row, 'sales_amount_py_total')
            s_fc = get_safe_float(row, 'sales_forecast_total')
            gp_cur = get_safe_float(row, 'gross_profit_fytd')
            gp_py = get_safe_float(row, 'gross_profit_py_total')
            gp_fc = get_safe_float(row, 'gp_forecast_total')

            st.markdown("##### ■ 売上 (Sales)")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("① 現状 (FYTD)", f"¥{s_cur:,.0f}")
            c2.metric("② 昨年度実績", f"¥{s_py:,.0f}")
            c3.metric("③ 着地予測", f"¥{s_fc:,.0f}", delta_color="normal")
            c4.metric("④ GAP", f"¥{s_fc - s_py:,.0f}", delta=None, delta_color="off")

            st.markdown("##### ■ 粗利 (Gross Profit)")
            c5, c6, c7, c8 = st.columns(4)
            c5.metric("① 現状 (FYTD)", f"¥{gp_cur:,.0f}")
            c6.metric("② 昨年度実績", f"¥{gp_py:,.0f}")
            c7.metric("③ 着地予測", f"¥{gp_fc:,.0f}", delta_color="normal")
            c8.metric("④ GAP", f"¥{gp_fc - gp_py:,.0f}", delta=None, delta_color="off")
            
            st.divider()

        # Interactive Ranking Tabs
        st.subheader("📊 増減要因分析 (ドリルダウン)")
        tab_worst, tab_best = st.tabs(["📉 ワースト (売上減)", "📈 ベスト (売上増)"])
        
        with tab_worst:
            render_interactive_ranking_flow(client, cache_key, "worst", opts)
        
        with tab_best:
            render_interactive_ranking_flow(client, cache_key, "best", opts)

    else:
        st.info("👆 上のボタンを押して全社データを読み込んでください")

def render_fytd_me_section(client, cache_key, login_email, opts):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(client, cache_key, sql, "login_email", login_email, opts)
        if df_me.empty:
            st.warning("データがありません。")
            return

        row = df_me.iloc[0]
        s_cur = get_safe_float(row, 'sales_amount_fytd')
        s_fc = get_safe_float(row, 'sales_forecast_total')
        s_py = get_safe_float(row, 'sales_amount_py_total')
        gp_cur = get_safe_float(row, 'gross_profit_fytd')
        gp_fc = get_safe_float(row, 'gp_forecast_total')
        gp_py = get_safe_float(row, 'gross_profit_py_total')

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
        
        df_disp = rename_columns_for_display(df_me, JP_COLS_FYTD)
        cols = list(df_disp.columns)
        if "ログインメール" in cols: cols.remove("ログインメール")
        if "閲覧者メール" in cols: cols.remove("閲覧者メール")
        if "担当者名" in cols:
            cols.remove("担当者名")
            cols.insert(0, "担当者名")
        
        col_cfg = create_default_column_config(df_disp[cols])
        st.dataframe(df_disp[cols], use_container_width=True, hide_index=True, column_config=col_cfg)

def render_yoy_section(client, cache_key, login_email, allow_fallback, opts):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)
    
    def _show_table(title, view_name, key):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(client, cache_key, sql, "login_email", login_email, opts, allow_fallback)
            if not df.empty:
                df_disp = rename_columns_for_display(df, JP_COLS_YOY)
                cols = list(df_disp.columns)
                if "ログインメール" in cols: cols.remove("ログインメール")
                if "担当者名" in cols:
                    cols.remove("担当者名")
                    cols.insert(0, "担当者名")
                
                df_final = append_total_row(df_disp[cols], label_col="担当者名")
                col_cfg = create_default_column_config(df_final)
                st.dataframe(df_final, use_container_width=True, hide_index=True, column_config=col_cfg)
            else:
                st.info("0件です。")

    with c1: _show_table("YoY Top (伸び)", VIEW_YOY_TOP, "btn_top")
    with c2: _show_table("YoY Bottom (落ち)", VIEW_YOY_BOTTOM, "btn_btm")
    with c3: _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")

def render_customer_drilldown(client, cache_key, login_email, opts):
    st.subheader("🎯 得意先別・戦略提案（AI Gap Analysis）")
    
    sql_cust = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_FACT_DAILY}` WHERE login_email = @login_email ORDER BY customer_code"
    df_cust = query_df_safe(client, sql_cust, {"login_email": login_email}, "Cust List", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    
    if df_cust.empty:
        st.info("担当得意先データがありません（またはログインメール不一致）。")
        return

    cust_options = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in df_cust.iterrows()}
    selected_code = st.selectbox("分析する得意先を選択してください", options=cust_options.keys(), format_func=lambda x: cust_options[x])
    if not selected_code: return

    st.divider()
    sql_rec = f"SELECT * FROM `{VIEW_RECOMMEND}` WHERE customer_code = @cust_code ORDER BY priority_rank ASC"
    df_rec = query_df_safe(client, sql_rec, {"cust_code": selected_code}, "Recommendation", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
    
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
            disp_df = df_rec[["priority_rank", "recommend_product", "manufacturer", "market_scale"]].rename(columns={"priority_rank": "優先順位", "recommend_product": "推奨商品名", "manufacturer": "メーカー", "market_scale": "全社売上規模"})
            col_cfg = create_default_column_config(disp_df)
            st.dataframe(disp_df, use_container_width=True, hide_index=True, column_config=col_cfg)
            
    with st.expander("参考: 現在の採用品リストを見る"):
        sql_adopted = f"""
        SELECT 
            m.product_name, 
            SUM(t.sales_amount) as sales_fytd,
            SUM(t.gross_profit) as gp_fytd
        FROM `{VIEW_FACT_DAILY}` t
        LEFT JOIN `{PROJECT_DEFAULT}.{DATASET_DEFAULT}.vw_item_master_norm` m 
            ON CAST(t.jan AS STRING) = CAST(m.jan_code AS STRING)
        WHERE t.customer_code = @cust_code AND t.fiscal_year = 2025
        GROUP BY 1 ORDER BY 2 DESC LIMIT 100
        """
        df_adopted = query_df_safe(client, sql_adopted, {"cust_code": selected_code}, "Adopted List", opts["use_bqstorage"], opts["timeout_sec"], cache_key)
        
        renamed_df = df_adopted.rename(columns={"product_name": "商品名", "sales_fytd": "売上(FYTD)", "gp_fytd": "粗利(FYTD)"})
        col_cfg = create_default_column_config(renamed_df)
        st.dataframe(renamed_df, use_container_width=True, column_config=col_cfg)


# -----------------------------
# 8. Main
# -----------------------------
def main():
    if 'org_data_loaded' not in st.session_state: st.session_state.org_data_loaded = False

    set_page()
    
    client, project_id, location, sa_json = setup_bigquery_client()
    cache_key = (project_id, location, sa_json)
    
    opts = sidebar_controls()
    login_email = get_login_email_ui()
    st.divider()

    role = resolve_role(client, cache_key, login_email, opts)
    st.write(f"**Login:** {role.login_email} / **Role:** {role.role_key} ({role.area_name})")
    is_admin = role.role_key in ("HQ_ADMIN", "AREA_MANAGER")
    st.divider()
    
    if is_admin:
        t1, t2, t3 = st.tabs(["🏢 全社状況", "👤 エリア/個人", "🎯 戦略提案(Beta)"])
        with t1: render_fytd_org_section(client, cache_key, login_email, opts)
        with t2:
            render_fytd_me_section(client, cache_key, login_email, opts)
            st.divider()
            render_yoy_section(client, cache_key, login_email, is_admin, opts)
        with t3: render_customer_drilldown(client, cache_key, login_email, opts)
    else:
        t1, t2, t3 = st.tabs(["👤 今年の成績", "📊 得意先分析", "🎯 提案を作る"])
        with t1: render_fytd_me_section(client, cache_key, login_email, opts)
        with t2: render_yoy_section(client, cache_key, login_email, is_admin, opts)
        with t3: render_customer_drilldown(client, cache_key, login_email, opts)

    st.caption("Updated: v1.9.3 (Drill-Down Update)")

if __name__ == "__main__":
    main()
