# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v2.0.0 (Secure Auth / High Performance)

【更新履歴 v2.0.0】
- [Auth] ログインID(Email)とログインコード(携帯下4桁)による認証を実装。
- [Master] 担当者台帳(sales_staff_master)に基づく動的権限管理。
- [UX] ログイン後の担当者情報表示（名前・権限・電話番号）を強化。
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
# 1. Configuration
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

APP_URL = "https://sfa-premium-app-2.streamlit.app/"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# 分析の土台となるView
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"

# 担当者マスタ（スプレッドシート連携テーブル）
VIEW_ROLE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.sales_staff_master"

NOISE_JAN_SQL = "('0', '22221', '99998', '33334')"


# -----------------------------
# 2. Helpers (Display)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v2.0.0 (Secure Auth)｜BigQuery集計・動的SQL版")

def get_qr_code_url(url: str) -> str:
    return f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={url}"

def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty: return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config = {}
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
    if pd.isna(val): return 0.0
    return float(val)

JP_COLS_FYTD = {
    "login_email": "ログインメール", "display_name": "担当者名",
    "sales_amount_fytd": "売上（FYTD）", "gross_profit_fytd": "粗利（FYTD）",
    "sales_amount_py_total": "前年売上実績（年）", "sales_forecast_total": "売上着地予測（年）",
    "gross_profit_py_total": "前年粗利実績（年）", "gp_forecast_total": "粗利着地予測（年）"
}
JP_COLS_YOY = {
    "customer_code": "得意先コード", "customer_name": "得意先名",
    "sales_amount": "売上（当月）", "gross_profit": "粗利（当月）",
    "sales_amount_py": "売上（前年同月）", "sales_diff_yoy": "前年差（売上）"
}


# -----------------------------
# 3. BigQuery Connection & Auth Logic
# -----------------------------
def setup_bigquery_client() -> Tuple[bigquery.Client, str, str, str]:
    if "bigquery" not in st.secrets:
        st.error("❌ Secrets設定が見つかりません。")
        st.stop()
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    creds = service_account.Credentials.from_service_account_info(sa)
    client = bigquery.Client(project=project_id, credentials=creds, location=location)
    return client, project_id, location, json.dumps(sa)

def query_df_safe(client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None, label: str = "", use_bqstorage: bool = True, timeout_sec: int = 60) -> pd.DataFrame:
    try:
        job_config = bigquery.QueryJobConfig()
        qparams = []
        if params:
            for k, v in params.items():
                if isinstance(v, int): qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
                elif isinstance(v, float): qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
                else: qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
        if qparams: job_config.query_parameters = qparams
        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except Exception as e:
        st.error(f"Query Failed: {label}\n{e}")
        return pd.DataFrame()

@dataclass(frozen=True)
class RoleInfo:
    is_authenticated: bool = False
    login_email: str = ""
    staff_name: str = "ゲスト"
    role_key: str = "GUEST"
    role_admin_view: bool = False
    phone: str = "-"
    area_name: str = "未設定"

def resolve_role(client, login_email, login_code) -> RoleInfo:
    """
    ID(Email)とコード(携帯番号下4桁)で認証
    """
    if not login_email or not login_code:
        return RoleInfo()

    sql = f"SELECT email, staff_name, role, phone FROM `{VIEW_ROLE}` WHERE email = @login_email LIMIT 1"
    df = query_df_safe(client, sql, {"login_email": login_email}, "Auth Check")
    
    if df.empty:
        return RoleInfo(login_email=login_email)
    
    r = df.iloc[0]
    master_phone = str(r.get("phone", "")).replace("-", "").strip()
    # 携帯番号の末尾4桁を取得
    last_4_digits = master_phone[-4:]
    
    # 入力コードと末尾4桁が一致するか判定
    if login_code == last_4_digits:
        raw_role = str(r.get("role", "")).strip().upper()
        is_admin = any(x in raw_role for x in ["ADMIN", "MANAGER", "HQ"])
        rk = "HQ_ADMIN" if is_admin else "SALES"
        
        return RoleInfo(
            is_authenticated=True,
            login_email=login_email,
            staff_name=str(r.get("staff_name", "不明")),
            role_key=rk,
            role_admin_view=is_admin,
            phone=str(r.get("phone", "-")),
            area_name=raw_role
        )
    else:
        return RoleInfo(is_authenticated=False, login_email=login_email)

def run_scoped_query(client, sql_template, scope_col, login_email, allow_fallback=False):
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    df = query_df_safe(client, sql, {"login_email": login_email}, "Scoped Query")
    if not df.empty: return df
    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        return query_df_safe(client, sql_all, None, "Fallback Query")
    return pd.DataFrame()


# -----------------------------
# 4. Calculation Logic (v1.9.8踏襲)
# -----------------------------
def fetch_ranking_from_bq(client, ranking_type: str, axis_mode: str, is_sales_mode: bool) -> pd.DataFrame:
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
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN {target_val} ELSE 0 END) - 
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN {target_val} ELSE 0 END) AS diff_val,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END) - 
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_diff,
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END) - 
        SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN gross_profit ELSE 0 END) AS gp_diff
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
    return query_df_safe(client, sql, None, "Ranking Query")

def fetch_drilldown_from_bq(client, key_col: str, key_val: str, target_col: str, is_worst: bool, is_sales_mode: bool) -> pd.DataFrame:
    order_dir = "ASC" if is_worst else "DESC"
    sort_col_alias = "売上差額" if is_sales_mode else "粗利差額"
    target_label = "得意先名" if target_col == "customer_name" else "商品名"

    sql = f"""
        SELECT 
            {target_col} AS `{target_label}`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) as `今年売上`,
            SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as `前年売上`,
            SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) - 
            SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as `売上差額`,
            SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) as `今年粗利`,
            SUM(CASE WHEN fiscal_year = 2025 THEN gross_profit ELSE 0 END) - 
            SUM(CASE WHEN fiscal_year = 2024 THEN gross_profit ELSE 0 END) as `粗利差額`
        FROM `{VIEW_UNIFIED}`
        WHERE {key_col} = @key_val
        GROUP BY 1
        ORDER BY `{sort_col_alias}` {order_dir}
        LIMIT 500
    """
    return query_df_safe(client, sql, {"key_val": key_val}, "Drilldown Query")


# -----------------------------
# 5. UI Sections (v1.9.8踏襲)
# -----------------------------
def sidebar_login_panel() -> Tuple[str, str]:
    st.sidebar.header("🔑 ログイン")
    login_id = st.sidebar.text_input("ログインID (メールアドレス)", placeholder="example@company.com")
    login_pw = st.sidebar.text_input("ログインコード (携帯下4桁)", type="password", help="登録携帯番号の末尾4桁")
    return login_id.strip(), login_pw.strip()

def render_interactive_ranking_matrix(client, ranking_type: str, axis_mode: str, is_sales_mode: bool):
    is_worst = (ranking_type == "worst")
    is_product = (axis_mode == "product")
    label_col = "商品名" if is_product else "得意先名"
    mode_label = "売上" if is_sales_mode else "粗利"
    df_rank = fetch_ranking_from_bq(client, ranking_type, axis_mode, is_sales_mode)
    if df_rank.empty:
        st.info("データがありません。")
        return
    df_disp = df_rank.rename(columns={"name": label_col, "sales_cur": "今年売上", "sales_prev": "前年売上", "sales_diff": "売上差額", "gp_cur": "今年粗利", "gp_diff": "粗利差額"})
    cols = [label_col, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"] if is_sales_mode else [label_col, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]
    st.markdown(f"##### ① {label_col}を選択 ({mode_label}ベース)")
    key_suffix = f"{ranking_type}_{axis_mode}_{mode_label}"
    event = st.dataframe(df_disp[cols], use_container_width=True, hide_index=True, column_config=create_default_column_config(df_disp), height=400, on_select="rerun", selection_mode="single-row", key=f"t1_{key_suffix}")
    if len(event.selection["rows"]) > 0:
        idx = event.selection["rows"][0]
        selected_val = df_disp.iloc[idx][label_col]
        st.divider()
        st.subheader(f"🔎 内訳分析: {selected_val}")
        key_col = "product_name" if is_product else "customer_name"
        target_col = "customer_name" if is_product else "product_name"
        df_drill = fetch_drilldown_from_bq(client, key_col, selected_val, target_col, is_worst, is_sales_mode)
        if not df_drill.empty:
            drill_label = "得意先名" if is_product else "商品名"
            d_cols = [drill_label, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"] if is_sales_mode else [drill_label, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]
            st.dataframe(df_drill[d_cols], use_container_width=True, hide_index=True, column_config=create_default_column_config(df_drill), key=f"t2_{key_suffix}")

def render_fytd_org_section(client, login_email):
    st.subheader("🏢 年度累計（FYTD）｜全社")
    if st.button("全社データを読み込む", key="btn_org_load", use_container_width=True):
        st.session_state.org_data_loaded = True
    if st.session_state.get('org_data_loaded'):
        sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
        df_org = run_scoped_query(client, sql_kpi, "viewer_email", login_email, allow_fallback=True)
        if not df_org.empty:
            row = df_org.iloc[0]
            s_cur, s_py, s_fc = get_safe_float(row,'sales_amount_fytd'), get_safe_float(row,'sales_amount_py_total'), get_safe_float(row,'sales_forecast_total')
            gp_cur, gp_py, gp_fc = get_safe_float(row,'gross_profit_fytd'), get_safe_float(row,'gross_profit_py_total'), get_safe_float(row,'gp_forecast_total')
            st.markdown("##### ■ 売上 (Sales)")
            c1, c2, c3, c4 = st.columns(4); c1.metric("① 現状", f"¥{s_cur:,.0f}"); c2.metric("② 昨年", f"¥{s_py:,.0f}"); c3.metric("③ 予測", f"¥{s_fc:,.0f}"); c4.metric("④ GAP", f"¥{s_fc - s_py:,.0f}", delta_color="off")
            st.markdown("##### ■ 粗利 (Gross Profit)")
            c5, c6, c7, c8 = st.columns(4); c5.metric("① 現状", f"¥{gp_cur:,.0f}"); c6.metric("② 昨年", f"¥{gp_py:,.0f}"); c7.metric("③ 予測", f"¥{gp_fc:,.0f}"); c8.metric("④ GAP", f"¥{gp_fc - gp_py:,.0f}", delta_color="off")
            st.divider()
        st.subheader("📊 増減要因分析")
        c_axis, c_val = st.columns(2)
        with c_axis: axis_sel = st.radio("集計軸:", ["📦 商品軸", "🏥 得意先軸"], horizontal=True); axis_mode = "product" if "商品" in axis_sel else "customer"
        with c_val: val_sel = st.radio("評価指標:", ["💰 売上金額", "💹 粗利金額"], horizontal=True); is_sales_mode = "売上" in val_sel
        t_w, t_b = st.tabs(["📉 ワースト", "📈 ベスト"])
        with t_w: render_interactive_ranking_matrix(client, "worst", axis_mode, is_sales_mode)
        with t_b: render_interactive_ranking_matrix(client, "best", axis_mode, is_sales_mode)

def render_fytd_me_section(client, login_email):
    st.subheader("👤 年度累計（FYTD）｜自分")
    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(client, sql, "login_email", login_email)
        if not df_me.empty:
            df_disp = rename_columns_for_display(df_me, JP_COLS_FYTD)
            cols = list(df_disp.columns)
            if "担当者名" in cols: cols.remove("担当者名"); cols.insert(0, "担当者名")
            st.dataframe(df_disp[cols], use_container_width=True, hide_index=True, column_config=create_default_column_config(df_disp[cols]))

def render_yoy_section(client, login_email, allow_fallback):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)
    def _show_table(title, view_name, key):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(client, sql, "login_email", login_email, allow_fallback)
            if not df.empty:
                st.dataframe(rename_columns_for_display(df, JP_COLS_YOY), use_container_width=True, hide_index=True)
    with c1: _show_table("YoY Top", VIEW_YOY_TOP, "btn_top")
    with c2: _show_table("YoY Bottom", VIEW_YOY_BOTTOM, "btn_btm")
    with c3: _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")

def render_customer_drilldown(client, login_email):
    st.subheader("🎯 得意先別・戦略提案")
    sql_cust = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_FACT_DAILY}` WHERE login_email = @login_email ORDER BY customer_code"
    df_cust = query_df_safe(client, sql_cust, {"login_email": login_email}, "Cust List")
    if not df_cust.empty:
        opts = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in df_cust.iterrows()}
        sel = st.selectbox("得意先を選択", options=opts.keys(), format_func=lambda x: opts[x])
        if sel:
            st.divider()
            sql_rec = f"SELECT * FROM `{VIEW_RECOMMEND}` WHERE customer_code = @c ORDER BY priority_rank ASC"
            df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
            c1, c2 = st.columns([1, 2])
            with c1: st.info(f"主力領域: **{df_rec.iloc[0].get('strong_category', '-')}**" if not df_rec.empty else "-")
            with c2:
                if not df_rec.empty:
                    st.dataframe(df_rec[["priority_rank", "recommend_product", "manufacturer"]].rename(columns={"priority_rank":"順", "recommend_product":"商品", "manufacturer":"メーカー"}), use_container_width=True, hide_index=True)


# -----------------------------
# 6. Main (Auth Integration)
# -----------------------------
def main():
    set_page()
    client, _, _, _ = setup_bigquery_client()
    
    # サイドバーログイン
    login_id, login_pw = sidebar_login_panel()
    st.sidebar.divider()
    if st.sidebar.button("Clear Cache"): st.cache_data.clear(); st.sidebar.success("Cleared.")
    
    if not login_id or not login_pw:
        st.info("👈 サイドバーからログインしてください。")
        st.stop()
    
    # 認証と権限の解決
    role = resolve_role(client, login_id, login_pw)
    
    if not role.is_authenticated:
        st.error("❌ ログインIDまたはコードが正しくありません。")
        st.stop()
    
    # ログイン成功後の表示
    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    
    c1, c2, c3 = st.columns(3)
    c1.metric("👤 担当", role.staff_name)
    c2.metric("🛡️ 権限", role.role_key)
    c3.metric("📞 電話", role.phone)
    st.divider()
    
    is_admin = role.role_admin_view
    if is_admin:
        tabs = st.tabs(["🏢 全社状況", "👤 エリア/個人", "🎯 戦略提案"])
        with tabs[0]: render_fytd_org_section(client, role.login_email)
        with tabs[1]:
            render_fytd_me_section(client, role.login_email)
            st.divider()
            render_yoy_section(client, role.login_email, True)
        with tabs[2]: render_customer_drilldown(client, role.login_email)
    else:
        tabs = st.tabs(["👤 今年の成績", "📊 得意先分析", "🎯 提案を作る"])
        with tabs[0]: render_fytd_me_section(client, role.login_email)
        with tabs[1]: render_yoy_section(client, role.login_email, False)
        with tabs[2]: render_customer_drilldown(client, role.login_email)

    st.caption("Updated: v2.0.0 (Secure Auth Integration)")

if __name__ == "__main__":
    main()
