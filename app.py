# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.6 (Full Integration / Annual YoY)
"""

from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import pandas as pd
import streamlit as st
from pandas.api.types import is_numeric_dtype
from google.cloud import bigquery
from google.oauth2 import service_account

# -----------------------------
# 1. Configuration (設定)
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# 本命ビューの定義
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
# ★ 年間ビューに変更済み
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_fy_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_fy_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_fy_named"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

# -----------------------------
# 2. Helpers (表示用)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.6｜判断専用・入口高速版 (Zero-Drop & Drive Scope Secured)")

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config = {}
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

# -----------------------------
# 3. BigQuery Connection & Auth
# -----------------------------
@st.cache_resource
def setup_bigquery_client() -> bigquery.Client:
    bq = st.secrets["bigquery"]
    sa_info = dict(bq["service_account"])
    SCOPES = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT_DEFAULT, credentials=creds, location=DEFAULT_LOCATION)

def query_df_safe(client, sql, params=None, label="", timeout_sec=60) -> pd.DataFrame:
    use_bqstorage = st.session_state.get("use_bqstorage", True)
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", str(v)) for k, v in params.items()
            ]
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

def resolve_role(client, login_email, login_code) -> RoleInfo:
    if not login_email or not login_code: return RoleInfo()
    sql = f"SELECT login_email, role_tier FROM `{VIEW_ROLE_CLEAN}` WHERE login_email = @login_email LIMIT 1"
    df = query_df_safe(client, sql, {"login_email": login_email}, "Auth Check")
    if df.empty: return RoleInfo(login_email=login_email)
    row = df.iloc[0]
    raw_role = str(row['role_tier']).strip().upper()
    is_admin = any(x in raw_role for x in ["ADMIN", "MANAGER", "HQ"])
    return RoleInfo(
        is_authenticated=True,
        login_email=login_email,
        staff_name=login_email.split('@')[0],
        role_key="HQ_ADMIN" if is_admin else "SALES",
        role_admin_view=is_admin,
        phone="-"
    )

def run_scoped_query(client, sql_template, scope_col, login_email, allow_fallback=False):
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    df = query_df_safe(client, sql, {"login_email": login_email}, "Scoped Query")
    if not df.empty: return df
    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        return query_df_safe(client, sql_all, None, "Fallback Query")
    return pd.DataFrame()

# -----------------------------
# 4. UI Sections (各セクション)
# -----------------------------
def render_fytd_org_section(client, login_email):
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")
    if st.button("全社データを読み込む", key="btn_org_load"):
        st.session_state.org_data_loaded = True
    if st.session_state.get('org_data_loaded'):
        sql = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 1"
        df_org = run_scoped_query(client, sql, "viewer_email", login_email, allow_fallback=True)
        if not df_org.empty:
            row = df_org.iloc[0]
            s_cur, s_py, s_fc = get_safe_float(row,'sales_amount_fytd'), get_safe_float(row,'sales_amount_py_total'), get_safe_float(row,'sales_forecast_total')
            gp_cur, gp_py, gp_fc = get_safe_float(row,'gross_profit_fytd'), get_safe_float(row,'gross_profit_py_total'), get_safe_float(row,'gp_forecast_total')
            
            st.markdown("##### ■ 売上")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("① 今期累計", f"¥{s_cur:,.0f}")
            c2.metric("② 昨年度着地", f"¥{s_py:,.0f}")
            c3.metric("③ 今期予測", f"¥{s_fc:,.0f}")
            c4.metric("④ 前年比GAP", f"¥{s_fc - s_py:,.0f}", delta_color="off")
            
            st.markdown("##### ■ 粗利")
            c5, c6, c7, c8 = st.columns(4)
            c5.metric("① 今期累計", f"¥{gp_cur:,.0f}")
            c6.metric("② 昨年度着地", f"¥{gp_py:,.0f}")
            c7.metric("③ 今期予測", f"¥{gp_fc:,.0f}")
            c8.metric("④ 前年比GAP", f"¥{gp_fc - gp_py:,.0f}", delta_color="off")

def render_fytd_me_section(client, login_email):
    st.subheader("👤 年度累計（FYTD）｜個人サマリー")
    if st.button("自分の成績を読み込む", key="btn_me_load"):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(client, sql, "login_email", login_email)
        if not df_me.empty:
            df_disp = df_me.rename(columns={
                "display_name": "担当者名", "sales_amount_fytd": "売上累計", "gross_profit_fytd": "粗利累計",
                "sales_forecast_total": "売上予測", "gp_forecast_total": "粗利予測"
            })
            st.dataframe(df_disp, use_container_width=True, hide_index=True, column_config=create_default_column_config(df_disp))

def render_yoy_section(client, login_email, is_admin):
    st.subheader("📊 年間 YoY ランキング（今年度 vs 昨年度）")
    c1, c2, c3 = st.columns(3)
    def _show_table(title, view_name, key):
        if st.button(title, key=key, use_container_width=True):
            where_clause = "" if is_admin else "WHERE login_email = @login_email"
            params = None if is_admin else {"login_email": login_email}
            sql = f"SELECT login_email, customer_name, sales_amount, gross_profit, sales_diff_yoy FROM `{view_name}` {where_clause} LIMIT 100"
            df = query_df_safe(client, sql, params, title)
            
            if not df.empty:
                df_disp = df.drop(columns=["login_email"], errors="ignore").rename(
                    columns={"customer_name": "得意先名", "sales_amount": "今期売上累計", "gross_profit": "今期粗利", "sales_diff_yoy": "前年比差額"}
                )
                df_disp = df_disp.fillna(0)
                styled_df = df_disp.style.format({
                    "今期売上累計": "¥{:,.0f}",
                    "今期粗利": "¥{:,.0f}",
                    "前年比差額": "¥{:,.0f}"
                })
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
            else:
                st.info("条件に一致するデータがありません。")
                
    with c1: _show_table("📉 下落幅ワースト", VIEW_YOY_BOTTOM, "btn_btm")
    with c2: _show_table("📈 上昇幅ベスト", VIEW_YOY_TOP, "btn_top")
    with c3: _show_table("🆕 新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")

def render_new_deliveries_section(client, login_email, is_admin):
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
            styled_df = df_new.style.format({
                "売上": "¥{:,.0f}",
                "粗利": "¥{:,.0f}"
            })
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.info("新規納品データがありません。")

def render_adoption_alerts_section(client, login_email, is_admin):
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
    if not df_alerts.empty:
        df_alerts['担当者名'] = df_alerts['担当者名'].fillna("未設定")
        col1, col2 = st.columns(2)
        with col1:
            selected_status = st.multiselect(
                "🎯 ステータスで絞り込み", 
                options=df_alerts['ステータス'].unique(),
                default=[s for s in df_alerts['ステータス'].unique() if '🟡' in s or '🔴' in s] 
            )
        with col2:
            all_staffs = sorted(df_alerts['担当者名'].unique().tolist())
            selected_staffs = st.multiselect(
                "👤 担当者で絞り込み", options=all_staffs, default=[] 
            )
        df_display = df_alerts.copy()
        if selected_status:
            df_display = df_display[df_display['ステータス'].isin(selected_status)]
        if selected_staffs:
            df_display = df_display[df_display['担当者名'].isin(selected_staffs)]
        if not df_display.empty:
            num_cols = ["今期売上", "前期売上", "売上差額"]
            for col in num_cols:
                df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0)
            styled_df = df_display.style.format({
                "今期売上": "¥{:,.0f}",
                "前期売上": "¥{:,.0f}",
                "売上差額": "¥{:,.0f}",
                "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else ""
            })
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.info("選択された条件に一致するアイテムはありません。")
    else:
        st.info("現在、アラート対象のアイテムはありません。")

@st.cache_data(ttl=300)
def fetch_cached_customers(_client, login_email, is_admin) -> pd.DataFrame:
    if is_admin:
        sql = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_UNIFIED}` WHERE customer_name IS NOT NULL"
        return query_df_safe(_client, sql, None, "Cached Customers")
    else:
        sql = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_UNIFIED}` WHERE login_email = @login_email AND customer_name IS NOT NULL"
        return query_df_safe(_client, sql, {"login_email": login_email}, "Cached Customers")

def render_customer_drilldown(client, login_email, is_admin):
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")
    df_cust = fetch_cached_customers(client, login_email, is_admin)
    if not df_cust.empty:
        search_term = st.text_input("🔍 得意先名で検索（一部入力）", placeholder="例：古賀")
        filtered_df = df_cust[df_cust['customer_name'].str.contains(search_term, na=False)] if search_term else df_cust
        if not filtered_df.empty:
            opts = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in filtered_df.iterrows()}
            sel = st.selectbox("得意先を選択", options=opts.keys(), format_func=lambda x: opts[x])
            if sel:
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
                    num_cols = ["今期売上", "前期売上"]
                    for col in num_cols:
                        df_adopt[col] = pd.to_numeric(df_adopt[col], errors='coerce').fillna(0)
                    styled_adopt = df_adopt.style.format({
                        "今期売上": "¥{:,.0f}",
                        "前期売上": "¥{:,.0f}",
                        "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else ""
                    })
                    st.dataframe(styled_adopt, use_container_width=True, hide_index=True)
                else:
                    st.info("この得意先の採用データはありません。")
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown("##### 💡 AI 推奨提案商品（Reco）")
                sql_rec = f"SELECT * FROM `{VIEW_RECOMMEND}` WHERE customer_code = @c ORDER BY priority_rank ASC LIMIT 10"
                df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
                if not df_rec.empty:
                    df_disp = df_rec[["priority_rank", "recommend_product", "manufacturer"]].rename(columns={"priority_rank":"順位", "recommend_product":"推奨商品", "manufacturer":"メーカー"})
                    st.dataframe(df_disp, use_container_width=True, hide_index=True)
                else:
                    st.info("現在、この得意先への推奨商品はありません。")
    else:
        st.info("表示できる得意先データがありません。")

# -----------------------------
# 5. Main Loop
# -----------------------------
def main():
    set_page()
    client = setup_bigquery_client()
    
    with st.sidebar:
        st.header("🔑 ログイン")
        login_id = st.sidebar.text_input("ログインID (メールアドレス)")
        login_pw = st.sidebar.text_input("パスコード (携帯下4桁)", type="password")
        st.divider()
        st.session_state.use_bqstorage = st.sidebar.checkbox("高速読込 (Storage API)", value=True)
        if st.sidebar.button("📡 通信ヘルスチェック"):
            try:
                client.query("SELECT 1").result(timeout=10)
                st.sidebar.success("BigQuery 接続正常")
            except Exception as e:
                st.sidebar.error("接続エラー")
        if st.sidebar.button("🧹 キャッシュクリア"): st.cache_data.clear()

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
        render_fytd_org_section(client, role.login_email)
        st.divider()
        render_yoy_section(client, role.login_email, is_admin=True)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=True)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=True)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=True)
    else:
        render_fytd_me_section(client, role.login_email)
        st.divider()
        render_yoy_section(client, role.login_email, is_admin=False)
        st.divider()
        render_new_deliveries_section(client, role.login_email, is_admin=False)
        st.divider()
        render_adoption_alerts_section(client, role.login_email, is_admin=False)
        st.divider()
        render_customer_drilldown(client, role.login_email, is_admin=False)

if __name__ == "__main__":
    main()
