# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v2.5.0 (SSOT / Scoped RBAC / Restore P0)

【OS準拠ポイント】
1. SSOT: 担当者・得意先・メールを dim_customer_owner_current で物理固定。
2. RBAC: アプリ側フィルタを廃止。BQの Scoped View に viewer_email を渡す設計。
3. 機能復元: 放置されていた「新規採用（Adoption）」をP0機能として最前面に配置。
4. 循環断ち: INFORMATION_SCHEMA 依存や Drive 参照を根絶し、ネイティブ BQ テーブルで完結。
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
# 1. Configuration (OS準拠：物理パス固定)
# =========================================================
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

PROJECT = "salesdb-479915"
DATASET = "sales_data"

# システム基盤View
VIEW_BOUNDS   = f"{PROJECT}.{DATASET}.v_sys_bounds"
TABLE_ROLE    = f"{PROJECT}.{DATASET}.sales_staff_master_native"
OWNER_MASTER  = f"{PROJECT}.{DATASET}.dim_customer_owner_current"

# Scoped Fact Views (閲覧Emailで自動フィルタされるView)
VIEW_FACT_SCOPED     = f"{PROJECT}.{DATASET}.v_sales_fact_scoped"
VIEW_ADOPTION_SCOPED = f"{PROJECT}.{DATASET}.v_new_adoption_unified_scoped"

# ノイズ除去
NOISE_JAN_LIST = ["0", "22221", "99998", "33334"]

# =========================================================
# 2. Helpers (UI & Formatting)
# =========================================================
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v2.5.0｜SSOT準拠・新規採用管理・物理スコープ版")

def money(x: float) -> str:
    try: return f"¥{float(x):,.0f}"
    except: return "¥0"

def pct(x: float) -> str:
    try: return f"{float(x):.1f}%"
    except: return "0.0%"

def get_column_config(df: pd.DataFrame):
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
# 3. BigQuery Connection & Auth Logic
# =========================================================
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    if "bigquery" not in st.secrets:
        st.error("❌ secrets.bigquery が未設定です")
        st.stop()
    bq = st.secrets["bigquery"]
    # 403エラー対策：Cloud Platformスコープを付与
    creds = service_account.Credentials.from_service_account_info(
        dict(bq["service_account"]),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=bq["project_id"], credentials=creds, location=DEFAULT_LOCATION)

@st.cache_data(ttl=CACHE_TTL_SEC)
def run_query(sql: str, params: Optional[List[bigquery.ScalarQueryParameter]] = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    try:
        return client.query(sql, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

@dataclass(frozen=True)
class RoleInfo:
    is_authenticated: bool
    email: str
    staff_name: str
    role: str
    is_admin: bool
    phone_last4: str

def resolve_role(email: str, code: str) -> RoleInfo:
    sql = f"SELECT email, staff_name, role, phone FROM `{TABLE_ROLE}` WHERE LOWER(email) = @email LIMIT 1"
    params = [bigquery.ScalarQueryParameter("email", "STRING", email.lower().strip())]
    df = run_query(sql, params)
    
    if df.empty:
        return RoleInfo(False, email, "ゲスト", "GUEST", False, "")
    
    r = df.iloc[0]
    master_phone = str(r.get("phone", "")).replace("-", "").strip()
    if code != master_phone[-4:]:
        return RoleInfo(False, email, str(r.get("staff_name")), "GUEST", False, "")

    raw_role = str(r.get("role", ""))
    is_admin = any(k in raw_role for k in ["統括", "エリア", "本部", "ADMIN"])
    
    return RoleInfo(True, email, str(r.get("staff_name")), raw_role, is_admin, master_phone[-4:])

# =========================================================
# 4. Data Logic (Scoped View 連携)
# =========================================================
def get_viewer_params(email: str):
    return [bigquery.ScalarQueryParameter("email", "STRING", email)]

def sql_kpi_summary(email: str) -> str:
    return f"""
    WITH b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1)
    SELECT
      SUM(CASE WHEN DATE(sales_date) BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b) THEN sales_amount ELSE 0 END) AS s_fytd,
      SUM(CASE WHEN DATE(sales_date) BETWEEN (SELECT fy_start FROM b) AND (SELECT current_month FROM b) THEN gross_profit ELSE 0 END) AS g_fytd,
      SUM(CASE WHEN DATE(sales_date) BETWEEN (SELECT py_fy_start FROM b) AND (SELECT py_current_month FROM b) THEN sales_amount ELSE 0 END) AS s_pytd
    FROM `{VIEW_FACT_SCOPED}`
    WHERE viewer_email = @email
    """

def sql_rank(email: str, dim: str, metric: str) -> str:
    dim_col = "customer_name" if dim == "customer" else "product_name"
    met_col = "sales_amount" if metric == "sales" else "gross_profit"
    return f"""
    SELECT {dim_col} AS name, SUM({met_col}) AS val
    FROM `{VIEW_FACT_SCOPED}`
    WHERE viewer_email = @email
    GROUP BY 1 HAVING val != 0 ORDER BY val DESC LIMIT 100
    """

# =========================================================
# 5. UI Layout (Restore Tabs & Features)
# =========================================================
def render_header(role: RoleInfo):
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        st.success(f"🔓 ログイン中: {role.staff_name} 様 ({role.role})")
    with c2:
        st.metric("ログインID", role.email)
    with c3:
        st.metric("コード照合", f"****-{role.phone_last4}")

def render_kpi_row(email: str):
    df = run_query(sql_kpi_summary(email), get_viewer_params(email))
    if not df.empty:
        r = df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("売上 FYTD", money(r['s_fytd']), delta=pct((r['s_fytd']/r['s_pytd']-1)*100) if r['s_pytd'] else None)
        c2.metric("粗利 FYTD", money(r['g_fytd']))
        c3.metric("粗利率", pct(r['g_fytd']/r['s_fytd']*100) if r['s_fytd'] else "0%")
        c4.metric("前年同期売上", money(r['s_pytd']))

# =========================================================
# 6. Main Application
# =========================================================
def main():
    set_page()
    
    # --- Sidebar Login (v2.0.0 踏襲) ---
    st.sidebar.header("🔑 OS 認証パネル")
    input_email = st.sidebar.text_input("ログイン Email").strip()
    input_code = st.sidebar.text_input("携帯下4桁コード", type="password").strip()
    
    if not input_email or not input_code:
        st.info("👈 サイドバーからログインしてください（SSOT/RBAC制御）")
        st.sidebar.image(get_qr_code_url(APP_URL), caption="📱 スマホからアクセス", width=150)
        st.stop()
        
    role = resolve_role(input_email, input_code)
    if not role.is_authenticated:
        st.error("❌ 認証に失敗しました。IDまたはコードをご確認ください。")
        st.stop()
    
    # --- 認証成功後のUI ---
    render_header(role)
    render_kpi_row(role.email)
    st.divider()

    # --- Tabs (P0機能の復元) ---
    tabs = st.tabs(["🎯 新規採用・納品管理", "📊 実績ランキング", "📈 YoY分析", "🔎 詳細ドリル"])

    # 【P0】新規採用管理タブ
    with tabs[0]:
        st.subheader("当期 新規採用（Target vs Realized）")
        sql_adopt = f"SELECT * FROM `{VIEW_ADOPTION_SCOPED}` WHERE viewer_email = @email"
        df_adopt = run_query(sql_adopt, get_viewer_params(role.email))
        if df_adopt.empty:
            st.info("表示可能な新規採用データがありません。")
        else:
            # 達成率計算などをフロントで行う
            st.dataframe(df_adopt, use_container_width=True, column_config=get_column_config(df_adopt))
            # 必要ならCSV出力
            st.download_button("採用リスト(CSV)", df_adopt.to_csv(index=False).encode("utf-8-sig"), "new_adoption.csv")

    # 実績ランキングタブ
    with tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 得意先売上ランク")
            df_cr = run_query(sql_rank(role.email, "customer", "sales"), get_viewer_params(role.email))
            st.dataframe(df_cr, use_container_width=True, column_config=get_column_config(df_cr))
        with col2:
            st.markdown("##### 商品売上ランク")
            df_pr = run_query(sql_rank(role.email, "product", "sales"), get_viewer_params(role.email))
            st.dataframe(df_pr, use_container_width=True, column_config=get_column_config(df_pr))

    # YoY分析タブ
    with tabs[2]:
        st.subheader("当月売上 YoY 得意先推移")
        sql_yoy = f"""
        WITH b AS (SELECT * FROM `{VIEW_BOUNDS}` LIMIT 1)
        SELECT customer_name AS 得意先名,
               SUM(CASE WHEN DATE_TRUNC(DATE(sales_date), MONTH) = (SELECT current_month FROM b) THEN sales_amount ELSE 0 END) AS 当月売上,
               SUM(CASE WHEN DATE_TRUNC(DATE(sales_date), MONTH) = (SELECT py_current_month FROM b) THEN sales_amount ELSE 0 END) AS 前年同月,
               SUM(CASE WHEN DATE_TRUNC(DATE(sales_date), MONTH) = (SELECT current_month FROM b) THEN sales_amount ELSE 0 END) -
               SUM(CASE WHEN DATE_TRUNC(DATE(sales_date), MONTH) = (SELECT py_current_month FROM b) THEN sales_amount ELSE 0 END) AS 差分
        FROM `{VIEW_FACT_SCOPED}`
        WHERE viewer_email = @email
        GROUP BY 1 HAVING 当月売上 > 0 OR 前年同月 > 0 ORDER BY 差分 DESC
        """
        df_yoy = run_query(sql_yoy, get_viewer_params(role.email))
        st.dataframe(df_yoy, use_container_width=True, column_config=get_column_config(df_yoy))

    # 詳細ドリルタブ
    with tabs[3]:
        st.subheader("詳細明細検索 (Scoped)")
        kw = st.text_input("得意先名 / 商品名 / JAN で絞り込み検索")
        where_kw = ""
        if kw:
            where_kw = f"AND (customer_name LIKE '%{kw}%' OR product_name LIKE '%{kw}%' OR jan_code LIKE '%{kw}%')"
        
        sql_drill = f"""
        SELECT DATE(sales_date) as 販売日, customer_name, product_name, jan_code, sales_amount, gross_profit
        FROM `{VIEW_FACT_SCOPED}`
        WHERE viewer_email = @email {where_kw}
        ORDER BY sales_date DESC LIMIT 500
        """
        df_drill = run_query(sql_drill, get_viewer_params(role.email))
        st.dataframe(df_drill, use_container_width=True, column_config=get_column_config(df_drill))

    st.caption(f"OS v2.5.0 | {role.staff_name} 担当としてログイン中 | SSOT & Scoped RBAC 有効")

if __name__ == "__main__":
    main()
