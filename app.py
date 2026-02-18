# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.5.1
(Based on v1.4.7 + Dynamic Filtering & On-the-fly Aggregation)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.api.types import is_numeric_dtype

# -----------------------------
# 1. Configuration
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# ★ここが重要：ランキング用ビューを使わず、すべてこの「Unified（明細）」から計算します
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

# -----------------------------
# 2. Helpers
# -----------------------------
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.5.1｜グループ・得意先絞り込み × 実態解明モード搭載")

def get_safe_float(row: pd.Series, key: str) -> float:
    val = row.get(key)
    return float(val) if not pd.isna(val) else 0.0

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差額", "実績", "予測", "GAP", "ty", "py"]):
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

# -----------------------------
# 3. BigQuery & Auth
# -----------------------------
@st.cache_resource
def setup_bigquery_client() -> bigquery.Client:
    bq = st.secrets["bigquery"]
    sa_info = dict(bq["service_account"])
    scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return bigquery.Client(project=PROJECT_DEFAULT, credentials=creds, location=DEFAULT_LOCATION)

def _normalize_param(value: Any) -> Tuple[str, Optional[Any]]:
    if isinstance(value, tuple) and len(value) == 2: return str(value[0]).upper(), value[1]
    if value is None: return "STRING", None
    if isinstance(value, int): return "INT64", value
    if isinstance(value, float): return "FLOAT64", value
    return "STRING", str(value)

def query_df_safe(client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None, label: str = "") -> pd.DataFrame:
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [bigquery.ScalarQueryParameter(k, *_normalize_param(v)) for k, v in params.items()]
        return client.query(sql, job_config=job_config).to_dataframe()
    except Exception as e:
        # st.error(f"SQL Error ({label}): {e}") # 運用時はコメントアウト推奨
        return pd.DataFrame()

@dataclass(frozen=True)
class RoleInfo:
    is_authenticated: bool = False
    login_email: str = ""
    staff_name: str = "ゲスト"
    role_key: str = "GUEST"
    role_admin_view: bool = False

def resolve_role(client: bigquery.Client, login_email: str, login_code: str) -> RoleInfo:
    if not login_email or not login_code: return RoleInfo()
    # login_code列の有無確認は省略し、ある前提かtry-catchで対応するのが高速だが、ここではv1.4.7準拠
    sql = f"SELECT login_email, role_tier FROM `{VIEW_ROLE_CLEAN}` WHERE login_email = @email"
    # ※ login_codeの実装は環境に合わせて調整してください。今回は簡易化のためemailのみでチェック
    df = query_df_safe(client, sql, {"email": login_email}, "Auth Check")
    if df.empty: return RoleInfo(login_email=login_email)
    row = df.iloc[0]
    is_admin = any(x in str(row["role_tier"]).upper() for x in ["ADMIN", "MANAGER", "HQ"])
    return RoleInfo(True, login_email, login_email.split("@")[0], "HQ_ADMIN" if is_admin else "SALES", is_admin)

# -----------------------------
# 4. Filter Logic (ここが新機能)
# -----------------------------
def render_filters(client: bigquery.Client, role: RoleInfo) -> Tuple[str, Dict[str, Any]]:
    """画面上部にフィルターを表示し、WHERE句を生成する"""
    where_parts = []
    params = {}

    # 1. 権限フィルタ
    if not role.role_admin_view:
        where_parts.append("login_email = @login_email")
        params["login_email"] = role.login_email

    # 2. UIフィルタ
    st.markdown("### 🔍 絞り込み条件")
    with st.expander("営業所・得意先で絞り込む", expanded=True):
        c1, c2 = st.columns(2)
        
        # グループ（営業所）
        group_val = "指定なし"
        if role.role_admin_view:
            # グループ列の存在確認も兼ねて取得
            try:
                sql = f"SELECT DISTINCT sales_group_name FROM `{VIEW_UNIFIED}` WHERE sales_group_name IS NOT NULL ORDER BY 1"
                df_grp = query_df_safe(client, sql)
                if not df_grp.empty:
                    opts = ["指定なし"] + df_grp["sales_group_name"].tolist()
                    group_val = c1.selectbox("営業所 / グループ", opts)
            except:
                c1.warning("グループ情報の取得に失敗しました")
        
        if group_val != "指定なし":
            where_parts.append("sales_group_name = @s_group")
            params["s_group"] = group_val

        # 得意先検索
        cust_input = c2.text_input("得意先名（部分一致）", placeholder="病院名・薬局名を入力")
        if cust_input:
            where_parts.append("customer_name LIKE @cust_name")
            params["cust_name"] = f"%{cust_input}%"

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    return where_clause, params

# -----------------------------
# 5. UI Sections (動的集計版)
# -----------------------------

def render_summary(client, where_clause, params):
    """サマリー：フィルタ条件で動的に集計"""
    sql = f"""
        WITH info AS (
          SELECT CURRENT_DATE('Asia/Tokyo') AS td, DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_td,
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS cfy
        )
        SELECT
          SUM(CASE WHEN fiscal_year = cfy THEN sales_amount ELSE 0 END) AS ty,
          SUM(CASE WHEN fiscal_year = cfy - 1 AND sales_date <= py_td THEN sales_amount ELSE 0 END) AS py_ytd,
          SUM(CASE WHEN fiscal_year = cfy - 1 THEN sales_amount ELSE 0 END) AS py_tot
        FROM `{VIEW_UNIFIED}` CROSS JOIN info {where_clause}
    """
    df = query_df_safe(client, sql, params, "Summary")
    if not df.empty:
        row = df.iloc[0]
        ty, py_ytd, py_tot = get_safe_float(row, "ty"), get_safe_float(row, "py_ytd"), get_safe_float(row, "py_tot")
        fc = ty * (py_tot / py_ytd) if py_ytd > 0 else ty
        
        st.markdown("##### ■ 売上進捗")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("今期累計", f"¥{ty:,.0f}")
        c2.metric("前年同期比", f"¥{py_ytd:,.0f}", delta=f"{int(ty-py_ytd):,.0f}")
        c3.metric("昨年度着地", f"¥{py_tot:,.0f}")
        c4.metric("着地予測", f"¥{fc:,.0f}")
        c5.metric("GAP", f"¥{fc-py_tot:,.0f}", delta=f"{int(fc-py_tot):,.0f}")

def render_ranking(client, where_clause, params):
    """ランキング：明細から動的に集計（YJ=0対策済み）"""
    st.subheader("📊 年間 YoY ランキング")
    
    if "rank_mode" not in st.session_state: st.session_state.rank_mode = "bottom"
    c1, c2 = st.columns(2)
    if c1.button("📉 下落幅ワースト", use_container_width=True): st.session_state.rank_mode = "bottom"
    if c2.button("📈 上昇幅ベスト", use_container_width=True): st.session_state.rank_mode = "top"
    
    sort_dir = "ASC" if st.session_state.rank_mode == "bottom" else "DESC"
    
    # ★ここがポイント：ビューを使わず、明細から直接集計する
    sql = f"""
        WITH base AS (
            SELECT 
                -- YJコードがない場合はJANコードをキーにする（実態解明ロジック）
                COALESCE(NULLIF(CAST(yj_code AS STRING), "0"), CAST(jan_code AS STRING)) AS code,
                MAX(product_name) AS name,
                SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) AS ty,
                SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) AS py
            FROM `{VIEW_UNIFIED}`
            {where_clause}
            GROUP BY 1
        )
        SELECT code AS `コード`, name AS `商品名`, ty AS `今期`, py AS `前期`, (ty - py) AS `差額`
        FROM base
        WHERE py > 0 AND (ty - py) != 0
        ORDER BY `差額` {sort_dir}
        LIMIT 50
    """
    
    df = query_df_safe(client, sql, params, "Ranking")
    if not df.empty:
        st.dataframe(df.style.format({"今期": "¥{:,.0f}", "前期": "¥{:,.0f}", "差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
        
        # ドリルダウン
        st.divider()
        sel_code = st.selectbox("🎯 詳細分析（成分/JANを選択）", options=df["コード"].tolist(), format_func=lambda x: f"{x} : {df[df['コード']==x]['商品名'].values[0]}")
        
        if sel_code:
            p_drill = params.copy()
            p_drill["code"] = sel_code
            
            # JAN内訳
            st.markdown("##### 🧪 要因分析：JANコード（単品）別 内訳")
            sql_jan = f"""
                SELECT jan_code, ANY_VALUE(product_name) as pname,
                SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) as ty,
                SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) as py,
                (SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) - SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END)) as diff
                FROM `{VIEW_UNIFIED}`
                {where_clause} AND COALESCE(NULLIF(CAST(yj_code AS STRING), "0"), CAST(jan_code AS STRING)) = @code
                GROUP BY 1 ORDER BY diff {sort_dir} LIMIT 10
            """
            df_jan = query_df_safe(client, sql_jan, p_drill, "JAN Drill")
            st.dataframe(df_jan.style.format({"ty": "¥{:,.0f}", "py": "¥{:,.0f}", "diff": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

            # 得意先内訳
            st.markdown("##### 🏥 アクション先：得意先別 内訳")
            sql_cust = f"""
                SELECT customer_name AS `得意先`,
                SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) AS `今期`,
                SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) AS `前期`,
                (SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) - SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END)) AS `差額`
                FROM `{VIEW_UNIFIED}`
                {where_clause} AND COALESCE(NULLIF(CAST(yj_code AS STRING), "0"), CAST(jan_code AS STRING)) = @code
                GROUP BY 1 ORDER BY `差額` {sort_dir} LIMIT 30
            """
            df_d = query_df_safe(client, sql_cust, p_drill, "Cust Drill")
            st.dataframe(df_d.style.format({"今期": "¥{:,.0f}", "前期": "¥{:,.0f}", "差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

def render_new_deliveries(client, where_clause, params):
    """新規納品：明細から動的に集計"""
    st.subheader("🎉 新規納品サマリー")
    # ※本来は新規納品ビューも結合が必要だが、今回は簡易的に全社表示またはエラー回避
    try:
        sql = f"""
            WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
            SELECT '① 昨日' AS `期間`, COUNT(DISTINCT customer_code) AS `得意先数`, COUNT(DISTINCT jan_code) AS `品目数`, SUM(sales_amount) AS `売上`
            FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE first_sales_date = DATE_SUB(today, INTERVAL 1 DAY)
            UNION ALL
            SELECT '② 当月', COUNT(DISTINCT customer_code), COUNT(DISTINCT jan_code), SUM(sales_amount)
            FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE DATE_TRUNC(first_sales_date, MONTH) = DATE_TRUNC(today, MONTH)
        """
        df = query_df_safe(client, sql, None, "New Deliv")
        if not df.empty:
            st.dataframe(df.style.format({"売上": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
    except:
        st.empty()

# -----------------------------
# 6. Main
# -----------------------------
def main():
    set_page()
    client = setup_bigquery_client()
    
    with st.sidebar:
        st.header("🔑 Login")
        l_id = st.text_input("Email")
        l_pw = st.text_input("Password", type="password")
        if st.button("Clear Cache"): st.cache_data.clear()

    if not l_id or not l_pw:
        st.info("ログインしてください。")
        return

    role = resolve_role(client, l_id.strip(), l_pw.strip())
    if not role.is_authenticated:
        st.error("ログイン失敗")
        return

    st.success(f"ログイン中: {role.staff_name}")
    
    # フィルタリング
    where_clause, params = render_filters(client, role)
    
    # 各セクション
    render_summary(client, where_clause, params)
    st.divider()
    render_ranking(client, where_clause, params)
    st.divider()
    render_new_deliveries(client, where_clause, params)

if __name__ == "__main__":
    main()
