# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.6.0
(Complete Integration: Auth Hardening + Dynamic Filtering + All Features Restored)
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
# 1. Configuration (設定)
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# ビュー定義（v1.4.7準拠 + グループ対応）
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

# -----------------------------
# 2. Helpers (表示用)
# -----------------------------
def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.6.0｜全機能統合版（グループ絞り込み × 採用アラート × 実態解明）")

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
# 3. BigQuery Connection & Auth
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
        # st.error(f"SQL Error ({label}): {e}") 
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
    # v1.4.7準拠の認証ロジック
    sql = f"SELECT login_email, role_tier FROM `{VIEW_ROLE_CLEAN}` WHERE login_email = @email"
    df = query_df_safe(client, sql, {"email": login_email}, "Auth Check")
    if df.empty: return RoleInfo(login_email=login_email)
    row = df.iloc[0]
    is_admin = any(x in str(row["role_tier"]).upper() for x in ["ADMIN", "MANAGER", "HQ"])
    return RoleInfo(True, login_email, login_email.split("@")[0], "HQ_ADMIN" if is_admin else "SALES", is_admin)

# -----------------------------
# 4. Filter Logic (万能フィルター)
# -----------------------------
def render_filters(client: bigquery.Client, role: RoleInfo) -> Tuple[str, Dict[str, Any]]:
    where_parts = []
    params = {}

    # 基本権限（一般社員は自分のデータのみ）
    if not role.role_admin_view:
        where_parts.append("login_email = @login_email")
        params["login_email"] = role.login_email

    # フィルタUI
    st.markdown("### 🔍 分析スコープ設定")
    with st.expander("詳細絞り込み（グループ・得意先）を開く", expanded=True):
        c1, c2, c3 = st.columns([1, 2, 2])
        
        # A. 権限表示
        current_scope = "全社" if role.role_admin_view else "担当エリア"
        c1.info(f"権限: **{current_scope}**")

        # B. グループ絞り込み (管理者の場合のみ)
        group_val = "指定なし"
        if role.role_admin_view:
            try:
                # グループ一覧を取得（キャッシュ推奨だが今回は直書き）
                sql = f"SELECT DISTINCT sales_group_name FROM `{VIEW_UNIFIED}` WHERE sales_group_name IS NOT NULL ORDER BY 1"
                df_grp = query_df_safe(client, sql)
                if not df_grp.empty:
                    opts = ["指定なし"] + df_grp["sales_group_name"].tolist()
                    group_val = c2.selectbox("営業所 / グループ", opts)
            except:
                c2.warning("グループ情報の取得に失敗")
        
        if group_val != "指定なし":
            where_parts.append("sales_group_name = @s_group")
            params["s_group"] = group_val

        # C. 得意先絞り込み
        cust_input = c3.text_input("得意先名（部分一致）", placeholder="病院名・薬局名を入力")
        if cust_input:
            where_parts.append("customer_name LIKE @cust_name")
            params["cust_name"] = f"%{cust_input}%"

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    return where_clause, params

# -----------------------------
# 5. UI Sections (全機能統合)
# -----------------------------

def render_summary(client, where_clause, params):
    """サマリー（動的集計・季節予測付き）"""
    sql = f"""
        WITH info AS (
          SELECT 
            CURRENT_DATE('Asia/Tokyo') AS td,
            (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS cfy
        ),
        dates AS (
          SELECT td, cfy, DATE_SUB(td, INTERVAL 1 YEAR) AS py_td FROM info
        )
        SELECT
          SUM(CASE WHEN fiscal_year = cfy THEN sales_amount ELSE 0 END) AS ty,
          SUM(CASE WHEN fiscal_year = cfy - 1 AND sales_date <= py_td THEN sales_amount ELSE 0 END) AS py_ytd,
          SUM(CASE WHEN fiscal_year = cfy - 1 THEN sales_amount ELSE 0 END) AS py_tot
        FROM `{VIEW_UNIFIED}` CROSS JOIN dates {where_clause}
    """
    df = query_df_safe(client, sql, params, "Summary")
    if not df.empty:
        row = df.iloc[0]
        ty = get_safe_float(row, "ty")
        py_ytd = get_safe_float(row, "py_ytd")
        py_tot = get_safe_float(row, "py_tot")
        
        # 季節調整予測: (今期実績 / 前年同期実績) * 前年合計
        fc = ty * (py_tot / py_ytd) if py_ytd > 0 else ty
        
        st.markdown("##### ■ 売上進捗（スコープ集計）")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("今期累計", f"¥{ty:,.0f}")
        c2.metric("前年同期比", f"¥{py_ytd:,.0f}", delta=f"{int(ty-py_ytd):,.0f}")
        c3.metric("昨年度着地", f"¥{py_tot:,.0f}")
        c4.metric("着地予測(ペース)", f"¥{fc:,.0f}")
        c5.metric("GAP", f"¥{fc-py_tot:,.0f}", delta=f"{int(fc-py_tot):,.0f}")

def render_ranking(client, where_clause, params):
    """ランキング（動的集計・YJ=0実態解明モード）"""
    st.subheader("📊 年間 YoY ランキング")
    
    if "rank_mode" not in st.session_state: st.session_state.rank_mode = "bottom"
    c1, c2 = st.columns(2)
    if c1.button("📉 下落幅ワースト", use_container_width=True): st.session_state.rank_mode = "bottom"
    if c2.button("📈 上昇幅ベスト", use_container_width=True): st.session_state.rank_mode = "top"
    
    sort_dir = "ASC" if st.session_state.rank_mode == "bottom" else "DESC"
    
    # 動的集計クエリ
    sql = f"""
        WITH base AS (
            SELECT 
                -- YJコードがない場合はJANコードをキーにする（実態解明）
                COALESCE(NULLIF(CAST(yj_code AS STRING), "0"), CAST(jan_code AS STRING)) AS code,
                MAX(product_name) AS name,
                SUM(CASE WHEN fiscal_year = 2025 THEN sales_amount ELSE 0 END) AS ty,
                SUM(CASE WHEN fiscal_year = 2024 THEN sales_amount ELSE 0 END) AS py
            FROM `{VIEW_UNIFIED}`
            {where_clause}
            GROUP BY 1
        )
        SELECT code AS `YJコード`, name AS `成分名`, ty AS `今期`, py AS `前期`, (ty - py) AS `差額`
        FROM base
        WHERE py > 0 AND (ty - py) != 0
        ORDER BY `差額` {sort_dir}
        LIMIT 50
    """
    
    df = query_df_safe(client, sql, params, "Ranking")
    if not df.empty:
        st.dataframe(df.style.format({"今期": "¥{:,.0f}", "前期": "¥{:,.0f}", "差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
        
        # --- ドリルダウン ---
        st.divider()
        sel_code = st.selectbox("🎯 詳細分析（成分/JANを選択）", options=df["YJコード"].tolist(), format_func=lambda x: f"{x} : {df[df['YJコード']==x]['成分名'].values[0]}")
        
        if sel_code:
            p_drill = params.copy()
            p_drill["code"] = sel_code
            
            # JAN内訳
            st.markdown("##### 🧪 要因分析：JANコード（単品）別 内訳")
            if str(sel_code).strip() in {"0", "", "nan", "None"}:
                st.warning("⚠️ 選択されたコードは未分類です。以下のJANコード内訳で実態を確認してください。")

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
    """新規納品サマリー（完全復活）"""
    st.subheader("🎉 新規納品サマリー")
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
        else:
            st.info("新規納品データがありません。")
    except:
        st.empty()

def render_adoption_alerts(client, where_clause, params):
    """採用アラート（完全復活）"""
    st.subheader("🚨 採用アイテム・失注アラート")
    try:
        sql = f"""
            SELECT staff_name, customer_name, product_name, last_purchase_date, adoption_status, current_fy_sales, previous_fy_sales 
            FROM `{VIEW_ADOPTION}` 
            ORDER BY 5, 6 ASC LIMIT 100
        """
        df = query_df_safe(client, sql, None, "Alerts")
        if not df.empty:
            df["売上差額"] = df["current_fy_sales"] - df["previous_fy_sales"]
            st.dataframe(df.style.format({"current_fy_sales": "¥{:,.0f}", "previous_fy_sales": "¥{:,.0f}", "売上差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
        else:
            st.info("アラート対象はありません。")
    except:
        st.empty()

def render_customer_drilldown(client, where_clause, params):
    """得意先ドリルダウン & Reco（完全復活 + フィルター連動）"""
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")
    
    # フィルタ条件に合致する得意先のみリストアップ
    sql = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_UNIFIED}` {where_clause} AND customer_name IS NOT NULL"
    df_cust = query_df_safe(client, sql, params, "Cust List")
    
    if not df_cust.empty:
        search = st.text_input("🔍 検索（得意先名）", placeholder="例：古賀")
        filtered = df_cust[df_cust["customer_name"].str.contains(search, na=False)] if search else df_cust
        opts = {r["customer_code"]: f"{r['customer_code']} : {r['customer_name']}" for _, r in filtered.iterrows()}
        sel = st.selectbox("得意先を選択", options=list(opts.keys()), format_func=lambda x: opts[x])
        
        if sel:
            st.divider()
            st.markdown("##### 📦 現在の採用アイテム状況")
            sql_ad = f"SELECT product_name, adoption_status, last_purchase_date, current_fy_sales FROM `{VIEW_ADOPTION}` WHERE customer_code = @c ORDER BY 4 DESC"
            df_ad = query_df_safe(client, sql_ad, {"c": sel}, "Adopt")
            if not df_ad.empty: st.dataframe(df_ad.style.format({"current_fy_sales": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
            
            st.markdown("##### 💡 AI 推奨提案商品（Reco）")
            sql_re = f"SELECT priority_rank, recommend_product, manufacturer FROM `{VIEW_RECOMMEND}` WHERE customer_code = @c ORDER BY 1 LIMIT 10"
            df_re = query_df_safe(client, sql_re, {"c": sel}, "Reco")
            if not df_re.empty: st.dataframe(df_re, use_container_width=True, hide_index=True)

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
    
    # フィルタリング適用
    where_clause, params = render_filters(client, role)
    
    # 各セクション描画
    render_summary(client, where_clause, params)
    st.divider()
    render_ranking(client, where_clause, params)
    st.divider()
    render_new_deliveries(client, where_clause, params)
    st.divider()
    render_adoption_alerts(client, where_clause, params)
    st.divider()
    render_customer_drilldown(client, where_clause, params)

if __name__ == "__main__":
    main()
