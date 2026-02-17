# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.7
(Complete Strategic Integration / Pace Forecast & YJ-Customer Hierarchical Drilldown)
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

# ビュー定義
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_top_fy_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_bottom_fy_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_uncomparable_fy_named"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

# -----------------------------
# 2. Helpers (表示・計算用)
# -----------------------------

def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.7｜戦略ドリルダウン ＋ 認証安定化 統合版")

def get_safe_float(row: pd.Series, key: str) -> float:
    val = row.get(key)
    return float(val) if not pd.isna(val) else 0.0

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    """表のフォーマットを一括設定"""
    config: Dict[str, st.column_config.Column] = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差額", "実績", "予測", "GAP"]):
            config[col] = st.column_config.NumberColumn(col, format="¥%d")
        elif any(k in col for k in ["率", "比", "ペース"]):
            config[col] = st.column_config.NumberColumn(col, format="%.1f%%")
        elif "日" in col or pd.api.types.is_datetime64_any_dtype(df[col]):
            config[col] = st.column_config.DateColumn(col, format="YYYY-MM-DD")
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
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return bigquery.Client(project=PROJECT_DEFAULT, credentials=creds, location=DEFAULT_LOCATION)

def _normalize_param(value: Any) -> Tuple[str, Optional[Any]]:
    """型安全なBigQueryパラメータ変換"""
    if isinstance(value, tuple) and len(value) == 2:
        return str(value[0]).upper(), value[1]
    if value is None: return "STRING", None
    if isinstance(value, bool): return "BOOL", value
    if isinstance(value, int): return "INT64", value
    if isinstance(value, float): return "FLOAT64", value
    if isinstance(value, pd.Timestamp): return "TIMESTAMP", value.to_pydatetime()
    return "STRING", str(value)

def query_df_safe(client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None, label: str = "") -> pd.DataFrame:
    use_bqstorage = st.session_state.get("use_bqstorage", True)
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, *_normalize_param(v)) for k, v in params.items()
            ]
        job = client.query(sql, job_config=job_config)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)
    except Exception as e:
        st.error(f"クエリエラー ({label}): {e}")
        return pd.DataFrame()

@dataclass(frozen=True)
class RoleInfo:
    is_authenticated: bool = False
    login_email: str = ""
    staff_name: str = "ゲスト"
    role_key: str = "GUEST"
    role_admin_view: bool = False

@st.cache_data(ttl=3600)
def check_login_code_col(_client: bigquery.Client) -> bool:
    parts = VIEW_ROLE_CLEAN.split(".")
    sql = f"SELECT column_name FROM `{parts[0]}.{parts[1]}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = @t AND column_name = 'login_code'"
    df = query_df_safe(_client, sql, {"t": parts[2]})
    return not df.empty

def resolve_role(client: bigquery.Client, login_email: str, login_code: str) -> RoleInfo:
    if not login_email or not login_code: return RoleInfo()
    has_code = check_login_code_col(client)
    sql = f"SELECT login_email, role_tier FROM `{VIEW_ROLE_CLEAN}` WHERE login_email = @email"
    if has_code: sql += " AND CAST(login_code AS STRING) = @code"
    
    df = query_df_safe(client, sql, {"email": login_email, "code": login_code}, "Auth")
    if df.empty: return RoleInfo(login_email=login_email)
    
    row = df.iloc[0]
    is_admin = any(x in str(row["role_tier"]).upper() for x in ["ADMIN", "MANAGER", "HQ"])
    return RoleInfo(True, login_email, login_email.split("@")[0], "HQ_ADMIN" if is_admin else "SALES", is_admin)

# -----------------------------
# 4. UI Sections (各セクション)
# -----------------------------

def render_metrics_dashboard(row: pd.Series):
    """サマリーメトリクスの表示ロジック"""
    s_cur = get_safe_float(row, "sales_amount_fytd")
    s_py_ytd = get_safe_float(row, "sales_amount_py_ytd")
    s_py_total = get_safe_float(row, "sales_amount_py_total")
    s_fc = s_cur * (s_py_total / s_py_ytd) if s_py_ytd > 0 else s_cur

    gp_cur = get_safe_float(row, "gross_profit_fytd")
    gp_py_ytd = get_safe_float(row, "gross_profit_py_ytd")
    gp_py_total = get_safe_float(row, "gross_profit_py_total")
    gp_fc = gp_cur * (gp_py_total / gp_py_ytd) if gp_py_ytd > 0 else gp_cur

    st.caption("💡 今期予測： 今期実績 × (昨年度着地 ÷ 前年同期) ※季節変動を加味した推移ペース")
    
    # 売上表示
    st.markdown("##### ■ 売上")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{s_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{s_py_ytd:,.0f}", delta=f"{int(s_cur - s_py_ytd):,.0f}")
    c3.metric("③ 昨年度着地", f"¥{s_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{s_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{s_fc - s_py_total:,.0f}", delta=f"{int(s_fc - s_py_total):,.0f}")

    # 粗利表示
    st.markdown("##### ■ 粗利")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{gp_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{gp_py_ytd:,.0f}", delta=f"{int(gp_cur - gp_py_ytd):,.0f}")
    c3.metric("③ 昨年度着地", f"¥{gp_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{gp_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{gp_fc - gp_py_total:,.0f}", delta=f"{int(gp_fc - gp_py_total):,.0f}")

def get_summary_data(client, email=None):
    where = f"WHERE login_email = '{email}'" if email else ""
    sql = f"""
        WITH info AS (
          SELECT CURRENT_DATE('Asia/Tokyo') AS td, DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_td,
          (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS cfy
        )
        SELECT
          SUM(CASE WHEN fiscal_year = cfy THEN sales_amount ELSE 0 END) AS sales_amount_fytd,
          SUM(CASE WHEN fiscal_year = cfy THEN gross_profit ELSE 0 END) AS gross_profit_fytd,
          SUM(CASE WHEN fiscal_year = cfy - 1 AND sales_date <= py_td THEN sales_amount ELSE 0 END) AS sales_amount_py_ytd,
          SUM(CASE WHEN fiscal_year = cfy - 1 AND sales_date <= py_td THEN gross_profit ELSE 0 END) AS gross_profit_py_ytd,
          SUM(CASE WHEN fiscal_year = cfy - 1 THEN sales_amount ELSE 0 END) AS sales_amount_py_total,
          SUM(CASE WHEN fiscal_year = cfy - 1 THEN gross_profit ELSE 0 END) AS gross_profit_py_total
        FROM `{VIEW_UNIFIED}` CROSS JOIN info {where}
    """
    return query_df_safe(client, sql, label="Summary")

def render_yj_drilldown(client, login_email, is_admin):
    """成分ランキングと詳細分析"""
    st.subheader("📊 年間 YoY ランキング（成分・YJベース）")
    
    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode, st.session_state.yoy_df = None, pd.DataFrame()

    c1, c2, c3 = st.columns(3)
    def load_ranking(m, v):
        st.session_state.yoy_mode = m
        where = "" if is_admin else "WHERE login_email = @email"
        sql = f"SELECT yj_code, product_name, sales_amount, py_sales_amount, sales_diff_yoy FROM `{v}` {where} LIMIT 100"
        st.session_state.yoy_df = query_df_safe(client, sql, {"email": login_email}, m)

    with c1: 
        if st.button("📉 下落幅ワースト", use_container_width=True): load_ranking("ワースト", VIEW_YOY_BOTTOM)
    with c2: 
        if st.button("📈 上昇幅ベスト", use_container_width=True): load_ranking("ベスト", VIEW_YOY_TOP)
    with c3: 
        if st.button("🆕 新規/比較不能", use_container_width=True): load_ranking("新規", VIEW_YOY_UNCOMP)

    if not st.session_state.yoy_df.empty:
        df_disp = st.session_state.yoy_df.rename(columns={
            "yj_code": "YJコード", "product_name": "代表成分名", 
            "sales_amount": "今期売上", "py_sales_amount": "前期売上", "sales_diff_yoy": "前年比差額"
        }).fillna(0)
        
        st.markdown(f"#### 🏆 第一階層：成分（YJ）{st.session_state.yoy_mode} ランキング")
        st.dataframe(df_disp.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
        
        st.divider()
        st.markdown("#### 🔍 第二階層：成分の「得意先別」内訳")
        yj_list = df_disp.drop_duplicates("YJコード")
        yj_opts = {r["YJコード"]: f"{r['代表成分名']} (差額: ¥{r['前年比差額']:,.0f})" for _, r in yj_list.iterrows()}
        selected_yj = st.selectbox("分析する成分を選択", options=list(yj_opts.keys()), format_func=lambda x: yj_opts[x])
        
        if selected_yj:
            where_ext = "" if is_admin else "AND login_email = @email"
            sort = "ASC" if st.session_state.yoy_mode == "ワースト" else "DESC"
            sql_drill = f"""
                SELECT customer_name AS `得意先名`, 
                SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) THEN sales_amount ELSE 0 END) AS `今期売上`,
                SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) - 1 THEN sales_amount ELSE 0 END) AS `前期売上`
                FROM `{VIEW_UNIFIED}` WHERE yj_code = @yj {where_ext} GROUP BY 1 ORDER BY (`今期売上` - `前期売上`) {sort} LIMIT 50
            """
            df_drill = query_df_safe(client, sql_drill, {"yj": selected_yj, "email": login_email}, "Drilldown")
            if not df_drill.empty:
                df_drill["前年比差額"] = df_drill["今期売上"] - df_drill["前期売上"]
                st.dataframe(df_drill.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "前年比差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

                st.markdown("#### 🧪 第三階層：詳細要因（JAN別）")
                sql_jan = f"""
                    SELECT jan_code AS `JAN`, ANY_VALUE(product_name) AS `商品名`,
                    SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) THEN sales_amount ELSE 0 END) AS `今期売上`,
                    SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo')) - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) - 1 THEN sales_amount ELSE 0 END) AS `前期売上`
                    FROM `{VIEW_UNIFIED}` WHERE yj_code = @yj {where_ext} GROUP BY 1 ORDER BY (`今期売上` - `前期売上`) {sort}
                """
                df_jan = query_df_safe(client, sql_jan, {"yj": selected_yj, "email": login_email}, "JAN")
                if not df_jan.empty:
                    df_jan["差額"] = df_jan["今期売上"] - df_jan["前期売上"]
                    st.dataframe(df_jan.style.format({"今期売上": "¥{:,.0f}", "前期売上": "¥{:,.0f}", "差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

def render_adoption_alerts(client, email, is_admin):
    st.subheader("🚨 採用・失注アラート")
    where = "" if is_admin else "WHERE login_email = @email"
    sql = f"SELECT staff_name, customer_name, product_name, last_purchase_date, adoption_status, current_fy_sales, previous_fy_sales FROM `{VIEW_ADOPTION}` {where} ORDER BY 5, 6 ASC"
    df = query_df_safe(client, sql, {"email": email}, "Alerts")
    if not df.empty:
        df["売上差額"] = df["current_fy_sales"] - df["previous_fy_sales"]
        st.dataframe(df.style.format({"current_fy_sales": "¥{:,.0f}", "previous_fy_sales": "¥{:,.0f}", "売上差額": "¥{:,.0f}"}), use_container_width=True, hide_index=True)

def render_customer_drilldown(client, email, is_admin):
    st.subheader("🎯 担当先ドリルダウン ＆ 提案")
    where = "" if is_admin else "WHERE login_email = @email"
    sql = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_UNIFIED}` {where} AND customer_name IS NOT NULL"
    df_cust = query_df_safe(client, sql, {"email": email}, "Cust List")
    if not df_cust.empty:
        sel = st.selectbox("得意先を選択", options=df_cust["customer_code"].tolist(), format_func=lambda x: df_cust[df_cust["customer_code"]==x]["customer_name"].values[0])
        if sel:
            st.divider()
            st.markdown("##### 📦 現在の採用アイテム状況")
            sql_a = f"SELECT product_name, adoption_status, last_purchase_date, current_fy_sales FROM `{VIEW_ADOPTION}` WHERE customer_code = @c ORDER BY 4 DESC"
            df_a = query_df_safe(client, sql_a, {"c": sel}, "Adopt")
            if not df_a.empty: st.dataframe(df_a.style.format({"current_fy_sales": "¥{:,.0f}"}), use_container_width=True, hide_index=True)
            
            st.markdown("##### 💡 AI 推奨提案商品（Reco）")
            sql_r = f"SELECT priority_rank, recommend_product, manufacturer FROM `{VIEW_RECOMMEND}` WHERE customer_code = @c ORDER BY 1 LIMIT 10"
            df_r = query_df_safe(client, sql_r, {"c": sel}, "Reco")
            if not df_r.empty: st.dataframe(df_r, use_container_width=True, hide_index=True)

# -----------------------------
# 5. Main Loop
# -----------------------------

def main():
    set_page()
    client = setup_bigquery_client()
    
    with st.sidebar:
        st.header("🔑 ログイン")
        l_id = st.text_input("ログインID (メールアドレス)")
        l_pw = st.text_input("パスコード (携帯下4桁)", type="password")
        st.divider()
        st.session_state.use_bqstorage = st.checkbox("高速読込 (Storage API)", value=True)
        if st.button("🧹 キャッシュクリア"): st.cache_data.clear()

    if not l_id or not l_pw:
        st.info("👈 サイドバーからログインしてください。")
        return

    role = resolve_role(client, l_id.strip(), l_pw.strip())
    if not role.is_authenticated:
        st.error("❌ ログイン情報が正しくありません。")
        return

    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    st.divider()

    # サマリーセクション
    title = "全社サマリー" if role.role_admin_view else "個人サマリー"
    st.subheader(f"🏢 {title}")
    if st.button(f"{title}を読み込む"):
        df = get_summary_data(client, None if role.role_admin_view else role.login_email)
        if not df.empty: render_metrics_dashboard(df.iloc[0])

    st.divider()
    render_yj_drilldown(client, role.login_email, role.role_admin_view)
    st.divider()
    render_adoption_alerts(client, role.login_email, role.role_admin_view)
    st.divider()
    render_customer_drilldown(client, role.login_email, role.role_admin_view)

if __name__ == "__main__":
    main()
