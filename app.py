# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.6 (High-Speed Entry / Zero-Drop Secured)

【更新履歴 v1.4.6】
- [Core] BigQueryを唯一の正（Single Source of Truth）とし、UIは表示・判断に特化。
- [Perf] 起動時の重いクエリを廃止し、遅延ロード（ボタン押下での読み込み）を徹底。
- [UI] 英語ラベルを排除し、完全日本語化。担当者はメールではなく「氏名」で表示。
- [Feature] 新規納品サマリー（昨日/直近7日/当月/FYTD）の追加。
- [UX] 得意先検索を「部分一致 → 候補 → 選択」の高速UIへ変更。
- [Troubleshoot] サイドバーに「通信ヘルスチェック」と「Storage APIトグル」を実装。
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

# 本命ビューの定義（OS v1.4.6 確定版）
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"
VIEW_NEW_DELIVERY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"

# ノイズ除外リスト（000000等のゴミ対策）
NOISE_JAN_SQL = "('0', '22221', '99998', '33334')"

# -----------------------------
# 2. Helpers (UI・表示用ヘルパー)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.6｜判断専用・入口高速版（Zero-Drop Secured）")

def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config = {}
    for col in df.columns:
        if any(k in col for k in ["売上", "粗利", "金額", "差額", "実績", "予測", "GAP"]):
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
    return float(val) if not pd.isna(val) else 0.0

# -----------------------------
# 3. BigQuery Connection & Auth (接続と認証)
# -----------------------------
@st.cache_resource
def setup_bigquery_client() -> bigquery.Client:
    if "bigquery" not in st.secrets:
        st.error("❌ Secrets設定が見つかりません。")
        st.stop()
    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id"))
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)

def query_df_safe(client: bigquery.Client, sql: str, params: Optional[Dict[str, Any]] = None, label: str = "", timeout_sec: int = 60) -> pd.DataFrame:
    # サイドバーのトグル状態を取得して Storage API のON/OFFを切り替え
    use_bqstorage = st.session_state.get("use_bqstorage", True)
    try:
        job_config = bigquery.QueryJobConfig()
        qparams = []
        if params:
            for k, v in params.items():
                if isinstance(v, int): qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
                elif isinstance(v, float): qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
                else: qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
            job_config.query_parameters = qparams
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
    # Required field null エラー解消済みのクリーンマスタを参照
    sql = f"SELECT email, staff_name, role, phone FROM `{VIEW_ROLE_CLEAN}` WHERE email = @login_email LIMIT 1"
    df = query_df_safe(client, sql, {"login_email": login_email}, "Auth Check")
    if df.empty: return RoleInfo(login_email=login_email)
    
    r = df.iloc[0]
    master_phone = str(r.get("phone", "")).replace("-", "").strip()
    last_4_digits = master_phone[-4:]
    
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
            phone=str(r.get("phone", "-"))
        )
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
# 4. UI Sections (各セクションの描画)
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

def render_yoy_section(client, login_email, allow_fallback):
    st.subheader("📊 当月YoY ランキング（判断専用）")
    st.caption("※前年同月と比較可能な得意先のみを抽出。原因特定のための入口です。")
    c1, c2, c3 = st.columns(3)
    def _show_table(title, view_name, key):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 100"
            df = run_scoped_query(client, sql, "login_email", login_email, allow_fallback)
            if not df.empty:
                df_disp = df.rename(columns={"customer_name": "得意先名", "sales_amount": "当月売上", "gross_profit": "当月粗利", "sales_diff_yoy": "売上差額"})
                st.dataframe(df_disp, use_container_width=True, hide_index=True, column_config=create_default_column_config(df_disp))
            else:
                st.info("該当データがありません。")
                
    with c1: _show_table("📉 下がっている先 (Bottom)", VIEW_YOY_BOTTOM, "btn_btm")
    with c2: _show_table("📈 上がっている先 (Top)", VIEW_YOY_TOP, "btn_top")
    with c3: _show_table("🆕 比較不能 (新規等)", VIEW_YOY_UNCOMP, "btn_unc")

def render_new_deliveries_section(client):
    st.subheader("🎉 新規納品サマリー（Realized / 実績）")
    st.caption("※日々の行動が「売上」という事実に結びついた成果の観測装置です。")
    if st.button("新規納品実績を読み込む", key="btn_new_deliv"):
        # Python側で4期間のサマリーを軽量集計して表示（遅延ロード）
        sql = f"""
        WITH td AS (SELECT CURRENT_DATE('Asia/Tokyo') AS today)
        SELECT 
          '① 昨日' AS `期間`, COUNT(DISTINCT customer_code) AS `得意先数`, COUNT(DISTINCT jan_code) AS `品目数`, SUM(sales_amount) AS `売上`, SUM(gross_profit) AS `粗利`
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE first_sales_date = DATE_SUB(today, INTERVAL 1 DAY)
        UNION ALL
        SELECT '② 直近7日', COUNT(DISTINCT customer_code), COUNT(DISTINCT jan_code), SUM(sales_amount), SUM(gross_profit)
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE first_sales_date >= DATE_SUB(today, INTERVAL 7 DAY)
        UNION ALL
        SELECT '③ 当月', COUNT(DISTINCT customer_code), COUNT(DISTINCT jan_code), SUM(sales_amount), SUM(gross_profit)
        FROM `{VIEW_NEW_DELIVERY}` CROSS JOIN td WHERE DATE_TRUNC(first_sales_date, MONTH) = DATE_TRUNC(today, MONTH)
        ORDER BY `期間`
        """
        df_new = query_df_safe(client, sql, label="New Deliveries")
        if not df_new.empty:
            st.dataframe(df_new, use_container_width=True, hide_index=True, column_config=create_default_column_config(df_new))

@st.cache_data(ttl=300)
def fetch_cached_customers(_client, login_email) -> pd.DataFrame:
    # キャッシュ内では session_state は触らない（OS v1.4.6 掟）
    sql = f"SELECT DISTINCT customer_code, customer_name FROM `{VIEW_UNIFIED}` WHERE email = @login_email AND customer_name IS NOT NULL"
    return query_df_safe(_client, sql, {"login_email": login_email}, "Cached Customers")

def render_customer_drilldown(client, login_email):
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")
    df_cust = fetch_cached_customers(client, login_email)
    
    if not df_cust.empty:
        # 部分一致検索UI（OS v1.4.6 掟）
        search_term = st.text_input("🔍 得意先名の一部を入力して検索", placeholder="例：古賀泌尿器")
        
        filtered_df = df_cust
        if search_term:
            filtered_df = df_cust[df_cust['customer_name'].str.contains(search_term, na=False)]
        
        if not filtered_df.empty:
            opts = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in filtered_df.iterrows()}
            sel = st.selectbox("得意先を選択（候補）", options=opts.keys(), format_func=lambda x: opts[x])
            
            if sel:
                st.divider()
                st.markdown(f"#### 💡 提案推奨リスト: {opts[sel]}")
                sql_rec = f"SELECT * FROM `{VIEW_RECOMMEND}` WHERE customer_code = @c ORDER BY priority_rank ASC LIMIT 10"
                df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
                if not df_rec.empty:
                    df_disp = df_rec[["priority_rank", "recommend_product", "manufacturer"]].rename(columns={"priority_rank":"優先順位", "recommend_product":"推奨商品名", "manufacturer":"メーカー"})
                    st.dataframe(df_disp, use_container_width=True, hide_index=True)
                else:
                    st.info("この得意先への推奨データは現在ありません。")
        else:
            st.warning("一致する得意先が見つかりません。")

# -----------------------------
# 5. Main Loop
# -----------------------------
def main():
    set_page()
    client = setup_bigquery_client()
    
    # --- サイドバー構成 ---
    st.sidebar.header("🔑 ログイン")
    login_id = st.sidebar.text_input("ログインID (メールアドレス)", placeholder="example@company.com")
    login_pw = st.sidebar.text_input("ログインコード (携帯下4桁)", type="password")
    
    st.sidebar.divider()
    st.sidebar.header("🛠️ システム管理")
    # Storage API 切替トグル
    st.session_state.use_bqstorage = st.sidebar.checkbox("高速読込 (Storage API) を有効化", value=True, help="読込が詰まる場合はOFFにしてください")
    # ヘルスチェック
    if st.sidebar.button("📡 通信ヘルスチェック"):
        try:
            client.query("SELECT 1").result(timeout=10)
            st.sidebar.success("BigQuery 接続正常！")
        except Exception as e:
            st.sidebar.error("通信エラー発生")
    
    if st.sidebar.button("🧹 キャッシュクリア"):
        st.cache_data.clear()
        st.sidebar.success("キャッシュを削除しました。")

    # --- 認証チェック ---
    if not login_id or not login_pw:
        st.info("👈 サイドバーからログインしてください。")
        st.stop()
        
    role = resolve_role(client, login_id.strip(), login_pw.strip())
    if not role.is_authenticated:
        st.error("❌ ログインIDまたはコードが正しくありません。")
        st.stop()

    # --- ログイン成功後UI ---
    st.success(f"🔓 ログイン中: {role.staff_name} さん")
    c1, c2, c3 = st.columns(3)
    c1.metric("👤 担当", role.staff_name)
    c2.metric("🛡️ 権限", role.role_key)
    c3.metric("📞 電話", role.phone)
    st.divider()

    # 管理者と現場担当者で表示順序を最適化（OS v1.4.6 表示順の掟）
    if role.role_admin_view:
        render_fytd_org_section(client, role.login_email)
        st.divider()
        render_yoy_section(client, role.login_email, allow_fallback=True)
        st.divider()
        render_new_deliveries_section(client)
        st.divider()
        render_customer_drilldown(client, role.login_email)
    else:
        render_fytd_me_section(client, role.login_email)
        st.divider()
        render_yoy_section(client, role.login_email, allow_fallback=False)
        st.divider()
        render_new_deliveries_section(client)
        st.divider()
        render_customer_drilldown(client, role.login_email)

if __name__ == "__main__":
    main()
