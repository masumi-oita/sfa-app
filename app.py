# app.py
# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.9.9 (Native Staff Master / No-Drive Auth)

【更新履歴 v1.9.9】
- [Fix] Role取得の403（Drive credentials）を根絶：sales_staff_master_native（ネイティブBQ）を参照
- [Fix] staff_code を保持（GAS同期のスキーマ前提）
- [UX] 権限・電話番号を上部に明示（電話は末尾4桁のみ表示）
- [Safety] 403が出た場合「外部テーブル依存VIEWの可能性」を画面で明示
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
# 1. Configuration
# -----------------------------
APP_TITLE = "SFA｜戦略ダッシュボード"
DEFAULT_LOCATION = "asia-northeast1"
CACHE_TTL_SEC = 300

APP_URL = "https://sfa-premium-app-2.streamlit.app/"
PROJECT_DEFAULT = "salesdb-479915"
DATASET_DEFAULT = "sales_data"

# ★分析の土台となる統合View
VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"

# KPIカード用などの既存View
VIEW_FYTD_ORG = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_admin_org_fytd_summary_scoped"
VIEW_FYTD_ME = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_staff_fytd_summary_scoped"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_top_current_month_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_bottom_current_month_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_customer_yoy_uncomparable_current_month_named"

# 戦略提案
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_FACT_DAILY = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_login_jan_daily"
VIEW_ITEM_MASTER = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.vw_item_master_norm"

# ★ここが最重要：Roleは “ネイティブBQテーブル” のみに固定
# GAS同期で作った sales_staff_master_native を参照する
VIEW_ROLE_NATIVE = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.sales_staff_master_native"

# 除外コード定義
NOISE_JAN_SQL = "('0', '22221', '99998', '33334')"


# -----------------------------
# 2. Helpers (Display)
# -----------------------------
def set_page():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.9.9｜Staff Master: Native BQ｜BigQuery集計・動的SQL版")


def get_qr_code_url(url: str) -> str:
    return f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={url}"


def rename_columns_for_display(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)


def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
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
    if pd.isna(val):
        return 0.0
    return float(val)


def mask_phone_tail4(phone: str) -> str:
    p = (phone or "").replace("-", "").replace(" ", "").strip()
    tail = p[-4:] if len(p) >= 4 else "----"
    return f"***-****-{tail}"


JP_COLS_FYTD = {
    "login_email": "ログインメール",
    "display_name": "担当者名",
    "sales_amount_fytd": "売上（FYTD）",
    "gross_profit_fytd": "粗利（FYTD）",
    "sales_amount_py_total": "前年売上実績（年）",
    "sales_forecast_total": "売上着地予測（年）",
    "gross_profit_py_total": "前年粗利実績（年）",
    "gp_forecast_total": "粗利着地予測（年）",
}

JP_COLS_YOY = {
    "customer_code": "得意先コード",
    "customer_name": "得意先名",
    "sales_amount": "売上（当月）",
    "gross_profit": "粗利（当月）",
    "sales_amount_py": "売上（前年同月）",
    "sales_diff_yoy": "前年差（売上）",
}


# -----------------------------
# 3. BigQuery Connection
# -----------------------------
@st.cache_resource(show_spinner=False)
def setup_bigquery_client() -> bigquery.Client:
    if "bigquery" not in st.secrets:
        st.error("❌ Secrets設定が見つかりません。st.secrets['bigquery'] を設定してください。")
        st.stop()

    bq = st.secrets["bigquery"]
    project_id = str(bq.get("project_id") or PROJECT_DEFAULT)
    location = str(bq.get("location") or DEFAULT_LOCATION)
    sa = dict(bq.get("service_account"))

    # ※ Drive/Sheetsスコープを付けない（Drive外部テーブルを参照しない設計へ寄せる）
    creds = service_account.Credentials.from_service_account_info(sa)
    return bigquery.Client(project=project_id, credentials=creds, location=location)


def query_df_safe(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    use_bqstorage: bool = True,
    timeout_sec: int = 60,
) -> pd.DataFrame:
    try:
        job_config = bigquery.QueryJobConfig()
        qparams = []
        if params:
            for k, v in params.items():
                if isinstance(v, int):
                    qparams.append(bigquery.ScalarQueryParameter(k, "INT64", v))
                elif isinstance(v, float):
                    qparams.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
                else:
                    qparams.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
        if qparams:
            job_config.query_parameters = qparams

        job = client.query(sql, job_config=job_config)
        job.result(timeout=timeout_sec)
        return job.to_dataframe(create_bqstorage_client=use_bqstorage)

    except Exception as e:
        st.error(f"Query Failed: {label}\n{e}")
        return pd.DataFrame()


# -----------------------------
# 4. Role (Native Table Only)
# -----------------------------
@dataclass(frozen=True)
class RoleInfo:
    login_email: str
    staff_code: str = ""
    staff_name: str = "ゲスト"
    role_key: str = "SALES"  # HQ_ADMIN / SALES
    role_admin_view: bool = False
    phone: str = "-"
    area_name: str = "未設定"


def resolve_role_native(client: bigquery.Client, login_email: str) -> RoleInfo:
    """
    ★重要：Drive外部テーブルを参照しない
    ここは sales_staff_master_native（ネイティブBQ）だけを見る
    """
    sql = f"""
    SELECT
      staff_code,
      email,
      staff_name,
      role,
      phone,
      area
    FROM `{VIEW_ROLE_NATIVE}`
    WHERE LOWER(email) = LOWER(@login_email)
    LIMIT 1
    """
    df = query_df_safe(client, sql, {"login_email": login_email}, "Role Check (NATIVE)")
    if df.empty:
        return RoleInfo(login_email=login_email)

    r = df.iloc[0]
    raw_role = str(r.get("role", "")).strip().upper()
    is_admin = any(x in raw_role for x in ["ADMIN", "MANAGER", "HQ", "統括"])

    return RoleInfo(
        login_email=login_email,
        staff_code=str(r.get("staff_code", "") or ""),
        staff_name=str(r.get("staff_name", "不明")),
        role_key="HQ_ADMIN" if is_admin else "SALES",
        role_admin_view=is_admin,
        phone=str(r.get("phone", "-")),
        area_name=str(r.get("area", "未設定")),
    )


# -----------------------------
# 5. Scoped Query Helper
# -----------------------------
def run_scoped_query(client: bigquery.Client, sql_template: str, scope_col: str, login_email: str, allow_fallback: bool = False):
    """
    sql_template 内に __WHERE__ を含める。
    例: SELECT * FROM `...` __WHERE__ LIMIT 100
    """
    sql = sql_template.replace("__WHERE__", f"WHERE {scope_col} = @login_email")
    df = query_df_safe(client, sql, {"login_email": login_email}, "Scoped Query")
    if not df.empty:
        return df

    if allow_fallback:
        sql_all = sql_template.replace("__WHERE__", f'WHERE {scope_col} = "all" OR {scope_col} IS NULL')
        return query_df_safe(client, sql_all, None, "Fallback Query")
    return pd.DataFrame()


# -----------------------------
# 6. Ranking / Drilldown (Unified View)
# -----------------------------
def fetch_ranking_from_bq(client: bigquery.Client, ranking_type: str, axis_mode: str, is_sales_mode: bool) -> pd.DataFrame:
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
      SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN {target_val} ELSE 0 END)
        - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN {target_val} ELSE 0 END) AS diff_val,
      SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN sales_amount ELSE 0 END)
        - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN sales_amount ELSE 0 END) AS sales_diff,
      SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) THEN gross_profit ELSE 0 END)
        - SUM(CASE WHEN fiscal_year = (SELECT current_fy FROM base_stats) - 1 THEN gross_profit ELSE 0 END) AS gp_diff
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


def fetch_drilldown_from_bq(client: bigquery.Client, key_col: str, key_val: str, target_col: str, is_worst: bool, is_sales_mode: bool) -> pd.DataFrame:
    order_dir = "ASC" if is_worst else "DESC"
    sort_col_alias = "売上差額" if is_sales_mode else "粗利差額"
    target_label = "得意先名" if target_col == "customer_name" else "商品名"

    sql = f"""
    SELECT
      {target_col} AS `{target_label}`,
      SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) THEN sales_amount ELSE 0 END) AS `今年売上`,
      SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) - 1 THEN sales_amount ELSE 0 END) AS `前年売上`,
      SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) THEN sales_amount ELSE 0 END)
        - SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) - 1 THEN sales_amount ELSE 0 END) AS `売上差額`,
      SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) THEN gross_profit ELSE 0 END) AS `今年粗利`,
      SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) THEN gross_profit ELSE 0 END)
        - SUM(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_UNIFIED}`) - 1 THEN gross_profit ELSE 0 END) AS `粗利差額`
    FROM `{VIEW_UNIFIED}`
    WHERE {key_col} = @key_val
    GROUP BY 1
    ORDER BY `{sort_col_alias}` {order_dir}
    LIMIT 500
    """
    return query_df_safe(client, sql, {"key_val": key_val}, "Drilldown Query")


# -----------------------------
# 7. UI Parts
# -----------------------------
def sidebar_controls() -> Dict[str, Any]:
    st.sidebar.image(get_qr_code_url(APP_URL), caption="📱スマホでアクセス", width=150)
    st.sidebar.divider()
    use_bqstorage = st.sidebar.toggle("BigQuery Storage API（高速）", value=True)
    timeout_sec = st.sidebar.slider("タイムアウト（秒）", 10, 180, 60, 10)
    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.sidebar.success("Cache Cleared.")
    return {"use_bqstorage": use_bqstorage, "timeout_sec": timeout_sec}


def get_login_email_ui() -> str:
    st.sidebar.header("Login（暫定）")
    default = st.secrets.get("default_login_email", "") if "default_login_email" in st.secrets else ""
    return st.sidebar.text_input("Login Email", value=default).strip()


def render_interactive_ranking_matrix(client: bigquery.Client, ranking_type: str, axis_mode: str, is_sales_mode: bool):
    is_worst = (ranking_type == "worst")
    is_product = (axis_mode == "product")
    label_col = "商品名" if is_product else "得意先名"
    mode_label = "売上" if is_sales_mode else "粗利"

    df_rank = fetch_ranking_from_bq(client, ranking_type, axis_mode, is_sales_mode)
    if df_rank.empty:
        st.info("データがありません。")
        return

    df_disp = df_rank.rename(
        columns={
            "name": label_col,
            "sales_cur": "今年売上",
            "sales_prev": "前年売上",
            "sales_diff": "売上差額",
            "gp_cur": "今年粗利",
            "gp_diff": "粗利差額",
        }
    )

    cols = (
        [label_col, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
        if is_sales_mode
        else [label_col, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]
    )

    st.markdown(f"##### ① {label_col}を選択（{mode_label}ベース）")
    st.caption(f"※{mode_label}の増減額が大きい順（計算: BigQuery）")

    key_suffix = f"{ranking_type}_{axis_mode}_{mode_label}"
    event = st.dataframe(
        df_disp[cols],
        use_container_width=True,
        hide_index=True,
        column_config=create_default_column_config(df_disp[cols]),
        height=400,
        on_select="rerun",
        selection_mode="single-row",
        key=f"t1_{key_suffix}",
    )

    if len(event.selection["rows"]) > 0:
        idx = event.selection["rows"][0]
        selected_val = df_disp.iloc[idx][label_col]

        st.divider()
        st.subheader(f"🔎 内訳分析: {selected_val}")

        key_col = "product_name" if is_product else "customer_name"
        target_col = "customer_name" if is_product else "product_name"

        df_drill = fetch_drilldown_from_bq(client, key_col, selected_val, target_col, is_worst, is_sales_mode)
        if df_drill.empty:
            st.warning("詳細データなし")
            return

        drill_label = "得意先名" if is_product else "商品名"
        d_cols = (
            [drill_label, "売上差額", "今年売上", "前年売上", "粗利差額", "今年粗利"]
            if is_sales_mode
            else [drill_label, "粗利差額", "今年粗利", "売上差額", "今年売上", "前年売上"]
        )
        st.dataframe(
            df_drill[d_cols],
            use_container_width=True,
            hide_index=True,
            column_config=create_default_column_config(df_drill[d_cols]),
            key=f"t2_{key_suffix}",
        )


def render_fytd_org_section(client: bigquery.Client, login_email: str, allow_fallback: bool, opts: Dict[str, Any]):
    st.subheader("🏢 年度累計（FYTD）｜全社")

    if st.button("全社データを読み込む", key="btn_org_load", use_container_width=True):
        st.session_state.org_data_loaded = True

    if not st.session_state.org_data_loaded:
        st.info("👆 上のボタンを押して全社データを読み込んでください")
        return

    sql_kpi = f"SELECT * FROM `{VIEW_FYTD_ORG}` __WHERE__ LIMIT 100"
    df_org = run_scoped_query(client, sql_kpi, "viewer_email", login_email, allow_fallback=allow_fallback)

    if df_org.empty:
        st.warning("全社KPIが取得できません。")
        st.info("⚠️ 403 が出る場合、このVIEWが Drive/Sheets 外部テーブル依存の可能性があります。VIEW定義を外部依存なしに修正してください。")
    else:
        row = df_org.iloc[0]
        s_cur, s_py, s_fc = get_safe_float(row, "sales_amount_fytd"), get_safe_float(row, "sales_amount_py_total"), get_safe_float(row, "sales_forecast_total")
        gp_cur, gp_py, gp_fc = get_safe_float(row, "gross_profit_fytd"), get_safe_float(row, "gross_profit_py_total"), get_safe_float(row, "gp_forecast_total")

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

    st.subheader("📊 増減要因分析（多次元）")
    c_axis, c_val = st.columns(2)
    with c_axis:
        axis_sel = st.radio("集計軸:", ["📦 商品軸", "🏥 得意先軸"], horizontal=True)
        axis_mode = "product" if "商品" in axis_sel else "customer"
    with c_val:
        val_sel = st.radio("評価指標:", ["💰 売上金額", "💹 粗利金額"], horizontal=True)
        is_sales_mode = "売上" in val_sel

    tab_worst, tab_best = st.tabs(["📉 ワースト（減）", "📈 ベスト（増）"])
    with tab_worst:
        render_interactive_ranking_matrix(client, "worst", axis_mode, is_sales_mode)
    with tab_best:
        render_interactive_ranking_matrix(client, "best", axis_mode, is_sales_mode)


def render_fytd_me_section(client: bigquery.Client, login_email: str):
    st.subheader("👤 年度累計（FYTD）｜自分")

    if st.button("自分データを読み込む", key="btn_me", use_container_width=True):
        sql = f"SELECT * FROM `{VIEW_FYTD_ME}` __WHERE__ LIMIT 100"
        df_me = run_scoped_query(client, sql, "login_email", login_email, allow_fallback=False)
        if df_me.empty:
            st.warning("自分FYTDが取得できません。")
            return

        df_disp = rename_columns_for_display(df_me, JP_COLS_FYTD)
        cols = list(df_disp.columns)
        if "担当者名" in cols:
            cols.remove("担当者名")
            cols.insert(0, "担当者名")
        st.dataframe(df_disp[cols], use_container_width=True, hide_index=True, column_config=create_default_column_config(df_disp[cols]))


def render_yoy_section(client: bigquery.Client, login_email: str, allow_fallback: bool):
    st.subheader("📊 当月YoY（得意先ランキング）")
    c1, c2, c3 = st.columns(3)

    def _show_table(title: str, view_name: str, key: str):
        if st.button(title, key=key, use_container_width=True):
            sql = f"SELECT * FROM `{view_name}` __WHERE__ LIMIT 200"
            df = run_scoped_query(client, sql, "login_email", login_email, allow_fallback=allow_fallback)
            if df.empty:
                st.info("0件です。")
                return
            df_disp = rename_columns_for_display(df, JP_COLS_YOY)
            st.dataframe(df_disp, use_container_width=True, hide_index=True)

    with c1:
        _show_table("YoY Top（伸び）", VIEW_YOY_TOP, "btn_top")
    with c2:
        _show_table("YoY Bottom（落ち）", VIEW_YOY_BOTTOM, "btn_btm")
    with c3:
        _show_table("新規/比較不能", VIEW_YOY_UNCOMP, "btn_unc")


def render_customer_drilldown(client: bigquery.Client, login_email: str):
    st.subheader("🎯 得意先別・戦略提案")

    sql_cust = f"""
    SELECT DISTINCT customer_code, customer_name
    FROM `{VIEW_FACT_DAILY}`
    WHERE login_email = @login_email
    ORDER BY customer_code
    """
    df_cust = query_df_safe(client, sql_cust, {"login_email": login_email}, "Cust List")
    if df_cust.empty:
        st.info("得意先一覧が取得できません。")
        return

    cust_options = {row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}" for _, row in df_cust.iterrows()}
    selected_code = st.selectbox("分析する得意先を選択してください", options=list(cust_options.keys()), format_func=lambda x: cust_options[x])

    if not selected_code:
        return

    st.divider()

    # 提案（recommendation engine）
    sql_rec = f"""
    SELECT *
    FROM `{VIEW_RECOMMEND}`
    WHERE customer_code = @cust_code
    ORDER BY priority_rank ASC
    """
    df_rec = query_df_safe(client, sql_rec, {"cust_code": selected_code}, "Recommendation")

    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("#### 🏥 プロファイル")
        strong = df_rec.iloc[0].get("strong_category", "-") if not df_rec.empty and "strong_category" in df_rec.columns else "-"
        st.info(f"主力領域: **{strong}**")

    with c2:
        st.markdown("#### 💡 AI提案リスト")
        if df_rec.empty:
            st.info("提案データがありません。")
        else:
            cols = [c for c in ["priority_rank", "recommend_product", "manufacturer", "market_scale"] if c in df_rec.columns]
            disp_df = df_rec[cols].copy()
            disp_df = disp_df.rename(
                columns={
                    "priority_rank": "順位",
                    "recommend_product": "商品",
                    "manufacturer": "メーカー",
                    "market_scale": "規模",
                }
            )
            st.dataframe(disp_df, use_container_width=True, hide_index=True)

    with st.expander("参考: 現在の採用品リストを見る"):
        sql_adopted = f"""
        SELECT
          m.product_name AS product_name,
          SUM(t.sales_amount) AS sales_fytd,
          SUM(t.gross_profit) AS gp_fytd
        FROM `{VIEW_FACT_DAILY}` t
        LEFT JOIN `{VIEW_ITEM_MASTER}` m
          ON CAST(t.jan AS STRING) = CAST(m.jan_code AS STRING)
        WHERE t.customer_code = @cust_code
          AND t.fiscal_year = (SELECT MAX(fiscal_year) FROM `{VIEW_FACT_DAILY}`)
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 100
        """
        df_adopted = query_df_safe(client, sql_adopted, {"cust_code": selected_code}, "Adopted List")
        if df_adopted.empty:
            st.info("採用品リストはありません。")
        else:
            renamed_df = df_adopted.rename(columns={"product_name": "商品名", "sales_fytd": "売上(FYTD)", "gp_fytd": "粗利(FYTD)"})
            st.dataframe(renamed_df, use_container_width=True, hide_index=True, column_config=create_default_column_config(renamed_df))


# -----------------------------
# 8. Main
# -----------------------------
def main():
    if "org_data_loaded" not in st.session_state:
        st.session_state.org_data_loaded = False

    set_page()

    client = setup_bigquery_client()
    opts = sidebar_controls()

    login_email = get_login_email_ui()
    if not login_email:
        st.info("左のサイドバーで login_email を入力してください。")
        st.stop()

    st.divider()

    # ★RoleはネイティブBQから取得（Drive依存ゼロ）
    role = resolve_role_native(client, login_email)

    # 上部ヘッダー
    st.subheader("ログイン情報（Native Staff Master）")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("👤 担当", role.staff_name)
    c2.metric("📧 Email", role.login_email)
    c3.metric("🛡️ Role", role.role_key)
    c4.metric("📞 Phone", mask_phone_tail4(role.phone))

    st.caption(f"staff_code: {role.staff_code or '-'} / area: {role.area_name or '未設定'}")

    st.divider()

    allow_fallback = role.role_admin_view  # HQ_ADMINのみ all fallback

    # タブ分岐
    if role.role_admin_view:
        t1, t2, t3 = st.tabs(["🏢 全社状況（経営）", "👤 個人/担当（行動）", "🎯 得意先別・提案"])
        with t1:
            render_fytd_org_section(client, login_email, allow_fallback=True, opts=opts)
        with t2:
            render_fytd_me_section(client, login_email)
            st.divider()
            render_yoy_section(client, login_email, allow_fallback=True)
        with t3:
            render_customer_drilldown(client, login_email)
    else:
        t1, t2, t3 = st.tabs(["👤 今年の成績（FYTD）", "📊 得意先別（YoY）", "🎯 提案を作る"])
        with t1:
            render_fytd_me_section(client, login_email)
        with t2:
            render_yoy_section(client, login_email, allow_fallback=False)
        with t3:
            render_customer_drilldown(client, login_email)

    st.caption("※ 403（Drive credentials）が出たVIEWは、Drive/Sheets外部テーブル依存の可能性があります。Roleはnativeなので403になりません。")


if __name__ == "__main__":
    main()
