# -*- coding: utf-8 -*-
"""
SFA｜戦略ダッシュボード - OS v1.4.7
(Integrated Update / Auth Hardening & Typed Params)
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

VIEW_UNIFIED = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_fact_unified"
VIEW_ROLE_CLEAN = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.dim_staff_role_clean"
VIEW_YOY_TOP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_top_fy_named"
VIEW_YOY_BOTTOM = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_bottom_fy_named"
VIEW_YOY_UNCOMP = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_yj_yoy_uncomparable_fy_named"
VIEW_NEW_DELIVERY = (
    f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_new_deliveries_realized_daily_fact_all_months"
)
VIEW_RECOMMEND = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_sales_recommendation_engine"
VIEW_ADOPTION = f"{PROJECT_DEFAULT}.{DATASET_DEFAULT}.v_customer_adoption_status"

# -----------------------------
# 2. Helpers (表示用)
# -----------------------------

def set_page() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("OS v1.4.7｜認証強化・型安全化・YJドリル安定化")


def create_default_column_config(df: pd.DataFrame) -> Dict[str, st.column_config.Column]:
    config: Dict[str, st.column_config.Column] = {}
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
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return bigquery.Client(
        project=PROJECT_DEFAULT,
        credentials=creds,
        location=DEFAULT_LOCATION,
    )


def _normalize_param(
    value: Any,
) -> Tuple[str, Optional[Any]]:
    """
    query_df_safe() 用の型推定ヘルパー
    - tuple("TYPE", value) の場合は明示型を優先
    - それ以外は値型から推定
    """
    if isinstance(value, tuple) and len(value) == 2:
        p_type, p_value = value
        return str(p_type).upper(), p_value

    if value is None:
        return "STRING", None
    if isinstance(value, bool):
        return "BOOL", value
    if isinstance(value, int):
        return "INT64", value
    if isinstance(value, float):
        return "FLOAT64", value
    if isinstance(value, pd.Timestamp):
        return "TIMESTAMP", value.to_pydatetime()

    return "STRING", str(value)


def query_df_safe(
    client: bigquery.Client,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    label: str = "",
    timeout_sec: int = 60,
) -> pd.DataFrame:
    use_bqstorage = st.session_state.get("use_bqstorage", True)
    try:
        job_config = bigquery.QueryJobConfig()
        if params:
            query_params = []
            for key, raw_value in params.items():
                p_type, p_value = _normalize_param(raw_value)
                query_params.append(bigquery.ScalarQueryParameter(key, p_type, p_value))
            job_config.query_parameters = query_params

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


def resolve_role(client: bigquery.Client, login_email: str, login_code: str) -> RoleInfo:
    if not login_email or not login_code:
        return RoleInfo()

    # login_code を認証に利用（最小修正）。
    sql = f"""
        SELECT login_email, role_tier
        FROM `{VIEW_ROLE_CLEAN}`
        WHERE login_email = @login_email
          AND CAST(login_code AS STRING) = @login_code
        LIMIT 1
    """
    df = query_df_safe(
        client,
        sql,
        {"login_email": login_email, "login_code": login_code},
        "Auth Check",
    )

    if df.empty:
        return RoleInfo(login_email=login_email)

    row = df.iloc[0]
    raw_role = str(row["role_tier"]).strip().upper()
    is_admin = any(x in raw_role for x in ["ADMIN", "MANAGER", "HQ"])

    return RoleInfo(
        is_authenticated=True,
        login_email=login_email,
        staff_name=login_email.split("@")[0],
        role_key="HQ_ADMIN" if is_admin else "SALES",
        role_admin_view=is_admin,
        phone="-",
    )


# -----------------------------
# 4. UI Sections (各セクション)
# -----------------------------

def render_summary_metrics(row: pd.Series) -> None:
    s_cur = get_safe_float(row, "sales_amount_fytd")
    s_py_ytd = get_safe_float(row, "sales_amount_py_ytd")
    s_py_total = get_safe_float(row, "sales_amount_py_total")

    # 季節変動を加味したペース予測
    s_fc = s_cur * (s_py_total / s_py_ytd) if s_py_ytd > 0 else s_cur

    gp_cur = get_safe_float(row, "gross_profit_fytd")
    gp_py_ytd = get_safe_float(row, "gross_profit_py_ytd")
    gp_py_total = get_safe_float(row, "gross_profit_py_total")
    gp_fc = gp_cur * (gp_py_total / gp_py_ytd) if gp_py_ytd > 0 else gp_cur

    st.caption(
        "※ 【今期予測】はAI予測ではなく、「今期実績 × (昨年度着地 ÷ 前年同期)」"
        "による季節変動を加味した推移ペース（着地見込）です。"
    )

    st.markdown("##### ■ 売上")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{s_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{s_py_ytd:,.0f}", delta=f"{int(s_cur - s_py_ytd):,.0f}" if s_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{s_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{s_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{s_fc - s_py_total:,.0f}", delta=f"{int(s_fc - s_py_total):,.0f}")

    st.markdown("##### ■ 粗利")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("① 今期累計", f"¥{gp_cur:,.0f}")
    c2.metric("② 前年同期", f"¥{gp_py_ytd:,.0f}", delta=f"{int(gp_cur - gp_py_ytd):,.0f}" if gp_py_ytd > 0 else None)
    c3.metric("③ 昨年度着地", f"¥{gp_py_total:,.0f}")
    c4.metric("④ 今期予測", f"¥{gp_fc:,.0f}")
    c5.metric("⑤ 着地GAP", f"¥{gp_fc - gp_py_total:,.0f}", delta=f"{int(gp_fc - gp_py_total):,.0f}")


def render_fytd_org_section(client: bigquery.Client, login_email: str) -> None:
    del login_email
    st.subheader("🏢 年度累計（FYTD）｜全社サマリー")
    if st.button("全社データを読み込む", key="btn_org_load"):
        st.session_state.org_data_loaded = True

    if st.session_state.get("org_data_loaded"):
        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN gross_profit ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
        """
        df_org = query_df_safe(client, sql, None, "Org Summary")
        if not df_org.empty:
            render_summary_metrics(df_org.iloc[0])


def render_fytd_me_section(client: bigquery.Client, login_email: str) -> None:
    st.subheader("👤 年度累計（FYTD）｜個人サマリー")
    if st.button("自分の成績を読み込む", key="btn_me_load"):
        sql = f"""
            WITH today_info AS (
              SELECT
                CURRENT_DATE('Asia/Tokyo') AS today,
                DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 YEAR) AS py_today,
                (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                    - CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END) AS current_fy
            )
            SELECT
              SUM(CASE WHEN fiscal_year = current_fy THEN sales_amount ELSE 0 END) AS sales_amount_fytd,
              SUM(CASE WHEN fiscal_year = current_fy THEN gross_profit ELSE 0 END) AS gross_profit_fytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN sales_amount ELSE 0 END) AS sales_amount_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 AND sales_date <= py_today THEN gross_profit ELSE 0 END) AS gross_profit_py_ytd,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN sales_amount ELSE 0 END) AS sales_amount_py_total,
              SUM(CASE WHEN fiscal_year = current_fy - 1 THEN gross_profit ELSE 0 END) AS gross_profit_py_total
            FROM `{VIEW_UNIFIED}`
            CROSS JOIN today_info
            WHERE login_email = @login_email
        """
        df_me = query_df_safe(client, sql, {"login_email": login_email}, "Me Summary")
        if not df_me.empty:
            render_summary_metrics(df_me.iloc[0])


def render_yoy_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
    st.subheader("📊 年間 YoY ランキング（成分・YJベース）")

    if "yoy_mode" not in st.session_state:
        st.session_state.yoy_mode = None
    if "yoy_df" not in st.session_state:
        st.session_state.yoy_df = pd.DataFrame()

    c1, c2, c3 = st.columns(3)

    def load_yj_data(mode_name: str, view_name: str) -> None:
        st.session_state.yoy_mode = mode_name
        where_clause = "" if is_admin else "WHERE login_email = @login_email"
        params = None if is_admin else {"login_email": login_email}
        sql = (
            "SELECT login_email, yj_code, product_name, sales_amount, py_sales_amount, sales_diff_yoy "
            f"FROM `{view_name}` {where_clause} LIMIT 100"
        )
        st.session_state.yoy_df = query_df_safe(client, sql, params, mode_name)

    with c1:
        if st.button("📉 下落幅ワースト", use_container_width=True):
            load_yj_data("ワースト", VIEW_YOY_BOTTOM)
    with c2:
        if st.button("📈 上昇幅ベスト", use_container_width=True):
            load_yj_data("ベスト", VIEW_YOY_TOP)
    with c3:
        if st.button("🆕 新規/比較不能", use_container_width=True):
            load_yj_data("新規", VIEW_YOY_UNCOMP)

    if not st.session_state.yoy_df.empty:
        df = st.session_state.yoy_df.copy()
        df_disp = df.drop(columns=["login_email"], errors="ignore").rename(
            columns={
                "yj_code": "YJコード",
                "product_name": "代表商品名(成分)",
                "sales_amount": "今期売上",
                "py_sales_amount": "前期売上",
                "sales_diff_yoy": "前年比差額",
            }
        )
        df_disp = df_disp.fillna(0)
        df_disp = df_disp[["YJコード", "代表商品名(成分)", "今期売上", "前期売上", "前年比差額"]]

        st.markdown(f"#### 🏆 第一階層：成分（YJ）{st.session_state.yoy_mode} ランキング")
        st.dataframe(
            df_disp.style.format(
                {
                    "今期売上": "¥{:,.0f}",
                    "前期売上": "¥{:,.0f}",
                    "前年比差額": "¥{:,.0f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.divider()
        st.markdown("#### 🔍 第二階層：成分の「得意先別」内訳")

        # 重複YJコードがあっても先頭1件を採用し、辞書上書きを防止
        yj_master = df_disp.drop_duplicates(subset=["YJコード"], keep="first").copy()
        yj_options = {
            row["YJコード"]: f"{row['代表商品名(成分)']} (差額: ¥{row['前年比差額']:,.0f})"
            for _, row in yj_master.iterrows()
        }
        selected_yj = st.selectbox(
            "詳細を見たい成分（YJ）を選択してください",
            options=list(yj_options.keys()),
            format_func=lambda x: yj_options[x],
        )

        if selected_yj:
            where_ext = "" if is_admin else "AND login_email = @login_email"
            params: Dict[str, Any] = {"yj": selected_yj}
            if not is_admin:
                params["login_email"] = login_email

            sort_order = "ASC" if st.session_state.yoy_mode == "ワースト" else "DESC"

            sql_drill = f"""
                WITH fy_cust AS (
                    SELECT
                        customer_name,
                        SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                            - (CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END))
                            THEN sales_amount ELSE 0 END) AS ty_sales,
                        SUM(CASE WHEN fiscal_year = (EXTRACT(YEAR FROM CURRENT_DATE('Asia/Tokyo'))
                            - (CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE('Asia/Tokyo')) < 4 THEN 1 ELSE 0 END)) - 1
                            THEN sales_amount ELSE 0 END) AS py_sales
                    FROM `{VIEW_UNIFIED}`
                    WHERE yj_code = @yj {where_ext}
                    GROUP BY customer_name
                )
                SELECT
                    customer_name AS `得意先名`,
                    ty_sales AS `今期売上`,
                    py_sales AS `前期売上`,
                    (ty_sales - py_sales) AS `前年比差額`
                FROM fy_cust
                WHERE (ty_sales - py_sales) != 0 OR ty_sales > 0
                ORDER BY `前年比差額` {sort_order}
                LIMIT 50
            """
            df_drill = query_df_safe(client, sql_drill, params, "YJ Drilldown")
            if not df_drill.empty:
                st.dataframe(
                    df_drill.fillna(0).style.format(
                        {
                            "今期売上": "¥{:,.0f}",
                            "前期売上": "¥{:,.0f}",
                            "前年比差額": "¥{:,.0f}",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("この成分の得意先内訳データが見つかりません。")


def render_new_deliveries_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
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
            st.dataframe(
                df_new.style.format({"売上": "¥{:,.0f}", "粗利": "¥{:,.0f}"}),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("新規納品データがありません。")


def render_adoption_alerts_section(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
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
        df_alerts["担当者名"] = df_alerts["担当者名"].fillna("未設定")
        col1, col2 = st.columns(2)
        with col1:
            selected_status = st.multiselect(
                "🎯 ステータスで絞り込み",
                options=df_alerts["ステータス"].unique(),
                default=[s for s in df_alerts["ステータス"].unique() if "🟡" in s or "🔴" in s],
            )
        with col2:
            all_staffs = sorted(df_alerts["担当者名"].unique().tolist())
            selected_staffs = st.multiselect("👤 担当者で絞り込み", options=all_staffs, default=[])

        df_display = df_alerts.copy()
        if selected_status:
            df_display = df_display[df_display["ステータス"].isin(selected_status)]
        if selected_staffs:
            df_display = df_display[df_display["担当者名"].isin(selected_staffs)]

        if not df_display.empty:
            for col in ["今期売上", "前期売上", "売上差額"]:
                df_display[col] = pd.to_numeric(df_display[col], errors="coerce").fillna(0)
            st.dataframe(
                df_display.style.format(
                    {
                        "今期売上": "¥{:,.0f}",
                        "前期売上": "¥{:,.0f}",
                        "売上差額": "¥{:,.0f}",
                        "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else "",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("選択された条件に一致するアイテムはありません。")
    else:
        st.info("現在、アラート対象のアイテムはありません。")


@st.cache_data(ttl=300)
def fetch_cached_customers(_client: bigquery.Client, login_email: str, is_admin: bool) -> pd.DataFrame:
    if is_admin:
        sql = (
            f"SELECT DISTINCT customer_code, customer_name "
            f"FROM `{VIEW_UNIFIED}` WHERE customer_name IS NOT NULL"
        )
        return query_df_safe(_client, sql, None, "Cached Customers")

    sql = f"""
        SELECT DISTINCT customer_code, customer_name
        FROM `{VIEW_UNIFIED}`
        WHERE login_email = @login_email
          AND customer_name IS NOT NULL
    """
    return query_df_safe(_client, sql, {"login_email": login_email}, "Cached Customers")


def render_customer_drilldown(client: bigquery.Client, login_email: str, is_admin: bool) -> None:
    st.subheader("🎯 担当先ドリルダウン ＆ 提案（Reco）")
    df_cust = fetch_cached_customers(client, login_email, is_admin)
    if df_cust.empty:
        st.info("表示できる得意先データがありません。")
        return

    search_term = st.text_input("🔍 得意先名で検索（一部入力）", placeholder="例：古賀")
    filtered_df = (
        df_cust[df_cust["customer_name"].str.contains(search_term, na=False)]
        if search_term
        else df_cust
    )
    if filtered_df.empty:
        st.info("検索条件に一致する得意先がありません。")
        return

    opts = {
        row["customer_code"]: f"{row['customer_code']} : {row['customer_name']}"
        for _, row in filtered_df.iterrows()
    }
    sel = st.selectbox("得意先を選択", options=list(opts.keys()), format_func=lambda x: opts[x])
    if not sel:
        return

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
        for col in ["今期売上", "前期売上"]:
            df_adopt[col] = pd.to_numeric(df_adopt[col], errors="coerce").fillna(0)
        st.dataframe(
            df_adopt.style.format(
                {
                    "今期売上": "¥{:,.0f}",
                    "前期売上": "¥{:,.0f}",
                    "最終購入日": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else "",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("この得意先の採用データはありません。")

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("##### 💡 AI 推奨提案商品（Reco）")
    sql_rec = f"""
        SELECT *
        FROM `{VIEW_RECOMMEND}`
        WHERE customer_code = @c
        ORDER BY priority_rank ASC
        LIMIT 10
    """
    df_rec = query_df_safe(client, sql_rec, {"c": sel}, "Recommendation")
    if not df_rec.empty:
        df_disp = df_rec[["priority_rank", "recommend_product", "manufacturer"]].rename(
            columns={"priority_rank": "順位", "recommend_product": "推奨商品", "manufacturer": "メーカー"}
        )
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
    else:
        st.info("現在、この得意先への推奨商品はありません。")


# -----------------------------
# 5. Main Loop
# -----------------------------

def main() -> None:
    set_page()
    client = setup_bigquery_client()

    with st.sidebar:
        st.header("🔑 ログイン")
        login_id = st.text_input("ログインID (メールアドレス)")
        login_pw = st.text_input("パスコード (携帯下4桁)", type="password")

        st.divider()
        st.session_state.use_bqstorage = st.checkbox("高速読込 (Storage API)", value=True)

        if st.button("📡 通信ヘルスチェック"):
            try:
                client.query("SELECT 1").result(timeout=10)
                st.success("BigQuery 接続正常")
            except Exception:
                st.error("接続エラー")

        if st.button("🧹 キャッシュクリア"):
            st.cache_data.clear()

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
