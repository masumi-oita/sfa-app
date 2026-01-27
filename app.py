# ============================================================
# app.py  管理者ダッシュボード（OS v1.4.5 完成形）
#  - 日本語UI
#  - 担当者氏名表示（email→氏名）
#  - FYTD構造 → FYTD MoM → 当月YoY → ドリル
# ============================================================

from __future__ import annotations
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from typing import Dict, Any, Optional
import re

# --------------------
# 基本設定
# --------------------
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"

BQ = bigquery.Client(project=PROJECT_ID)

# --------------------
# VIEW定義
# --------------------
V_SYS_MONTH = f"`{PROJECT_ID}.{DATASET}.v_sys_current_month`"

V_ADMIN_ORG_FYTD = f"`{PROJECT_ID}.{DATASET}.v_admin_org_fytd_summary_scoped`"
V_ADMIN_FYTD_MOM_TOP = f"`{PROJECT_ID}.{DATASET}.v_admin_customer_fytd_mom_top_named_scoped`"
V_ADMIN_FYTD_MOM_BOTTOM = f"`{PROJECT_ID}.{DATASET}.v_admin_customer_fytd_mom_bottom_named_scoped`"

V_YOY_TOP = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_top_current_month`"
V_YOY_BOTTOM = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_bottom_current_month`"
V_YOY_INVALID = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_uncomparable_current_month`"

V_FACT = f"`{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily`"
V_STAFF_NAME = f"`{PROJECT_ID}.{DATASET}.v_staff_email_name`"
DIM_ROLE = f"`{PROJECT_ID}.{DATASET}.dim_staff_role`"

# --------------------
# 共通関数
# --------------------
@st.cache_data(ttl=300)
def qdf(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING", v)
            for k, v in (params or {}).items()
        ]
    )
    try:
        return BQ.query(sql, job_config=job_config).result().to_dataframe()
    except Exception as e:
        st.warning(f"Query Error: {e}")
        return pd.DataFrame()


def yen(x):
    if pd.isna(x): return ""
    return f"¥{int(x):,}"


def pct(x):
    if pd.isna(x): return ""
    return f"{x*100:.1f}%"


def get_user_email() -> str:
    if "user_email" in st.query_params:
        return st.query_params["user_email"].lower().strip()
    return st.text_input("ログインメール（user_email）").lower().strip()


def get_staff_name(email: str) -> str:
    df = qdf(
        f"""
        SELECT staff_name_norm
        FROM {V_STAFF_NAME}
        WHERE login_email = @email
        LIMIT 1
        """,
        {"email": email}
    )
    if df.empty:
        return email
    return df.iloc[0]["staff_name_norm"]


def get_scope(email: str) -> Dict[str, Any]:
    df = qdf(
        f"""
        SELECT role_tier, area_name, scope_type, scope_branches
        FROM {DIM_ROLE}
        WHERE LOWER(login_email)=@email
        """,
        {"email": email}
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# --------------------
# UI開始
# --------------------
st.set_page_config(page_title="管理者ダッシュボード", layout="wide")
st.title("📊 管理者ダッシュボード（分析用）")

user_email = get_user_email()
if not user_email:
    st.stop()

staff_name = get_staff_name(user_email)
scope = get_scope(user_email)

# --------------------
# ヘッダー
# --------------------
sys = qdf(f"SELECT * FROM {V_SYS_MONTH} LIMIT 1")
current_month = str(sys.iloc[0]["current_month"]) if not sys.empty else "-"

c1, c2, c3, c4 = st.columns(4)
c1.metric("対象月", current_month)
c2.metric("担当者", staff_name)
c3.metric("ロール", scope.get("role_tier", "-"))
c4.metric("スコープ", scope.get("area_name", "-"))

st.divider()

# ============================================================
# A) FYTD 構造サマリー
# ============================================================
with st.expander("① 年度累計（FYTD）構造サマリー", expanded=True):
    df = qdf(
        f"""
        SELECT *
        FROM {V_ADMIN_ORG_FYTD}
        WHERE viewer_email=@email
        """,
        {"email": user_email}
    )

    if df.empty:
        st.info("FYTDデータはまだありません")
    else:
        r = df.iloc[0]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("売上（FYTD）", yen(r["sales_amount_fytd"]))
        k2.metric("粗利（FYTD）", yen(r["gross_profit_fytd"]))
        k3.metric("粗利率（FYTD）", pct(r["gross_profit_rate_fytd"]))
        k4.metric("前年差（売上）", yen(r.get("sales_diff_fytd")))

# ============================================================
# B) FYTD MoM（流れ）
# ============================================================
with st.expander("② FYTD 前月差（MoM）ランキング", expanded=True):
    l, r = st.columns(2)

    with l:
        st.subheader("📉 下落")
        df = qdf(
            f"""
            SELECT 得意先名, 支店名,
                   sales_amount_fytd AS FYTD売上,
                   gross_profit_fytd AS FYTD粗利,
                   sales_diff_mom AS 前月差_売上,
                   gross_profit_diff_mom AS 前月差_粗利
            FROM {V_ADMIN_FYTD_MOM_BOTTOM}
            WHERE viewer_email=@email
            ORDER BY sales_diff_mom ASC
            LIMIT 20
            """,
            {"email": user_email}
        )
        st.dataframe(df, use_container_width=True)

    with r:
        st.subheader("📈 伸長")
        df = qdf(
            f"""
            SELECT 得意先名, 支店名,
                   sales_amount_fytd AS FYTD売上,
                   gross_profit_fytd AS FYTD粗利,
                   sales_diff_mom AS 前月差_売上,
                   gross_profit_diff_mom AS 前月差_粗利
            FROM {V_ADMIN_FYTD_MOM_TOP}
            WHERE viewer_email=@email
            ORDER BY sales_diff_mom DESC
            LIMIT 20
            """,
            {"email": user_email}
        )
        st.dataframe(df, use_container_width=True)

# ============================================================
# C) 当月 YoY
# ============================================================
with st.expander("③ 当月 YoY（前年比較）", expanded=True):
    tabs = st.tabs(["下落", "伸長", "比較不能"])

    with tabs[0]:
        df = qdf(f"SELECT * FROM {V_YOY_BOTTOM}")
        st.dataframe(df, use_container_width=True)

    with tabs[1]:
        df = qdf(f"SELECT * FROM {V_YOY_TOP}")
        st.dataframe(df, use_container_width=True)

    with tabs[2]:
        df = qdf(f"SELECT * FROM {V_YOY_INVALID}")
        st.dataframe(df, use_container_width=True)

# ============================================================
# D) ドリル
# ============================================================
with st.expander("④ ドリル（得意先 → 日次）", expanded=True):
    keyword = st.text_input("得意先名（部分一致）")
    if keyword:
        cand = qdf(
            f"""
            SELECT DISTINCT customer_code, customer_name
            FROM {V_FACT}
            WHERE login_email=@email
              AND customer_name LIKE CONCAT('%', @kw, '%')
            LIMIT 50
            """,
            {"email": user_email, "kw": keyword}
        )
        if not cand.empty:
            pick = st.selectbox(
                "得意先選択",
                cand.apply(lambda r: f"{r.customer_name} ({r.customer_code})", axis=1)
            )
            code = re.search(r"\((.+?)\)", pick).group(1)

            detail = qdf(
                f"""
                SELECT sales_date AS 日付,
                       item_name AS 商品名,
                       quantity AS 数量,
                       sales_amount AS 売上,
                       gross_profit AS 粗利
                FROM {V_FACT}
                WHERE login_email=@email
                  AND customer_code=@code
                  AND month=DATE(@m)
                ORDER BY sales_date DESC
                """,
                {"email": user_email, "code": code, "m": current_month}
            )
            st.dataframe(detail, use_container_width=True)
