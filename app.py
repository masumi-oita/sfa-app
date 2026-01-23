import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "salesdb-479915"
VIEW_ADMIN = "salesdb-479915.sales_data.v_entry_admin_monthly"
VIEW_SALES_ME = "salesdb-479915.sales_data.v_entry_sales_monthly_me"

st.set_page_config(
    page_title="SFA 月次サマリー",
    page_icon="📈",
    layout="wide",
)

# =====================================================
# BigQuery Client（Secrets 明示指定）
# =====================================================
def get_bq_client():
    sa_info = dict(st.secrets["gcp_service_account"])
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
    )

@st.cache_data(ttl=600)
def bq_df(sql: str) -> pd.DataFrame:
    client = get_bq_client()
    return client.query(sql).to_dataframe()

# =====================================================
# UI
# =====================================================
st.title("📈 月次サマリー（入口VIEW）")

tab_sales, tab_admin = st.tabs(["🧑‍💼 営業（自分）", "🧑‍💼 管理者（全体）"])

# =====================================================
# 営業（自分）
# =====================================================
with tab_sales:
    st.subheader("🧑‍💼 営業用（月次・自分の得意先のみ）")

    months_df = bq_df(
        f"""
        SELECT DISTINCT month
        FROM `{VIEW_SALES_ME}`
        ORDER BY month DESC
        """
    )

    if months_df.empty:
        st.warning("表示するデータはありません。")
    else:
        month = st.selectbox(
            "対象月",
            months_df["month"].astype(str).tolist(),
        )

        df = bq_df(
            f"""
            SELECT
              month,
              branch_name,
              staff_code,
              staff_name,
              customer_code,
              customer_name,
              sales_amount,
              sales_amount_py,
              sales_amount_yoy_diff,
              sales_amount_yoy_pct,
              is_new_vs_py
            FROM `{VIEW_SALES_ME}`
            WHERE month = DATE('{month}')
            ORDER BY sales_amount DESC
            """
        )

        # KPI
        c1, c2, c3 = st.columns(3)
        c1.metric("売上合計", f"{df['sales_amount'].sum():,.0f}")
        c2.metric("前年差", f"{df['sales_amount_yoy_diff'].sum():,.0f}")
        c3.metric("新規得意先数", int(df["is_new_vs_py"].sum()))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

# =====================================================
# 管理者（全体）
# =====================================================
with tab_admin:
    st.subheader("🧑‍💼 管理者用（月次・全体）")

    months_df = bq_df(
        f"""
        SELECT DISTINCT month
        FROM `{VIEW_ADMIN}`
        ORDER BY month DESC
        """
    )

    if months_df.empty:
        st.warning("表示するデータはありません。")
    else:
        month = st.selectbox(
            "対象月（全体）",
            months_df["month"].astype(str).tolist(),
            key="admin_month",
        )

        df = bq_df(
            f"""
            SELECT
              month,
              branch_name,
              staff_code,
              staff_name,
              customer_code,
              customer_name,
              sales_amount,
              sales_amount_py,
              sales_amount_yoy_diff,
              sales_amount_yoy_pct,
              is_new_vs_py
            FROM `{VIEW_ADMIN}`
            WHERE month = DATE('{month}')
            ORDER BY sales_amount DESC
            """
        )

        # KPI
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("売上合計", f"{df['sales_amount'].sum():,.0f}")
        c2.metric("前年差", f"{df['sales_amount_yoy_diff'].sum():,.0f}")
        c3.metric("得意先数", df["customer_code"].nunique())
        c4.metric("新規得意先数", int(df["is_new_vs_py"].sum()))

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

# =====================================================
# FOOTER
# =====================================================
st.caption("Data Source: BigQuery / View-based SFA Architecture")
