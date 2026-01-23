# app.py
import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT = "salesdb-479915"
DATASET = "sales_data"

VIEW_SALES_ME = f"`{PROJECT}.{DATASET}.v_entry_sales_monthly_me`"
VIEW_ADMIN    = f"`{PROJECT}.{DATASET}.v_entry_admin_monthly`"

@st.cache_data(ttl=300)
def bq_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING" if isinstance(v, str) else "DATE", v)
            for k, v in (params or {}).items()
        ]
    ) if params else None
    return client.query(sql, job_config=job_config).to_dataframe()

st.set_page_config(page_title="SFA Monthly", layout="wide")
st.title("📈 月次サマリー（入口VIEW）")

tab_sales, tab_admin = st.tabs(["営業（自分）", "管理者（全体）"])

# -------------------------
# 営業（自分）
# -------------------------
with tab_sales:
    # monthリスト取得
    months = bq_df(f"SELECT DISTINCT month FROM {VIEW_SALES_ME} ORDER BY month DESC")
    if months.empty:
        st.warning("データがありません。VIEWまたは対象月を確認してください。")
        st.stop()

    month = st.selectbox("対象月", months["month"].tolist(), index=0)

    # 本体
    df = bq_df(
        f"""
        SELECT
          month, branch_name, staff_code, staff_name, customer_code, customer_name,
          sales_amount, sales_amount_py, sales_amount_yoy_diff, sales_amount_yoy_pct, is_new_vs_py
        FROM {VIEW_SALES_ME}
        WHERE month = @month
        """,
        {"month": month},
    )

    # KPI
    col1, col2, col3, col4 = st.columns(4)
    total_sales = float(df["sales_amount"].fillna(0).sum())
    total_py    = float(df["sales_amount_py"].fillna(0).sum())
    yoy_diff    = total_sales - total_py
    yoy_pct     = (yoy_diff / total_py) if total_py != 0 else None

    col1.metric("売上", f"{total_sales:,.0f}", delta=f"{yoy_diff:,.0f}")
    col2.metric("前年差%", "" if yoy_pct is None else f"{yoy_pct*100:,.1f}%")
    col3.metric("得意先数", f"{df['customer_code'].nunique():,}")
    col4.metric("PYゼロ得意先（新規扱い）", f"{int(df['is_new_vs_py'].fillna(0).sum()):,}")

    # ランキング切替
    sort_key = st.radio(
        "ランキング軸",
        ["売上", "前年差増減（額）", "新規（PYゼロ）優先"],
        horizontal=True,
    )

    df_view = df.copy()
    if sort_key == "売上":
        df_view = df_view.sort_values("sales_amount", ascending=False)
    elif sort_key == "前年差増減（額）":
        df_view = df_view.sort_values("sales_amount_yoy_diff", ascending=False)
    else:
        # 新規を上に、次に売上
        df_view["is_new_vs_py"] = df_view["is_new_vs_py"].fillna(0).astype(int)
        df_view = df_view.sort_values(["is_new_vs_py", "sales_amount"], ascending=[False, False])

    st.subheader("🏁 得意先ランキング")
    st.dataframe(
        df_view[
            ["branch_name","staff_name","customer_code","customer_name",
             "sales_amount","sales_amount_py","sales_amount_yoy_diff","sales_amount_yoy_pct","is_new_vs_py"]
        ],
        use_container_width=True,
        hide_index=True,
    )

    # ドリル（得意先選択）
    st.divider()
    st.subheader("🔍 ドリルダウン（得意先）")
    cust = st.selectbox("得意先", df_view["customer_name"].unique().tolist())
    cust_code = df_view.loc[df_view["customer_name"] == cust, "customer_code"].iloc[0]

    st.write(f"選択：**{cust}**（{cust_code}）")

    # ※ここは次ステップで「得意先×品目」ビューに繋ぐ（v_sales_fact_fy_norm など）
    # いったん月次入口の行だけ詳細表示
    st.dataframe(
        df_view[df_view["customer_code"] == cust_code],
        use_container_width=True,
        hide_index=True,
    )

# -------------------------
# 管理者（全体）
# -------------------------
with tab_admin:
    months = bq_df(f"SELECT DISTINCT month FROM {VIEW_ADMIN} ORDER BY month DESC")
    month = st.selectbox("対象月（全体）", months["month"].tolist(), index=0, key="admin_month")

    df = bq_df(
        f"""
        SELECT *
        FROM {VIEW_ADMIN}
        WHERE month = @month
        """,
        {"month": month},
    )

    # ざっくり KPI
    c1, c2, c3 = st.columns(3)
    c1.metric("全体 売上", f"{float(df['sales_amount'].fillna(0).sum()):,.0f}")
    c2.metric("全体 粗利", f"{float(df['gross_profit'].fillna(0).sum()):,.0f}")
    c3.metric("得意先数", f"{df['customer_code'].nunique():,}")

    st.subheader("📋 管理者一覧（支店→担当→得意先）")
    st.dataframe(df, use_container_width=True, hide_index=True)
