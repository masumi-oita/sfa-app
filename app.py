# app.py
# ============================================================
# 新規採用ドリル（担当者入口）v0
# - BigQuery から 2026-01 固定のビュー/テーブルを参照
# - ドリル：担当者 → 得意先（月次） → 品目（月次） → 日次
# ============================================================

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account


# ----------------------------
# CONFIG
# ----------------------------
PROJECT_ID = "salesdb-479915"
DATASET_ID = "sales_data"

# 2026-01 固定（現状の月別ビュー）
VIEW_CUSTOMER_MONTHLY = f"`{PROJECT_ID}.{DATASET_ID}.v_staff_customer_new_adoption_monthly_2026_01`"
VIEW_ITEM_MONTHLY     = f"`{PROJECT_ID}.{DATASET_ID}.v_staff_customer_item_new_adoption_monthly_2026_01`"
VIEW_DAILY            = f"`{PROJECT_ID}.{DATASET_ID}.v_staff_customer_item_new_adoption_daily_2026_01`"

DEFAULT_MONTH = "2026-01"


# ----------------------------
# BigQuery Client
# ----------------------------
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    # Streamlit secrets: st.secrets["gcp_service_account"] を想定
    # private_key は PEM 文字列のみ（JSONを混ぜない）
    sa_info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(project=PROJECT_ID, credentials=creds, location="asia-northeast1")


def qparams(**kwargs):
    # BigQuery query parameters helper
    params = []
    for k, v in kwargs.items():
        if isinstance(v, int):
            params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        else:
            params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return params


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    df = client.query(sql, job_config=job_config).result().to_dataframe()
    return df


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="新規採用ドリル（担当者）", layout="wide")
st.title("新規採用ドリル（担当者入口）")

st.caption(
    "現状は 2026-01 固定の月別ビューを参照しています。"
    "（将来は all_months 化または月別テーブルの自動生成に拡張）"
)

# Sidebar controls
st.sidebar.header("フィルタ")

# いまは月固定だが、UI上は選べる形だけ用意（将来拡張前提）
month = st.sidebar.selectbox("月", options=[DEFAULT_MONTH], index=0)

# 担当者候補：月次ビューから引く（軽い）
staff_list_sql = f"""
SELECT DISTINCT staff_code, staff_name, branch_name
FROM {VIEW_CUSTOMER_MONTHLY}
WHERE month = @month
ORDER BY branch_name, staff_name
"""
staff_df = run_query(staff_list_sql, qparams(month=month))

if staff_df.empty:
    st.error("この月のデータがありません。")
    st.stop()

# 表示用ラベル
staff_df["label"] = staff_df["branch_name"].fillna("") + " / " + staff_df["staff_name"].fillna("") + " (" + staff_df["staff_code"].astype(str) + ")"
selected_label = st.sidebar.selectbox("担当者", options=staff_df["label"].tolist(), index=0)

sel_row = staff_df.loc[staff_df["label"] == selected_label].iloc[0]
staff_code = str(sel_row["staff_code"])
branch_name = str(sel_row["branch_name"])
staff_name = str(sel_row["staff_name"])

st.subheader(f"担当者：{staff_name}（{branch_name} / {staff_code}）")

# ============================================================
# 1) 得意先（月次）一覧
# ============================================================
cust_sql = f"""
SELECT
  month, staff_code, staff_name, branch_name,
  customer_code, customer_name,
  realized_items, sales_sum, gross_profit, gross_margin,
  first_sales_date_in_month
FROM {VIEW_CUSTOMER_MONTHLY}
WHERE month = @month
  AND staff_code = @staff_code
  AND branch_name = @branch_name
ORDER BY gross_profit DESC
"""
cust_df = run_query(cust_sql, qparams(month=month, staff_code=staff_code, branch_name=branch_name))

left, right = st.columns([1.1, 1.0], gap="large")

with left:
    st.markdown("### ① 得意先（新規採用・月次）")
    if cust_df.empty:
        st.info("該当なし")
        st.stop()

    # 見やすい列だけ
    show_cust = cust_df.copy()
    show_cust["gross_margin"] = show_cust["gross_margin"].astype(float).round(4)
    st.dataframe(
        show_cust[[
            "customer_code", "customer_name",
            "realized_items", "sales_sum", "gross_profit", "gross_margin",
            "first_sales_date_in_month"
        ]],
        use_container_width=True,
        hide_index=True
    )

    # 得意先選択
    cust_options = (show_cust["customer_code"].astype(str) + " / " + show_cust["customer_name"]).tolist()
    selected_customer = st.selectbox("得意先を選択", options=cust_options, index=0)
    customer_code = selected_customer.split(" / ")[0].strip()


# ============================================================
# 2) 品目（月次）一覧（選択した得意先）
# ============================================================
item_sql = f"""
SELECT
  month, staff_code, staff_name, branch_name,
  customer_code, customer_name,
  yj_code, sales_sum, gross_profit, gross_margin, quantity,
  first_sales_date_in_month
FROM {VIEW_ITEM_MONTHLY}
WHERE month = @month
  AND staff_code = @staff_code
  AND branch_name = @branch_name
  AND customer_code = @customer_code
ORDER BY gross_profit DESC
"""
item_df = run_query(item_sql, qparams(month=month, staff_code=staff_code, branch_name=branch_name, customer_code=customer_code))

with right:
    st.markdown("### ② 品目（YJ・月次）")
    if item_df.empty:
        st.info("この得意先で該当品目なし")
        st.stop()

    show_item = item_df.copy()
    show_item["gross_margin"] = show_item["gross_margin"].astype(float).round(4)
    st.dataframe(
        show_item[[
            "yj_code", "sales_sum", "gross_profit", "gross_margin", "quantity",
            "first_sales_date_in_month"
        ]],
        use_container_width=True,
        hide_index=True
    )

    item_options = show_item["yj_code"].astype(str).tolist()
    selected_yj = st.selectbox("YJコードを選択", options=item_options, index=0)


# ============================================================
# 3) 日次（daily）明細（選択した得意先×品目）
# ============================================================
st.markdown("### ③ 日次（売上明細）")

daily_sql = f"""
SELECT
  month, staff_code, staff_name, branch_name,
  customer_code, customer_name,
  yj_code,
  sales_date, sales_amount, gross_profit, gross_margin, quantity,
  first_sales_date_in_month
FROM {VIEW_DAILY}
WHERE month = @month
  AND staff_code = @staff_code
  AND branch_name = @branch_name
  AND customer_code = @customer_code
  AND yj_code = @yj_code
ORDER BY sales_date
"""
daily_df = run_query(
    daily_sql,
    qparams(month=month, staff_code=staff_code, branch_name=branch_name, customer_code=customer_code, yj_code=selected_yj),
)

if daily_df.empty:
    st.info("日次明細なし（この条件では該当0）")
else:
    show_daily = daily_df.copy()
    show_daily["gross_margin"] = show_daily["gross_margin"].astype(float).round(4)
    st.dataframe(
        show_daily[[
            "sales_date", "sales_amount", "gross_profit", "gross_margin", "quantity",
            "first_sales_date_in_month"
        ]],
        use_container_width=True,
        hide_index=True
    )

    # ざっくり合計も表示
    total_sales = float(show_daily["sales_amount"].sum())
    total_gp = float(show_daily["gross_profit"].sum())
    total_margin = (total_gp / total_sales) if total_sales else 0.0
    c1, c2, c3 = st.columns(3)
    c1.metric("売上合計", f"{total_sales:,.0f}")
    c2.metric("粗利合計", f"{total_gp:,.0f}")
    c3.metric("粗利率", f"{total_margin:.4f}")
