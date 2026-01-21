import streamlit as st
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

# ============================================================
# CONFIG
# ============================================================
PROJECT_ID = "salesdb-479915"
BQ_LOCATION = "asia-northeast1"  # ← 重要（NotFound/Location違い対策）
DATASET = "sales_data"

# いまは 2026-01 固定（月別ビューを参照）
DEFAULT_MONTH = "2026-01"

# 参照ビュー（あなたが作成済みのもの）
VIEW_STAFF_MONTHLY = f"`{PROJECT_ID}.{DATASET}.v_staff_new_adoption_monthly_kpi_v3`"
VIEW_CUSTOMER_MONTHLY = f"`{PROJECT_ID}.{DATASET}.v_staff_customer_new_adoption_monthly_2026_01`"
VIEW_ITEM_MONTHLY = f"`{PROJECT_ID}.{DATASET}.v_staff_customer_item_new_adoption_monthly_2026_01_v3`"
VIEW_DAILY = f"`{PROJECT_ID}.{DATASET}.v_staff_customer_item_new_adoption_daily_2026_01_v2`"

# ============================================================
# STREAMLIT SETTINGS
# ============================================================
st.set_page_config(page_title="新規採用ドリル（担当者入口）", layout="wide")
st.title("新規採用ドリル（担当者入口）")
st.caption("現状は 2026-01 固定の月別ビューを参照（将来は all_months 化 or 自動生成へ拡張）")

# ============================================================
# BigQuery Client
# ============================================================
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    # Streamlit Cloud secrets:
    # st.secrets["gcp_service_account"] が dict で入り、private_key は PEM だけ（改行つき）
    sa_info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)

def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    client = get_bq_client()

    job_config = bigquery.QueryJobConfig()
    if params:
        qparams = []
        for k, v in params.items():
            # いまは month='YYYY-MM' の文字列だけを想定
            qparams.append(bigquery.ScalarQueryParameter(k, "STRING", v))
        job_config.query_parameters = qparams

    # BigQuery Storage API を強制しない（環境で不安定なことがある）
    # db-dtypes は requirements に入れる（後述）
    res = client.query(sql, job_config=job_config).result()
    return res.to_dataframe(create_bqstorage_client=False)

# ============================================================
# FILTERS
# ============================================================
with st.sidebar:
    st.header("フィルタ")
    month = st.selectbox("月", [DEFAULT_MONTH], index=0)

# ============================================================
# 1) STAFF LIST
# ============================================================
st.subheader("① 担当者（月次KPI）")

staff_list_sql = f"""
SELECT
  staff_code,
  staff_name,
  branch_name,
  target_customers,
  realized_customers,
  conversion_customer_rate,
  target_items,
  realized_items,
  conversion_item_rate,
  sales_sum,
  gross_profit,
  gross_margin
FROM {VIEW_STAFF_MONTHLY}
WHERE month = @month
ORDER BY gross_profit DESC
"""

staff_df = run_query(staff_list_sql, {"month": month})

# 表示整形（%系は後で）
st.dataframe(staff_df, use_container_width=True)

if staff_df.empty:
    st.stop()

# 担当者選択（表示名）
staff_options = [
    f"{r['branch_name']} / {int(r['staff_code'])} / {r['staff_name']}"
    for _, r in staff_df.iterrows()
]
staff_sel = st.selectbox("担当者を選択", staff_options)
staff_code = int(staff_sel.split(" / ")[1])

# ============================================================
# 2) CUSTOMER LIST (per staff)
# ============================================================
st.subheader("② 得意先（担当者×月次）")

cust_sql = f"""
SELECT
  month,
  staff_code,
  staff_name,
  branch_name,
  customer_code,
  customer_name,
  realized_items,
  sales_sum,
  gross_profit,
  gross_margin,
  first_sales_date_in_month
FROM {VIEW_CUSTOMER_MONTHLY}
WHERE month = @month
  AND staff_code = @staff_code
ORDER BY gross_profit DESC, sales_sum DESC
"""

cust_df = run_query(cust_sql, {"month": month, "staff_code": str(staff_code)})
st.dataframe(cust_df, use_container_width=True)

if cust_df.empty:
    st.stop()

cust_options = [
    f"{int(r['customer_code'])} / {r['customer_name']}（粗利 {r['gross_profit']:.0f}）"
    for _, r in cust_df.iterrows()
]
cust_sel = st.selectbox("得意先を選択", cust_options)
customer_code = int(cust_sel.split(" / ")[0])

# ============================================================
# 3) ITEM LIST (per staff + customer)  ※品目表示は yj_display
# ============================================================
st.subheader("③ 品目（得意先×担当者×月次）")

item_sql = f"""
SELECT
  month,
  staff_code,
  staff_name,
  branch_name,
  customer_code,
  customer_name,

  yj_code,
  yj_display,
  product_name,
  ingredient_name,
  maker_name,

  sales_sum,
  gross_profit,
  gross_margin,
  quantity,
  first_sales_date_in_month
FROM {VIEW_ITEM_MONTHLY}
WHERE month = @month
  AND staff_code = @staff_code
  AND customer_code = @customer_code
ORDER BY gross_profit DESC, sales_sum DESC
"""

item_df = run_query(
    item_sql,
    {"month": month, "staff_code": str(staff_code), "customer_code": str(customer_code)}
)

# ▼ 重複潰し（同一YJが複数行に出るケース対策）
if not item_df.empty:
    item_df_disp = (
        item_df.sort_values(["gross_profit", "sales_sum"], ascending=[False, False])
        .drop_duplicates(subset=["yj_code"], keep="first")
        .reset_index(drop=True)
    )
else:
    item_df_disp = item_df

# 表示は yj_display を先頭に
show_cols = [
    "yj_display",
    "product_name",
    "ingredient_name",
    "maker_name",
    "sales_sum",
    "gross_profit",
    "gross_margin",
    "quantity",
    "first_sales_date_in_month",
]
st.dataframe(item_df_disp[show_cols] if not item_df_disp.empty else item_df_disp, use_container_width=True)

if item_df_disp.empty:
    st.stop()

# プルダウン（表示= yj_display / 内部= yj_code）
labels = item_df_disp["yj_display"].tolist()
label_sel = st.selectbox("品目を選択（表示名）", labels)
selected_yj = item_df_disp.loc[item_df_disp["yj_display"] == label_sel, "yj_code"].iloc[0]

# ============================================================
# 4) DAILY DRILL (per staff + customer + item)
# ============================================================
st.subheader("④ 日次ドリル（担当者×得意先×品目）")

daily_sql = f"""
SELECT
  month,
  staff_code,
  staff_name,
  branch_name,
  customer_code,
  customer_name,
  yj_code,
  yj_display,
  sales_date,
  sales_amount,
  gross_profit,
  gross_margin,
  quantity,
  first_sales_date_in_month
FROM {VIEW_DAILY}
WHERE month = @month
  AND staff_code = @staff_code
  AND customer_code = @customer_code
  AND yj_code = @yj_code
ORDER BY sales_date ASC
"""

daily_df = run_query(
    daily_sql,
    {
        "month": month,
        "staff_code": str(staff_code),
        "customer_code": str(customer_code),
        "yj_code": str(selected_yj),
    },
)

st.dataframe(daily_df, use_container_width=True)

# ざっくり集計
if not daily_df.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("日数", f"{daily_df['sales_date'].nunique():,}")
    col2.metric("売上合計", f"{daily_df['sales_amount'].sum():,.0f}")
    col3.metric("粗利合計", f"{daily_df['gross_profit'].sum():,.0f}")

st.success("OK：担当者 → 得意先 → 品目（yj_display） → 日次 のドリルが動作しています。")
