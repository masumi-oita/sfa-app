import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery

# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"
VIEW_JP = f"{PROJECT_ID}.{DATASET}.jp_new_deliveries_realized_staff_period_summary"

PERIOD_LABELS = {
    "day": "日（day）",
    "week": "週（week）",
    "month": "月（month）",
    "fy": "年度（fy）",
}

# 表示用の期間ラベル（JP VIEW側は「期間」列）
PERIOD_VALUE_MAP = {
    "日（day）": "day",
    "週（week）": "week",
    "月（month）": "month",
    "年度（fy）": "fy",
}

# -----------------------------
# BigQuery helper
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_df(sql: str) -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)
    return client.query(sql).to_dataframe()

def sql_quote(s: str) -> str:
    # BigQuery用の簡易エスケープ（単一引用符のみ）
    return "'" + s.replace("'", "\\'") + "'"

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="新規納品（Realized）", layout="wide")

st.title("新規納品（Realized）— 最新担当者別")
st.caption("定義：売上事実 × 直近365日未売上（得意先×YJ） → 新規納品として計上（OS②）")

# ---------
# Filters
# ---------
with st.sidebar:
    st.header("フィルタ")

    period_label = st.selectbox("期間粒度", list(PERIOD_VALUE_MAP.keys()), index=1)  # default: 週
    period = PERIOD_VALUE_MAP[period_label]

    # 期間日の候補（該当periodの最大日付から過去N件）
    n_dates = st.slider("表示する期間数（最新から）", 5, 60, 20, step=5)

    # 担当者コードの候補（まず period = 選択値 で候補を取る）
    staff_query = f"""
    #standardSQL
    SELECT DISTINCT
      `担当者コード`,
      `担当者名`
    FROM `{VIEW_JP}`
    WHERE `期間` = {sql_quote(period)}
      AND `担当者コード` IS NOT NULL
      AND `担当者コード` <> '(UNSET)'
    ORDER BY `担当者コード`
    """
    staff_df = fetch_df(staff_query)

    staff_options = ["（全員）"] + [
        f"{row['担当者コード']}｜{row['担当者名']}"
        for _, row in staff_df.iterrows()
    ]
    staff_sel = st.selectbox("担当者（コードで絞り込み）", staff_options, index=0)

    only_new_delivery = st.checkbox("新規納品がある行だけ表示", value=True)

    st.divider()
    st.subheader("ランキング設定")
    top_n = st.slider("Top N", 5, 50, 20, step=5)
    metric = st.selectbox(
        "ランキング指標",
        ["新規納品_得意先数", "新規納品_得意先品目数", "売上金額", "粗利額", "粗利率"],
        index=1,
    )

# ---------
# Build base query
# ---------
where = [f"`期間` = {sql_quote(period)}"]

if staff_sel != "（全員）":
    staff_code = staff_sel.split("｜", 1)[0].strip()
    where.append(f"`担当者コード` = {sql_quote(staff_code)}")

if only_new_delivery:
    # 新規納品_行数 > 0 を条件に（集計済みなのでこれが一番早い）
    where.append("`新規納品_行数` > 0")

where_sql = " AND ".join(where)

# 期間日（候補）
dates_query = f"""
#standardSQL
SELECT DISTINCT `期間日`
FROM `{VIEW_JP}`
WHERE {where_sql}
ORDER BY `期間日` DESC
LIMIT {int(n_dates)}
"""
dates_df = fetch_df(dates_query)

if dates_df.empty:
    st.warning("該当データがありません。フィルタ条件を変えてください。")
    st.stop()

date_list = dates_df["期間日"].tolist()
selected_date = st.selectbox("期間日（選択）", date_list, index=0)

# メイン取得
main_query = f"""
#standardSQL
SELECT
  `期間`,
  `期間日`,
  `支店名`,
  `担当者コード`,
  `担当者名`,
  `新規納品_得意先数`,
  `新規納品_得意先品目数`,
  `新規納品_品目数`,
  `新規納品_行数`,
  `売上金額`,
  `粗利額`,
  `粗利率`,
  `数量`
FROM `{VIEW_JP}`
WHERE {where_sql}
  AND `期間日` = DATE({sql_quote(str(selected_date))})
"""
df = fetch_df(main_query)

# ---------
# KPI row
# ---------
kpi_cols = st.columns(5)
kpi_cols[0].metric("新規納品 得意先数", int(df["新規納品_得意先数"].sum()))
kpi_cols[1].metric("新規納品 得意先品目数", int(df["新規納品_得意先品目数"].sum()))
kpi_cols[2].metric("売上金額（合計）", f"{df['売上金額'].sum():,.0f}")
kpi_cols[3].metric("粗利額（合計）", f"{df['粗利額'].sum():,.0f}")
kpi_cols[4].metric("粗利率（合計）", f"{(df['粗利額'].sum() / df['売上金額'].sum() if df['売上金額'].sum() else 0):.2%}")

st.divider()

# ---------
# Ranking
# ---------
st.subheader(f"ランキング（{metric} / Top {top_n}）")

rank_df = df.copy()
# 粗利率は小数なので表示整形用に別列
if metric == "粗利率":
    rank_df = rank_df.sort_values("粗利率", ascending=False).head(top_n)
else:
    rank_df = rank_df.sort_values(metric, ascending=False).head(top_n)

# 表示列は見やすく
show_cols = [
    "支店名",
    "担当者コード",
    "担当者名",
    "新規納品_得意先数",
    "新規納品_得意先品目数",
    "新規納品_品目数",
    "新規納品_行数",
    "売上金額",
    "粗利額",
    "粗利率",
    "数量",
]
st.dataframe(rank_df[show_cols], use_container_width=True, hide_index=True)

# ---------
# Drilldown: staff detail -> customer/item fact
# ---------
st.subheader("ドリルダウン（担当者 → 得意先×品目）")

# ドリル先：担当者コードを選択
staff_drill_options = rank_df[["担当者コード", "担当者名"]].drop_duplicates()
staff_drill_label = [
    f"{r['担当者コード']}｜{r['担当者名']}" for _, r in staff_drill_options.iterrows()
]
if not staff_drill_label:
    st.info("ランキングに担当者がありません。フィルタを緩めてください。")
    st.stop()

staff_drill_sel = st.selectbox("ドリル対象 担当者", staff_drill_label, index=0)
staff_drill_code = staff_drill_sel.split("｜", 1)[0].strip()

# ここは “事実VIEW” に落として、新規納品の得意先×YJを出す（集計VIEWだけでは品目ドリルできない）
# ※ staffは m_customer_master をJOINして最新担当者で揃える
detail_query = f"""
#standardSQL
WITH cm AS (
  SELECT
    CAST(`得意先コード` AS INT64) AS customer_code_i,
    `担当者コード` AS staff_code,
    `担当者名` AS staff_name,
    `支店名` AS branch_name
  FROM `{PROJECT_ID}.{DATASET}.m_customer_master`
),
fact AS (
  SELECT
    sales_date,
    week_start,
    month_start,
    fy,
    customer_code,
    customer_name,
    yj_code,
    sales_amount,
    gp_amount,
    qty,
    prev_positive_sales_date,
    is_realized_new_delivery,
    sample_item_name
  FROM `{PROJECT_ID}.{DATASET}.v_new_deliveries_realized_daily_fact_all_months`
)
SELECT
  f.sales_date AS 売上日,
  f.customer_code AS 得意先コード,
  f.customer_name AS 得意先名,
  f.yj_code AS YJコード,
  f.sample_item_name AS 品目例,
  f.sales_amount AS 売上金額,
  f.gp_amount AS 粗利額,
  SAFE_DIVIDE(f.gp_amount, NULLIF(f.sales_amount, 0)) AS 粗利率,
  f.qty AS 数量,
  f.prev_positive_sales_date AS 前回正売上日
FROM fact f
JOIN cm
  ON SAFE_CAST(f.customer_code AS INT64) = cm.customer_code_i
WHERE cm.staff_code = {sql_quote(staff_drill_code)}
  AND f.is_realized_new_delivery = 1
  AND (
    ({sql_quote(period)} = 'day'   AND f.sales_date  = DATE({sql_quote(str(selected_date))})) OR
    ({sql_quote(period)} = 'week'  AND f.week_start  = DATE({sql_quote(str(selected_date))})) OR
    ({sql_quote(period)} = 'month' AND f.month_start = DATE({sql_quote(str(selected_date))})) OR
    ({sql_quote(period)} = 'fy'    AND DATE(f.fy, 4, 1) = DATE({sql_quote(str(selected_date))}))
  )
ORDER BY 売上金額 DESC
LIMIT 500;
"""
detail_df = fetch_df(detail_query)

if detail_df.empty:
    st.info("この担当者・期間では新規納品（Realized）がありません。")
else:
    st.dataframe(detail_df, use_container_width=True, hide_index=True)

