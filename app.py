import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account

# ============================================================
# Strategic Sales Console (FULL / Robust BigQuery Loader)
# - Reads BigQuery table: sales_history_2year (raw; NO modification)
# - Fixes BadRequest issues by querying SELECT * and validating columns in Python
# - New-delivery judge uses ユニークコード_YJ (customer_code × YJ)
# - FY: April start
# - Features:
#   ① FY-to-date sales/profit/margin + last-year comparison (ranking + drilldown)
#   ② New delivery summary (Yesterday/Week/Month/FY) + drilldown
#   ③ Lost/Growth customer ranking by diff (FY-to-date vs last-year-to-date) + item list
# ============================================================

# ----------------------------
# CONFIG
# ----------------------------
BQ_PROJECT = "salesdb-479915"
TABLE_SALES_2Y = f"{BQ_PROJECT}.sales_data.sales_history_2year"
LOOKBACK_DAYS_NEW = 365  # New delivery: no sales in past 365 days

st.set_page_config(page_title="Strategic Sales Console", layout="wide")

# ----------------------------
# BigQuery Client
# ----------------------------
def get_bq_client():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project=key_dict["project_id"])

# ----------------------------
# FY helpers (April start)
# ----------------------------
def fy_year(d: date) -> int:
    return d.year if d.month >= 4 else d.year - 1

def fy_start(d: date) -> date:
    return date(d.year, 4, 1) if d.month >= 4 else date(d.year - 1, 4, 1)

def same_day_last_year(d: date) -> date:
    try:
        return date(d.year - 1, d.month, d.day)
    except ValueError:
        return date(d.year - 1, d.month, 28)

def yen(x) -> str:
    try:
        return f"¥{float(x):,.0f}"
    except Exception:
        return ""

# ----------------------------
# Robust loader (prevents BadRequest from missing columns)
# ----------------------------
@st.cache_data(ttl=300)
def load_sales_2y():
    client = get_bq_client()

    q = f"SELECT * FROM `{TABLE_SALES_2Y}`"
    try:
        df = client.query(q).to_dataframe()
    except Exception as e:
        # Streamlit Cloud redacts, but str(e) usually contains safe hints
        st.error("BigQuery query failed (BadRequest/permission/location/etc).")
        st.write(str(e))
        st.stop()

    # Required columns (based on your schema)
    required = ["得意先コード", "得意先名", "商品名", "合計金額", "粗利", "販売日", "YJコード", "ユニークコード_YJ"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"sales_history_2year に必要列が見つかりません: {missing}")
        st.write("実際に取得できた列名一覧:", list(df.columns))
        st.stop()

    # --- Date parse: 販売日 (STRING) supports YYYYMMDD / YYYY-MM-DD / YYYY/MM/DD etc.
    s = df["販売日"].astype(str).str.strip()

    d1 = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    d2 = pd.to_datetime(s, errors="coerce")
    d = d1.fillna(d2)

    df["売上日"] = d.dt.date
    df = df[df["売上日"].notna()].copy()

    # --- Numeric
    df["売上額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)

    # --- FY & month key
    df["年度"] = df["売上日"].apply(fy_year)
    df["売上月キー"] = pd.to_datetime(df["売上日"]).dt.strftime("%Y-%m")

    # --- keys
    df["得意先コード"] = df["得意先コード"].astype(str)
    df["YJコード"] = df["YJコード"].astype(str)
    df["ユニークコード_YJ"] = df["ユニークコード_YJ"].astype(str)

    # --- margin
    df["利益率"] = df.apply(lambda r: (r["利益"] / r["売上額"]) if r["売上額"] else 0, axis=1)

    # Optional cols used in drilldown (create if absent)
    for col in ["包装単位", "JANコード"]:
        if col not in df.columns:
            df[col] = ""

    return df

def add_new_delivery_flag_by_unique_yj(df_sales: pd.DataFrame, lookback_days=365) -> pd.DataFrame:
    """
    New delivery flag by ユニークコード_YJ:
      - First appearance => True
      - If previous sale date gap > lookback_days => True
    """
    df = df_sales.copy()
    df = df.sort_values(["ユニークコード_YJ", "売上日"])

    df["prev_date"] = df.groupby("ユニークコード_YJ")["売上日"].shift(1)
    df["gap_days"] = (pd.to_datetime(df["売上日"]) - pd.to_datetime(df["prev_date"])).dt.days
    df["is_new_delivery"] = df["prev_date"].isna() | (df["gap_days"] > lookback_days)

    return df

def summarize(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    g = df.groupby(keys, dropna=False).agg(
        売上=("売上額", "sum"),
        利益=("利益", "sum")
    ).reset_index()
    g["利益率"] = g.apply(lambda r: (r["利益"] / r["売上"]) if r["売上"] else 0, axis=1)
    return g

def add_yj_rep_name(df_yj_agg: pd.DataFrame, df_base: pd.DataFrame) -> pd.DataFrame:
    """
    Attach representative 商品名 for each YJコード based on max sales
    """
    if "YJコード" not in df_yj_agg.columns or "YJコード" not in df_base.columns:
        return df_yj_agg

    tmp = df_base.groupby(["YJコード", "商品名"], dropna=False)["売上額"].sum().reset_index()
    rep = tmp.sort_values(["YJコード", "売上額"], ascending=[True, False]).drop_duplicates("YJコード")
    rep = rep.rename(columns={"商品名": "代表商品名"}).drop(columns=["売上額"])
    return df_yj_agg.merge(rep, on="YJコード", how="left")

# ============================================================
# MAIN
# ============================================================
df_sales = load_sales_2y()
df_sales = add_new_delivery_flag_by_unique_yj(df_sales, lookback_days=LOOKBACK_DAYS_NEW)

if df_sales.empty:
    st.error("データが空です。BigQueryテーブルを確認してください。")
    st.stop()

today = datetime.now().date()
yesterday = today - timedelta(days=1)
start_week = today - timedelta(days=today.weekday())
start_month = today.replace(day=1)
start_fy = fy_start(today)

fy0 = fy_year(today)
fy0_start = start_fy
fy0_end = today

fy1 = fy0 - 1
fy1_start = date(fy1, 4, 1)
fy1_end = same_day_last_year(today)

# ------------------------------------------------------------
# Sidebar filters (optional)
# ------------------------------------------------------------
st.sidebar.title("🎮 表示設定")
search_cust = st.sidebar.text_input("得意先検索（部分一致）", "")
search_item = st.sidebar.text_input("品名検索（部分一致）", "")

df_view = df_sales.copy()
if search_cust.strip():
    df_view = df_view[df_view["得意先名"].astype(str).str.contains(search_cust.strip(), na=False)]
if search_item.strip():
    df_view = df_view[df_view["商品名"].astype(str).str.contains(search_item.strip(), na=False)]

# ============================================================
# ① FY-to-date Sales/Profit/Margin + YoY compare
# ============================================================
st.header("① 年度内 売上・利益・利益率 / 昨年比較（ランキング→ドリルダウン）")

df_fy0 = df_view[(df_view["売上日"] >= fy0_start) & (df_view["売上日"] <= fy0_end)].copy()
df_fy1 = df_view[(df_view["売上日"] >= fy1_start) & (df_view["売上日"] <= fy1_end)].copy()

# KPI
c1, c2, c3, c4 = st.columns(4)
c1.metric("売上（今年度内）", yen(df_fy0["売上額"].sum()))
c2.metric("利益（今年度内）", yen(df_fy0["利益"].sum()))
c3.metric("利益率（今年度内）", f"{(df_fy0['利益'].sum()/df_fy0['売上額'].sum()*100) if df_fy0['売上額'].sum() else 0:.2f}%")
c4.metric("売上前年差（今年-昨年）", yen(df_fy0["売上額"].sum() - df_fy1["売上額"].sum()))

tab_cust, tab_yj = st.tabs(["🏥 得意先ランキング", "💊 YJランキング"])

with tab_cust:
    topn = st.slider("表示件数", 10, 100, 30, key="topn_cust_1")

    s0 = summarize(df_fy0, ["得意先名"])
    s1 = summarize(df_fy1, ["得意先名"])
    rank = s0.merge(s1, on="得意先名", how="left", suffixes=("_今年", "_昨年")).fillna(0)
    rank["売上前年差"] = rank["売上_今年"] - rank["売上_昨年"]
    rank["利益前年差"] = rank["利益_今年"] - rank["利益_昨年"]

    show = rank.sort_values("売上_今年", ascending=False).head(topn)

    st.dataframe(
        show[["得意先名", "売上_今年", "利益_今年", "利益率_今年", "売上_昨年", "売上前年差", "利益前年差"]]
        .style.format({
            "売上_今年": "¥{:,.0f}",
            "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}",
            "売上_昨年": "¥{:,.0f}",
            "売上前年差": "¥{:,.0f}",
            "利益前年差": "¥{:,.0f}",
        }),
        use_container_width=True
    )

    sel_c = st.selectbox("ドリルダウン（得意先 → YJ）", ["-- 選択 --"] + show["得意先名"].tolist(), key="dd_cust_1")
    if sel_c != "-- 選択 --":
        dd = df_fy0[df_fy0["得意先名"] == sel_c].copy()
        dd_yj = dd.groupby(["YJコード"], dropna=False).agg(売上=("売上額", "sum"), 利益=("利益", "sum")).reset_index()
        dd_yj["利益率"] = dd_yj.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
        dd_yj = add_yj_rep_name(dd_yj, df_view).sort_values("売上", ascending=False).head(50)

        st.subheader(f"🏥 {sel_c}：YJ別 上位50")
        st.dataframe(
            dd_yj[["YJコード", "代表商品名", "売上", "利益", "利益率"]]
            .style.format({"売上": "¥{:,.0f}", "利益": "¥{:,.0f}", "利益率": "{:.2%}"}),
            use_container_width=True
        )

        # Monthly trend
        trend = dd.groupby(["売上月キー"], dropna=False)["売上額"].sum().reset_index().sort_values("売上月キー")
        st.plotly_chart(px.line(trend, x="売上月キー", y="売上額", title="月次推移（今年度内）"), use_container_width=True)

with tab_yj:
    topn = st.slider("表示件数", 10, 100, 30, key="topn_yj_1")

    s0 = summarize(df_fy0, ["YJコード"])
    s1 = summarize(df_fy1, ["YJコード"])
    rank = s0.merge(s1, on="YJコード", how="left", suffixes=("_今年", "_昨年")).fillna(0)
    rank["売上前年差"] = rank["売上_今年"] - rank["売上_昨年"]
    rank["利益前年差"] = rank["利益_今年"] - rank["利益_昨年"]

    rank = add_yj_rep_name(rank, df_view)
    show = rank.sort_values("売上_今年", ascending=False).head(topn)

    st.dataframe(
        show[["YJコード", "代表商品名", "売上_今年", "利益_今年", "利益率_今年", "売上_昨年", "売上前年差", "利益前年差"]]
        .style.format({
            "売上_今年": "¥{:,.0f}",
            "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}",
            "売上_昨年": "¥{:,.0f}",
            "売上前年差": "¥{:,.0f}",
            "利益前年差": "¥{:,.0f}",
        }),
        use_container_width=True
    )

    sel_yj = st.selectbox("ドリルダウン（YJ → 得意先）", ["-- 選択 --"] + show["YJコード"].tolist(), key="dd_yj_1")
    if sel_yj != "-- 選択 --":
        dd = df_fy0[df_fy0["YJコード"] == sel_yj].copy()
        dd_c = dd.groupby(["得意先名"], dropna=False).agg(売上=("売上額", "sum"), 利益=("利益", "sum")).reset_index()
        dd_c["利益率"] = dd_c.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
        dd_c = dd_c.sort_values("売上", ascending=False).head(50)

        st.subheader(f"💊 YJ={sel_yj}：得意先別 上位50")
        st.dataframe(
            dd_c.style.format({"売上": "¥{:,.0f}", "利益": "¥{:,.0f}", "利益率": "{:.2%}"}),
            use_container_width=True
        )

# ============================================================
# ② New Delivery Summary (by ユニークコード_YJ)
# ============================================================
st.divider()
st.header("② 新規納品サマリー（得意先×YJ：過去1年売上なし）")

period = st.radio("期間", ["昨日", "今週", "今月", "年度内"], horizontal=True, key="new_period")

if period == "昨日":
    base = df_view[df_view["売上日"] == yesterday]
elif period == "今週":
    base = df_view[df_view["売上日"] >= start_week]
elif period == "今月":
    base = df_view[df_view["売上日"] >= start_month]
else:
    base = df_view[df_view["売上日"] >= start_fy]

new_df = base[base["is_new_delivery"]].copy()

c1, c2, c3, c4 = st.columns(4)
c1.metric("軒数（得意先数）", f"{new_df['得意先コード'].nunique():,}")
c2.metric("金額（売上）", yen(new_df["売上額"].sum()))
c3.metric("品目数（YJ数）", f"{new_df['YJコード'].nunique():,}")
c4.metric("利益率", f"{(new_df['利益'].sum()/new_df['売上額'].sum()*100) if new_df['売上額'].sum() else 0:.2f}%")

with st.expander("ドリルダウン（得意先 → 品目一覧）", expanded=False):
    cust_sum = new_df.groupby(["得意先名", "得意先コード"], dropna=False)["売上額"].sum().sort_values(ascending=False).reset_index()
    cust_list = cust_sum["得意先名"].head(200).tolist()

    sel = st.selectbox("得意先を選択", ["-- 選択 --"] + cust_list, key="new_dd_cust")
    if sel != "-- 選択 --":
        d = new_df[new_df["得意先名"] == sel].copy()

        d2 = d.groupby(["YJコード"], dropna=False).agg(
            売上=("売上額", "sum"),
            利益=("利益", "sum"),
            明細行数=("売上額", "size"),
        ).reset_index()
        d2["利益率"] = d2.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
        d2 = add_yj_rep_name(d2, df_view).sort_values("売上", ascending=False)

        st.subheader(f"🏥 {sel}：新規納品（YJ別）")
        st.dataframe(
            d2[["YJコード", "代表商品名", "売上", "利益", "利益率", "明細行数"]]
            .style.format({"売上": "¥{:,.0f}", "利益": "¥{:,.0f}", "利益率": "{:.2%}"}),
            use_container_width=True
        )

# ============================================================
# ③ Lost / Growth customer diff ranking (FY-to-date vs last-year-to-date)
# ============================================================
st.divider()
st.header("③ 下降 / 上昇 得意先差額ランキング（得意先→品目一覧）")

g0 = df_fy0.groupby(["得意先名", "得意先コード", "YJコード"], dropna=False).agg(
    売上_今年=("売上額", "sum"),
    利益_今年=("利益", "sum"),
).reset_index()

g1 = df_fy1.groupby(["得意先名", "得意先コード", "YJコード"], dropna=False).agg(
    売上_昨年=("売上額", "sum"),
    利益_昨年=("利益", "sum"),
).reset_index()

m = g0.merge(g1, on=["得意先名", "得意先コード", "YJコード"], how="outer").fillna(0)
m["売上差"] = m["売上_今年"] - m["売上_昨年"]
m["利益差"] = m["利益_今年"] - m["利益_昨年"]
m = add_yj_rep_name(m, df_view)

cust = m.groupby(["得意先名", "得意先コード"], dropna=False).agg(
    売上差=("売上差", "sum"),
    利益差=("利益差", "sum"),
    売上_今年=("売上_今年", "sum"),
    利益_今年=("利益_今年", "sum"),
).reset_index()
cust["利益率_今年"] = cust.apply(lambda r: (r["利益_今年"]/r["売上_今年"]) if r["売上_今年"] else 0, axis=1)

tab_lost, tab_gain = st.tabs(["🔻 下落（Lost）", "🔼 上昇（Growth）"])

with tab_lost:
    topn = st.slider("表示件数", 10, 100, 30, key="lost_topn")
    loss = cust.sort_values("売上差", ascending=True).head(topn)

    st.dataframe(
        loss[["得意先名", "売上差", "利益差", "売上_今年", "利益_今年", "利益率_今年"]]
        .style.format({
            "売上差": "¥{:,.0f}",
            "利益差": "¥{:,.0f}",
            "売上_今年": "¥{:,.0f}",
            "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}",
        }),
        use_container_width=True
    )

    sel = st.selectbox("下落得意先を選択（品目一覧へ）", ["-- 選択 --"] + loss["得意先名"].tolist(), key="lost_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差", ascending=True).head(80)
        st.subheader(f"🏥 {sel}：下落品目（売上差の小さい順）上位80")

        st.dataframe(
            dd[["YJコード", "代表商品名", "売上_昨年", "売上_今年", "売上差", "利益_昨年", "利益_今年", "利益差"]]
            .style.format({
                "売上_昨年": "¥{:,.0f}",
                "売上_今年": "¥{:,.0f}",
                "売上差": "¥{:,.0f}",
                "利益_昨年": "¥{:,.0f}",
                "利益_今年": "¥{:,.0f}",
                "利益差": "¥{:,.0f}",
            }),
            use_container_width=True
        )

        st.plotly_chart(
            px.bar(dd.sort_values("売上差", ascending=True).head(30),
                   x="売上差", y="代表商品名", orientation="h",
                   title="下落品目トップ（売上差）"),
            use_container_width=True
        )

with tab_gain:
    topn = st.slider("表示件数", 10, 100, 30, key="gain_topn")
    gain = cust.sort_values("売上差", ascending=False).head(topn)

    st.dataframe(
        gain[["得意先名", "売上差", "利益差", "売上_今年", "利益_今年", "利益率_今年"]]
        .style.format({
            "売上差": "¥{:,.0f}",
            "利益差": "¥{:,.0f}",
            "売上_今年": "¥{:,.0f}",
            "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}",
        }),
        use_container_width=True
    )

    sel = st.selectbox("上昇得意先を選択（品目一覧へ）", ["-- 選択 --"] + gain["得意先名"].tolist(), key="gain_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差", ascending=False).head(80)
        st.subheader(f"🏥 {sel}：上昇品目（売上差の大きい順）上位80")

        st.dataframe(
            dd[["YJコード", "代表商品名", "売上_昨年", "売上_今年", "売上差", "利益_昨年", "利益_今年", "利益差"]]
            .style.format({
                "売上_昨年": "¥{:,.0f}",
                "売上_今年": "¥{:,.0f}",
                "売上差": "¥{:,.0f}",
                "利益_昨年": "¥{:,.0f}",
                "利益_今年": "¥{:,.0f}",
                "利益差": "¥{:,.0f}",
            }),
            use_container_width=True
        )

        st.plotly_chart(
            px.bar(dd.sort_values("売上差", ascending=False).head(30),
                   x="売上差", y="代表商品名", orientation="h",
                   title="上昇品目トップ（売上差）"),
            use_container_width=True
        )

# Footer note
st.caption(
    f"注) 新規納品判定は ユニークコード_YJ 単位。直前取引から{LOOKBACK_DAYS_NEW}日超で True（初回も True）。"
    "FYは4月開始。"
)
