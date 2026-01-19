import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account

# ============================================================
# Strategic Sales Console (Full)
# - Data: BigQuery sales_history_2year (raw; NO modification)
# - Key: ユニークコード_YJ (= 得意先コード×YJコード) を新規納品判定に使用
# - FY: 4月開始（年度 = YEAR(売上日 - 3ヶ月)）
# - Features:
#   ① 年度内 売上/利益/利益率 + 昨年比較（ランキング→ドリルダウン）
#   ② 新規納品サマリー（昨日/週/月/年度）→得意先→品名
#   ③ 上昇/下降 得意先差額ランキング → 品名一覧（売上/利益差）
#
# Optional:
#   - 薬効(小分類) を表示したい場合は、JAN→薬効小分類名 のマスタを読み込んでJOIN可能
#   - ここでは「任意」でONにできるようにしています（列名は環境に合わせて設定）
# ============================================================

# ----------------------------
# CONFIG (CHANGE ONLY IF NEEDED)
# ----------------------------
BQ_PROJECT = "salesdb-479915"
TABLE_SALES_2Y = f"{BQ_PROJECT}.sales_data.sales_history_2year"

# Optional master mapping for 薬効小分類 (JAN -> 薬効小分類名)
# 例: VIEW_JAN_MASTER = f"{BQ_PROJECT}.sales_data.vw_dim_base_price_master_final"
# ただし列名が環境により異なるため、下の SQL をあなたの列名に合わせて調整してください。
ENABLE_YAKKO = False
VIEW_JAN_MASTER = f"{BQ_PROJECT}.sales_data.vw_dim_base_price_master_final"
JAN_MASTER_SQL = f"""
SELECT
  -- ↓↓↓ あなたのVIEWの列名に合わせて変更してください（例）
  jan_code AS JANコード,
  yakko_small_name AS 薬効小分類名
FROM `{VIEW_JAN_MASTER}`
"""

st.set_page_config(page_title="Strategic Sales Console", layout="wide")

# ----------------------------
# BigQuery Client
# ----------------------------
def get_bq_client():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project=key_dict["project_id"])

# ----------------------------
# Helpers (FY: April start)
# ----------------------------
def fy_year(d: date) -> int:
    return d.year if d.month >= 4 else d.year - 1

def fy_start(d: date) -> date:
    return date(d.year, 4, 1) if d.month >= 4 else date(d.year - 1, 4, 1)

def same_day_last_year(d: date) -> date:
    try:
        return date(d.year - 1, d.month, d.day)
    except ValueError:
        # 2/29 -> 2/28
        return date(d.year - 1, d.month, 28)

def yen(x) -> str:
    try:
        return f"¥{float(x):,.0f}"
    except Exception:
        return ""

# ----------------------------
# Loaders
# ----------------------------
@st.cache_data(ttl=300)
def load_sales_2y():
    client = get_bq_client()
    q = f"""
    WITH src AS (
      SELECT
        得意先コード,
        得意先名,
        商品コード,
        商品名,
        包装単位,
        JANコード,
        YJコード,
        ユニークコード_YJ,
        合計金額,
        粗利,
        販売日
      FROM `{TABLE_SALES_2Y}`
    ),
    dt AS (
      SELECT
        *,
        COALESCE(
          SAFE.PARSE_DATE('%Y%m%d', 販売日),
          SAFE.PARSE_DATE('%Y-%m-%d', 販売日),
          SAFE.PARSE_DATE('%Y/%m/%d', 販売日)
        ) AS 売上日
      FROM src
    )
    SELECT * FROM dt
    WHERE 売上日 IS NOT NULL
    """
    df = client.query(q).to_dataframe()

    # Types
    df["売上日"] = pd.to_datetime(df["売上日"]).dt.date
    df["売上額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)
    df["利益率"] = df.apply(lambda r: (r["利益"] / r["売上額"]) if r["売上額"] else 0, axis=1)

    df["年度"] = df["売上日"].apply(fy_year)
    df["売上月キー"] = pd.to_datetime(df["売上日"]).dt.strftime("%Y-%m")

    # keys
    df["得意先コード"] = df["得意先コード"].astype(str)
    df["YJコード"] = df["YJコード"].astype(str)
    df["ユニークコード_YJ"] = df["ユニークコード_YJ"].astype(str)

    return df

@st.cache_data(ttl=300)
def load_yakko_master():
    if not ENABLE_YAKKO:
        return pd.DataFrame(columns=["JANコード", "薬効小分類名"])
    client = get_bq_client()
    try:
        df = client.query(JAN_MASTER_SQL).to_dataframe()
        df["JANコード"] = df["JANコード"].astype(str)
        return df.dropna(subset=["JANコード"]).drop_duplicates("JANコード")
    except Exception:
        return pd.DataFrame(columns=["JANコード", "薬効小分類名"])

def add_new_delivery_flag_by_unique_yj(df_sales: pd.DataFrame, lookback_days=365) -> pd.DataFrame:
    """
    ユニークコード_YJ 単位で新規納品判定。
    直前取引からlookback_days超なら新規（初回も新規）。
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

def join_yj_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    YJコードの代表商品名を付与（見やすさのため）
    """
    if "YJコード" not in df.columns:
        return df
    # 代表商品名（売上額最大の名称）
    tmp = df_sales.groupby(["YJコード", "商品名"], dropna=False)["売上額"].sum().reset_index()
    rep = tmp.sort_values(["YJコード", "売上額"], ascending=[True, False]).drop_duplicates("YJコード")
    rep = rep.rename(columns={"商品名": "代表商品名"}).drop(columns=["売上額"])
    return df.merge(rep, on="YJコード", how="left")

# ============================================================
# Main
# ============================================================
df_sales = load_sales_2y()
if df_sales.empty:
    st.error("sales_history_2year が空です。BigQueryテーブルを確認してください。")
    st.stop()

df_sales = add_new_delivery_flag_by_unique_yj(df_sales)

# Optional 薬効(小分類)
df_yakko = load_yakko_master()
if ENABLE_YAKKO and not df_yakko.empty:
    df_sales["JANコード"] = df_sales["JANコード"].astype(str)
    df_sales = df_sales.merge(df_yakko, on="JANコード", how="left")
else:
    df_sales["薬効小分類名"] = None

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

# Sidebar Filters
st.sidebar.title("🎮 表示設定")
search_cust = st.sidebar.text_input("得意先検索（部分一致）", "")
search_item = st.sidebar.text_input("品名検索（部分一致）", "")

df_view = df_sales.copy()
if search_cust.strip():
    df_view = df_view[df_view["得意先名"].astype(str).str.contains(search_cust.strip(), na=False)]
if search_item.strip():
    df_view = df_view[df_view["商品名"].astype(str).str.contains(search_item.strip(), na=False)]

# ============================================================
# ① 年度内 売上・利益・利益率 / 昨年比較（ランキング→ドリルダウン）
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

tab_cust, tab_yj = st.tabs(["🏥 得意先ランキング", "💊 成分（YJ）ランキング"])

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
            "売上_今年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}", "利益率_今年": "{:.2%}",
            "売上_昨年": "¥{:,.0f}", "売上前年差": "¥{:,.0f}", "利益前年差": "¥{:,.0f}"
        }),
        use_container_width=True
    )

    sel_c = st.selectbox("ドリルダウン（得意先 → YJ）", ["-- 選択 --"] + show["得意先名"].tolist(), key="dd_cust_1")
    if sel_c != "-- 選択 --":
        dd = df_fy0[df_fy0["得意先名"] == sel_c].copy()
        dd_yj = dd.groupby(["YJコード"], dropna=False).agg(売上=("売上額", "sum"), 利益=("利益", "sum")).reset_index()
        dd_yj["利益率"] = dd_yj.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
        dd_yj = join_yj_label(dd_yj).sort_values("売上", ascending=False).head(50)

        st.subheader(f"🏥 {sel_c}：YJ別 上位50")
        st.dataframe(
            dd_yj[["YJコード", "代表商品名", "売上", "利益", "利益率"]]
            .style.format({"売上": "¥{:,.0f}", "利益": "¥{:,.0f}", "利益率": "{:.2%}"}),
            use_container_width=True
        )

with tab_yj:
    topn = st.slider("表示件数", 10, 100, 30, key="topn_yj_1")

    s0 = summarize(df_fy0, ["YJコード"])
    s1 = summarize(df_fy1, ["YJコード"])
    rank = s0.merge(s1, on="YJコード", how="left", suffixes=("_今年", "_昨年")).fillna(0)
    rank["売上前年差"] = rank["売上_今年"] - rank["売上_昨年"]
    rank["利益前年差"] = rank["利益_今年"] - rank["利益_昨年"]
    rank = join_yj_label(rank)

    show = rank.sort_values("売上_今年", ascending=False).head(topn)

    st.dataframe(
        show[["YJコード", "代表商品名", "売上_今年", "利益_今年", "利益率_今年", "売上_昨年", "売上前年差", "利益前年差"]]
        .style.format({
            "売上_今年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}", "利益率_今年": "{:.2%}",
            "売上_昨年": "¥{:,.0f}", "売上前年差": "¥{:,.0f}", "利益前年差": "¥{:,.0f}"
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
# ② 新規納品サマリー（ユニークコード_YJ：過去1年なし）
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

with st.expander("ドリルダウン（得意先 → 品名一覧）", expanded=False):
    cust_sum = new_df.groupby(["得意先名", "得意先コード"], dropna=False)["売上額"].sum().sort_values(ascending=False).reset_index()
    cust_list = cust_sum["得意先名"].head(200).tolist()

    sel = st.selectbox("得意先を選択", ["-- 選択 --"] + cust_list, key="new_dd_cust")
    if sel != "-- 選択 --":
        d = new_df[new_df["得意先名"] == sel].copy()
        # YJ単位でまとめて見やすく（包装/JANが複数でもYJで統合）
        d2 = d.groupby(["YJコード"], dropna=False).agg(
            売上=("売上額", "sum"),
            利益=("利益", "sum"),
            明細行数=("売上額", "size")
        ).reset_index()
        d2["利益率"] = d2.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
        d2 = join_yj_label(d2).sort_values("売上", ascending=False)

        st.subheader(f"🏥 {sel}：新規納品（YJ別）")
        st.dataframe(
            d2[["YJコード", "代表商品名", "売上", "利益", "利益率", "明細行数"]]
            .style.format({"売上": "¥{:,.0f}", "利益": "¥{:,.0f}", "利益率": "{:.2%}"}),
            use_container_width=True
        )

# ============================================================
# ③ 下降 / 上昇 得意先差額ランキング（今年度内 vs 昨年度同日まで）
# ============================================================
st.divider()
st.header("③ 下降 / 上昇 得意先差額ランキング（得意先→品目一覧）")

# FY集計（得意先×YJ単位）
g0 = df_fy0.groupby(["得意先名", "得意先コード", "YJコード"], dropna=False).agg(
    売上_今年=("売上額", "sum"),
    利益_今年=("利益", "sum")
).reset_index()

g1 = df_fy1.groupby(["得意先名", "得意先コード", "YJコード"], dropna=False).agg(
    売上_昨年=("売上額", "sum"),
    利益_昨年=("利益", "sum")
).reset_index()

m = g0.merge(g1, on=["得意先名", "得意先コード", "YJコード"], how="outer").fillna(0)
m["売上差"] = m["売上_今年"] - m["売上_昨年"]
m["利益差"] = m["利益_今年"] - m["利益_昨年"]

# 代表商品名を付与
m = join_yj_label(m)

# 得意先単位の差額ランキング
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
            "売上差": "¥{:,.0f}", "利益差": "¥{:,.0f}",
            "売上_今年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}"
        }),
        use_container_width=True
    )

    sel = st.selectbox("下落得意先を選択（品目一覧へ）", ["-- 選択 --"] + loss["得意先名"].tolist(), key="lost_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差", ascending=True).head(80)
        cols = ["YJコード", "代表商品名", "売上_昨年", "売上_今年", "売上差", "利益_昨年", "利益_今年", "利益差"]
        if ENABLE_YAKKO:
            # 薬効(小分類)は明細側にある場合のみ（ここでは代表商品名中心のため省略）
            pass

        st.subheader(f"🏥 {sel}：下落品目（売上差の小さい順）上位80")
        st.dataframe(
            dd[cols].style.format({
                "売上_昨年": "¥{:,.0f}", "売上_今年": "¥{:,.0f}", "売上差": "¥{:,.0f}",
                "利益_昨年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}", "利益差": "¥{:,.0f}"
            }),
            use_container_width=True
        )

        # 補助チャート（任意）
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
            "売上差": "¥{:,.0f}", "利益差": "¥{:,.0f}",
            "売上_今年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}",
            "利益率_今年": "{:.2%}"
        }),
        use_container_width=True
    )

    sel = st.selectbox("上昇得意先を選択（品目一覧へ）", ["-- 選択 --"] + gain["得意先名"].tolist(), key="gain_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差", ascending=False).head(80)
        cols = ["YJコード", "代表商品名", "売上_昨年", "売上_今年", "売上差", "利益_昨年", "利益_今年", "利益差"]

        st.subheader(f"🏥 {sel}：上昇品目（売上差の大きい順）上位80")
        st.dataframe(
            dd[cols].style.format({
                "売上_昨年": "¥{:,.0f}", "売上_今年": "¥{:,.0f}", "売上差": "¥{:,.0f}",
                "利益_昨年": "¥{:,.0f}", "利益_今年": "¥{:,.0f}", "利益差": "¥{:,.0f}"
            }),
            use_container_width=True
        )

        st.plotly_chart(
            px.bar(dd.sort_values("売上差", ascending=False).head(30),
                   x="売上差", y="代表商品名", orientation="h",
                   title="上昇品目トップ（売上差）"),
            use_container_width=True
        )

# ============================================================
# Notes
# ============================================================
st.caption(
    "注) 新規納品判定は sales_history_2year の ユニークコード_YJ を使用し、"
    "直前取引から365日超で True（初回も True）。FYは4月開始。"
)
