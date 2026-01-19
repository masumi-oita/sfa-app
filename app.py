import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account

# ----------------------------
# 設定（ここだけ環境に合わせて変更）
# ----------------------------
BQ_PROJECT = "salesdb-479915"
TABLE_SALES_2Y = f"{BQ_PROJECT}.sales_data.sales_history_2year"          # 売上明細（2年）
TABLE_NEW_2Y   = f"{BQ_PROJECT}.sales_data.new_deliveries_2year"         # 新規納品（2年）※要差替
VIEW_JAN_MASTER = f"{BQ_PROJECT}.sales_data.vw_dim_base_price_master_final"  # JAN統合VIEW（薬効用）

st.set_page_config(page_title="Strategic Sales Console", layout="wide")

# ----------------------------
# BigQuery client
# ----------------------------
def get_bq_client():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project=key_dict["project_id"])

# ----------------------------
# FY helper (Japan / FY starts April)
# ----------------------------
def fy_start(d: date) -> date:
    if d.month >= 4:
        return date(d.year, 4, 1)
    return date(d.year - 1, 4, 1)

def fy_year(d: date) -> int:
    # FY label (e.g., 2025 means 2025/04/01 - 2026/03/31)
    return (d.year if d.month >= 4 else d.year - 1)

def same_day_last_year(d: date) -> date:
    # 「同日」比較（2/29などの例外は簡易処理）
    try:
        return date(d.year - 1, d.month, d.day)
    except ValueError:
        # 2/29 -> 2/28
        return date(d.year - 1, d.month, 28)

# ----------------------------
# Loaders
# ----------------------------
@st.cache_data(ttl=300)
def load_sales_2y():
    client = get_bq_client()
    # 販売日がSTRINGなので、SAFEにDATE化（YYYYMMDD / YYYY-MM-DD / YYYY/MM/DD）
    q = f"""
    WITH src AS (
      SELECT
        得意先コード,
        得意先名,
        商品名,
        JANコード,
        YJコード,
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
    # 型整備
    df["売上日"] = pd.to_datetime(df["売上日"]).dt.date
    df["売上額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)
    df["利益率"] = df.apply(lambda r: (r["利益"] / r["売上額"]) if r["売上額"] else 0, axis=1)
    df["年度"] = df["売上日"].apply(fy_year)
    df["売上月キー"] = pd.to_datetime(df["売上日"]).dt.strftime("%Y-%m")
    return df

@st.cache_data(ttl=300)
def load_new_deliveries_2y():
    """
    新規納品テーブルが未確定なので、
    必要列をこの形に寄せておくと後が楽です。
    必須想定:
      - 納品日(STRING or DATE)
      - 得意先名
      - 商品名
      - 売上額(または合計金額)
      - 利益(または粗利) ※無ければ利益率は出せないので0扱いにする
    """
    client = get_bq_client()
    q = f"SELECT * FROM `{TABLE_NEW_2Y}`"
    df = client.query(q).to_dataframe()

    # ここはあなたの実データ列名に合わせて調整してください
    # 例: 納品日列が "売上日" や "販売日" の場合など
    if "納品日" in df.columns:
        df["日付"] = pd.to_datetime(df["納品日"], errors="coerce").dt.date
    elif "売上日" in df.columns:
        df["日付"] = pd.to_datetime(df["売上日"], errors="coerce").dt.date
    elif "販売日" in df.columns:
        df["日付"] = pd.to_datetime(df["販売日"], errors="coerce").dt.date
    else:
        df["日付"] = pd.NaT

    # 金額
    if "売上額" in df.columns:
        df["金額"] = pd.to_numeric(df["売上額"], errors="coerce").fillna(0)
    elif "合計金額" in df.columns:
        df["金額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    else:
        df["金額"] = 0

    # 利益（任意）
    if "利益" in df.columns:
        df["利益"] = pd.to_numeric(df["利益"], errors="coerce").fillna(0)
    elif "粗利" in df.columns:
        df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)
    else:
        df["利益"] = 0

    df["利益率"] = df.apply(lambda r: (r["利益"] / r["金額"]) if r["金額"] else 0, axis=1)
    return df

@st.cache_data(ttl=300)
def load_jan_master_min():
    """
    薬効（小分類）を出すために、JAN→薬効小分類名 を引ける表を最小で持つ。
    統合VIEWの列名は環境で違うので、必要ならここを合わせる。
    """
    client = get_bq_client()
    q = f"""
    SELECT
      -- ↓列名はあなたのVIEWに合わせて調整
      jan_code AS JANコード,
      yakko_small_name AS 薬効小分類名
    FROM `{VIEW_JAN_MASTER}`
    """
    try:
        df = client.query(q).to_dataframe()
        df["JANコード"] = df["JANコード"].astype(str)
        return df.dropna(subset=["JANコード"]).drop_duplicates("JANコード")
    except Exception:
        # まだ列名が合ってない / 無い場合は空で返す
        return pd.DataFrame(columns=["JANコード", "薬効小分類名"])

# ----------------------------
# UI
# ----------------------------
df_sales = load_sales_2y()
df_new = load_new_deliveries_2y()
df_jan = load_jan_master_min()

# 売上に薬効（小分類）を付与（③用）
if not df_jan.empty:
    df_sales["JANコード"] = df_sales["JANコード"].astype(str)
    df_sales = df_sales.merge(df_jan, on="JANコード", how="left")
else:
    df_sales["薬効小分類名"] = None

today = datetime.now().date()
fy0 = fy_year(today)
fy0_start = fy_start(today)
fy0_end = today
fy1 = fy0 - 1
fy1_start = date(fy1, 4, 1)
fy1_end = same_day_last_year(today)

st.sidebar.title("🎮 表示設定")
mode = st.sidebar.radio("モード", ["管理者モード", "営業員モード"])
# 担当社員名が売上テーブルに無いので、営業員モードは将来列が入ったら有効化してください
# いまは “得意先コード/名” で絞る等にしてもOK
if mode == "営業員モード":
    st.sidebar.info("※売上データに担当社員名が無いため、現在は管理者モード相当で表示します。")

# ------------------------------------------------------------
# ① 年度内 売上・利益・利益率 / 昨年との比較（ランキング→ドリルダウン）
# ------------------------------------------------------------
st.header("① 年度内 売上・利益・利益率 / 昨年比較（ランキング→ドリルダウン）")

df_fy0 = df_sales[(df_sales["売上日"] >= fy0_start) & (df_sales["売上日"] <= fy0_end)].copy()
df_fy1 = df_sales[(df_sales["売上日"] >= fy1_start) & (df_sales["売上日"] <= fy1_end)].copy()

def summarize(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    g = df.groupby(keys, dropna=False).agg(
        売上=("売上額", "sum"),
        利益=("利益", "sum")
    ).reset_index()
    g["利益率"] = g.apply(lambda r: (r["利益"] / r["売上"]) if r["売上"] else 0, axis=1)
    return g

# 得意先ランキング（売上）
sum_c0 = summarize(df_fy0, ["得意先名"])
sum_c1 = summarize(df_fy1, ["得意先名"])
rank_c = sum_c0.merge(sum_c1, on="得意先名", how="left", suffixes=("_今年", "_昨年")).fillna(0)
rank_c["売上前年差"] = rank_c["売上_今年"] - rank_c["売上_昨年"]
rank_c["利益前年差"] = rank_c["利益_今年"] - rank_c["利益_昨年"]

# 成分ランキング（売上）※成分規格名が無い場合、商品名で代替も可能
sum_i0 = summarize(df_fy0, ["商品名"])
sum_i1 = summarize(df_fy1, ["商品名"])
rank_i = sum_i0.merge(sum_i1, on="商品名", how="left", suffixes=("_今年", "_昨年")).fillna(0)
rank_i["売上前年差"] = rank_i["売上_今年"] - rank_i["売上_昨年"]
rank_i["利益前年差"] = rank_i["利益_今年"] - rank_i["利益_昨年"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("年度内 売上（今年）", f"¥{df_fy0['売上額'].sum():,.0f}")
c2.metric("年度内 利益（今年）", f"¥{df_fy0['利益'].sum():,.0f}")
c3.metric("利益率（今年）", f"{(df_fy0['利益'].sum()/df_fy0['売上額'].sum()*100) if df_fy0['売上額'].sum() else 0:.2f}%")
c4.metric("売上前年差（今年-昨年）", f"¥{(df_fy0['売上額'].sum()-df_fy1['売上額'].sum()):,.0f}")

tab_cust, tab_ing = st.tabs(["🏥 得意先ランキング", "💊 成分(代替:商品名)ランキング"])

with tab_cust:
    topn = st.slider("表示件数", 10, 100, 30)
    show = rank_c.sort_values("売上_今年", ascending=False).head(topn)
    st.dataframe(
        show[["得意先名","売上_今年","利益_今年","利益率_今年","売上_昨年","利益_昨年","売上前年差","利益前年差"]]
        .style.format({
            "売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}",
            "売上_昨年":"¥{:,.0f}","利益_昨年":"¥{:,.0f}",
            "売上前年差":"¥{:,.0f}","利益前年差":"¥{:,.0f}",
            "利益率_今年":"{:.2%}"
        }),
        use_container_width=True
    )

    # ドリルダウン：得意先→商品名（=成分代替）
    sel = st.selectbox("ドリルダウン（得意先選択）", ["-- 選択 --"] + show["得意先名"].tolist())
    if sel != "-- 選択 --":
        dd0 = summarize(df_fy0[df_fy0["得意先名"] == sel], ["商品名"]).sort_values("売上", ascending=False).head(50)
        dd1 = summarize(df_fy1[df_fy1["得意先名"] == sel], ["商品名"])
        dd = dd0.merge(dd1, on="商品名", how="left", suffixes=("_今年","_昨年")).fillna(0)
        dd["売上前年差"] = dd["売上_今年"] - dd["売上_昨年"]
        st.subheader(f"🏥 {sel}：商品（成分代替）ドリルダウン")
        st.dataframe(
            dd[["商品名","売上_今年","利益_今年","利益率_今年","売上_昨年","売上前年差"]]
            .style.format({"売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","売上_昨年":"¥{:,.0f}","売上前年差":"¥{:,.0f}","利益率_今年":"{:.2%}"}),
            use_container_width=True
        )

with tab_ing:
    topn2 = st.slider("表示件数 ", 10, 100, 30, key="topn_ing")
    show2 = rank_i.sort_values("売上_今年", ascending=False).head(topn2)
    st.dataframe(
        show2[["商品名","売上_今年","利益_今年","利益率_今年","売上_昨年","売上前年差","利益前年差"]]
        .style.format({"売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","売上_昨年":"¥{:,.0f}","売上前年差":"¥{:,.0f}","利益前年差":"¥{:,.0f}","利益率_今年":"{:.2%}"}),
        use_container_width=True
    )

    # ドリルダウン：商品→得意先
    sel2 = st.selectbox("ドリルダウン（商品名選択）", ["-- 選択 --"] + show2["商品名"].tolist())
    if sel2 != "-- 選択 --":
        dd0 = summarize(df_fy0[df_fy0["商品名"] == sel2], ["得意先名"]).sort_values("売上", ascending=False).head(50)
        dd1 = summarize(df_fy1[df_fy1["商品名"] == sel2], ["得意先名"])
        dd = dd0.merge(dd1, on="得意先名", how="left", suffixes=("_今年","_昨年")).fillna(0)
        dd["売上前年差"] = dd["売上_今年"] - dd["売上_昨年"]
        st.subheader(f"💊 {sel2}：得意先ドリルダウン")
        st.dataframe(
            dd[["得意先名","売上_今年","利益_今年","利益率_今年","売上_昨年","売上前年差"]]
            .style.format({"売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","売上_昨年":"¥{:,.0f}","売上前年差":"¥{:,.0f}","利益率_今年":"{:.2%}"}),
            use_container_width=True
        )

# ------------------------------------------------------------
# ② 新規納品サマリー（昨日/週/月/年度）→ドリル（得意先・品名・金額）
# ------------------------------------------------------------
st.divider()
st.header("② 新規納品サマリー（昨日→週→月→年度）")

period = st.radio("表示期間", ["昨日", "今週", "今月", "年度内"], horizontal=True)

yesterday = today - timedelta(days=1)
start_week = today - timedelta(days=today.weekday())
start_month = today.replace(day=1)
start_fy = fy0_start

df_new2 = df_new.dropna(subset=["日付"]).copy()
if period == "昨日":
    df_p = df_new2[df_new2["日付"] == yesterday]
elif period == "今週":
    df_p = df_new2[df_new2["日付"] >= start_week]
elif period == "今月":
    df_p = df_new2[df_new2["日付"] >= start_month]
else:
    df_p = df_new2[df_new2["日付"] >= start_fy]

# 列名ゆらぎ吸収（得意先名/商品名）
cust_col = "得意先名" if "得意先名" in df_p.columns else None
item_col = "商品名" if "商品名" in df_p.columns else None

c1, c2, c3, c4 = st.columns(4)
c1.metric("軒数（得意先数）", f"{df_p[cust_col].nunique() if cust_col else 0}件")
c2.metric("金額", f"¥{df_p['金額'].sum():,.0f}")
c3.metric("品目数（アイテム数）", f"{df_p[item_col].nunique() if item_col else 0}品目")
c4.metric("利益率", f"{(df_p['利益'].sum()/df_p['金額'].sum()*100) if df_p['金額'].sum() else 0:.2f}%")

if cust_col and item_col and not df_p.empty:
    st.subheader(f"{period}：得意先ドリルダウン")
    cust_sum = df_p.groupby(cust_col)["金額"].sum().sort_values(ascending=False).reset_index()
    sel_c = st.selectbox("得意先を選択", ["-- 選択 --"] + cust_sum[cust_col].head(50).tolist())
    if sel_c != "-- 選択 --":
        detail = df_p[df_p[cust_col] == sel_c].groupby(item_col).agg(
            金額=("金額","sum"),
            利益=("利益","sum")
        ).reset_index().sort_values("金額", ascending=False)
        detail["利益率"] = detail.apply(lambda r: (r["利益"]/r["金額"]) if r["金額"] else 0, axis=1)
        st.dataframe(
            detail.style.format({"金額":"¥{:,.0f}","利益":"¥{:,.0f}","利益率":"{:.2%}"}),
            use_container_width=True
        )
else:
    st.info("新規納品データの列名が未整備のため、ドリルダウンは保留です（得意先名・商品名列が必要）。")

# ------------------------------------------------------------
# ③ 下降/上昇 得意先差額ランキング → 品名・売上・利益・薬効（小分類）
# ------------------------------------------------------------
st.divider()
st.header("③ 下降/上昇 得意先差額ランキング（得意先→品名→薬効小分類）")

# 得意先×商品×薬効でFY別集計
def agg_key(df):
    g = df.groupby(["得意先名","商品名","薬効小分類名"], dropna=False).agg(
        売上=("売上額","sum"),
        利益=("利益","sum")
    ).reset_index()
    return g

a0 = agg_key(df_fy0).rename(columns={"売上":"売上_今年","利益":"利益_今年"})
a1 = agg_key(df_fy1).rename(columns={"売上":"売上_昨年","利益":"利益_昨年"})

m = a0.merge(a1, on=["得意先名","商品名","薬効小分類名"], how="outer").fillna(0)
m["売上差"] = m["売上_今年"] - m["売上_昨年"]
m["利益差"] = m["利益_今年"] - m["利益_昨年"]

# 得意先単位ランキング
cust_diff = m.groupby("得意先名").agg(
    売上差=("売上差","sum"),
    利益差=("利益差","sum"),
    売上_今年=("売上_今年","sum"),
    利益_今年=("利益_今年","sum"),
).reset_index()
cust_diff["利益率_今年"] = cust_diff.apply(lambda r: (r["利益_今年"]/r["売上_今年"]) if r["売上_今年"] else 0, axis=1)

tab_lost, tab_gain = st.tabs(["🔻 下落（Lost）", "🔼 上昇（Growth）"])

with tab_lost:
    top = cust_diff.sort_values("売上差").head(30)
    st.dataframe(
        top[["得意先名","売上差","利益差","売上_今年","利益_今年","利益率_今年"]]
        .style.format({"売上差":"¥{:,.0f}","利益差":"¥{:,.0f}","売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","利益率_今年":"{:.2%}"}),
        use_container_width=True
    )
    sel = st.selectbox("下落得意先を選択（品名一覧へ）", ["-- 選択 --"] + top["得意先名"].tolist(), key="lost_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差").head(50)
        st.subheader(f"🏥 {sel}：下落品目（売上差が大きい順）")
        st.dataframe(
            dd[["商品名","薬効小分類名","売上_昨年","売上_今年","売上差","利益_昨年","利益_今年","利益差"]]
            .style.format({"売上_昨年":"¥{:,.0f}","売上_今年":"¥{:,.0f}","売上差":"¥{:,.0f}","利益_昨年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","利益差":"¥{:,.0f}"}),
            use_container_width=True
        )

with tab_gain:
    top = cust_diff.sort_values("売上差", ascending=False).head(30)
    st.dataframe(
        top[["得意先名","売上差","利益差","売上_今年","利益_今年","利益率_今年"]]
        .style.format({"売上差":"¥{:,.0f}","利益差":"¥{:,.0f}","売上_今年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","利益率_今年":"{:.2%}"}),
        use_container_width=True
    )
    sel = st.selectbox("上昇得意先を選択（品名一覧へ）", ["-- 選択 --"] + top["得意先名"].tolist(), key="gain_sel")
    if sel != "-- 選択 --":
        dd = m[m["得意先名"] == sel].sort_values("売上差", ascending=False).head(50)
        st.subheader(f"🏥 {sel}：上昇品目（売上差が大きい順）")
        st.dataframe(
            dd[["商品名","薬効小分類名","売上_昨年","売上_今年","売上差","利益_昨年","利益_今年","利益差"]]
            .style.format({"売上_昨年":"¥{:,.0f}","売上_今年":"¥{:,.0f}","売上差":"¥{:,.0f}","利益_昨年":"¥{:,.0f}","利益_今年":"¥{:,.0f}","利益差":"¥{:,.0f}"}),
            use_container_width=True
        )
