import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, Forbidden

# =========================
# CONFIG
# =========================
BQ_PROJECT = "salesdb-479915"
TABLE_SNAPSHOT_2Y = f"{BQ_PROJECT}.sales_data.sales_history_2year"
TABLE_INC = f"{BQ_PROJECT}.sales_data.sales_details_snapshots"
LOOKBACK_DAYS_NEW = 365

st.set_page_config(page_title="Strategic Sales Console", layout="wide")

# =========================
# BQ client
# =========================
def get_bq_client():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    return bigquery.Client(credentials=credentials, project=key_dict["project_id"])

# =========================
# date helpers
# =========================
def fy_year(d: date) -> int:
    return d.year if d.month >= 4 else d.year - 1

def fy_start(d: date) -> date:
    return date(d.year, 4, 1) if d.month >= 4 else date(d.year - 1, 4, 1)

def same_day_last_year(d: date) -> date:
    try:
        return date(d.year - 1, d.month, d.day)
    except ValueError:
        return date(d.year - 1, d.month, 28)

def month_start(d: date) -> date:
    return d.replace(day=1)

def yen(x) -> str:
    try:
        return f"¥{float(x):,.0f}"
    except Exception:
        return ""

def safe_parse_date_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    d1 = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    d2 = pd.to_datetime(s, errors="coerce")
    return d1.fillna(d2)

# =========================
# schema helpers
# =========================
@st.cache_data(ttl=3600)
def get_table_columns(table_fqn: str) -> list[str]:
    """
    table_fqn: project.dataset.table
    """
    client = get_bq_client()
    try:
        t = client.get_table(table_fqn)
        return [f.name for f in t.schema]
    except Exception as e:
        st.error(f"テーブルスキーマ取得に失敗: {table_fqn}")
        st.write(str(e))
        st.stop()

def select_existing(cols_wanted: list[str], existing: list[str]) -> list[str]:
    return [c for c in cols_wanted if c in existing]

def run_query_df(sql: str, label: str):
    client = get_bq_client()
    try:
        return client.query(sql).to_dataframe(create_bqstorage_client=False)
    except BadRequest as e:
        st.error(f"[{label}] BigQuery BadRequest")
        # ここが赤塗りされない範囲で“できるだけ”出す
        st.write("SQL:", sql)
        st.write("Error:", str(e))
        st.stop()
    except Forbidden as e:
        st.error(f"[{label}] BigQuery Forbidden（権限不足の可能性）")
        st.write("Error:", str(e))
        st.stop()
    except Exception as e:
        st.error(f"[{label}] BigQuery query failed")
        st.write("SQL:", sql)
        st.write("Error:", str(e))
        st.stop()

# =========================
# LOADERS (schema-safe)
# =========================
@st.cache_data(ttl=1800)
def load_snapshot_2y():
    # スキーマ取得（project.dataset.table形式）
    cols = get_table_columns(TABLE_SNAPSHOT_2Y)

    wanted = [
        "得意先コード", "得意先名",
        "商品コード", "商品名", "包装単位",
        "ロットNo", "使用期限",
        "数量", "単価",
        "合計金額", "粗利",
        "JANコード", "YJコード",
        "ユニークコード_YJ",
        "販売日",
    ]
    use_cols = select_existing(wanted, cols)
    if not use_cols:
        st.error("[SNAPSHOT] 取得できる列がありません。")
        st.write("テーブル列:", cols)
        st.stop()

    select_sql = ",\n      ".join([f"`{c}`" for c in use_cols])
    q = f"""
    SELECT
      {select_sql}
    FROM `{TABLE_SNAPSHOT_2Y}`
    """

    df = run_query_df(q, "SNAPSHOT")

    # 必須列だけは厳密に確認
    required = ["得意先コード","得意先名","販売日","YJコード","ユニークコード_YJ","商品名","合計金額","粗利"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        st.error(f"[SNAPSHOT] 必須列不足: {miss}")
        st.write("取得列:", list(df.columns))
        st.stop()

    df["売上日"] = safe_parse_date_series(df["販売日"]).dt.date
    df = df[df["売上日"].notna()].copy()

    df["売上額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)

    df["年度"] = df["売上日"].apply(fy_year)
    df["売上月キー"] = pd.to_datetime(df["売上日"]).dt.strftime("%Y-%m")

    df["得意先コード"] = df["得意先コード"].astype(str)
    df["YJコード"] = df["YJコード"].astype(str)
    df["ユニークコード_YJ"] = df["ユニークコード_YJ"].astype(str)

    df["利益率"] = df.apply(lambda r: (r["利益"] / r["売上額"]) if r["売上額"] else 0, axis=1)

    # ない列は埋めて互換
    for col in ["商品コード","包装単位","数量","単価","JANコード","ロットNo","使用期限"]:
        if col not in df.columns:
            df[col] = "" if col in ["商品コード","包装単位","JANコード","ロットNo","使用期限"] else 0

    return df


@st.cache_data(ttl=1800)
def load_incremental_this_month():
    # こちらもスキーマに依存しないよう、存在列だけを使う
    cols = get_table_columns(TABLE_INC)

    # GASのMERGEで使っていた列候補
    # （存在しない場合は後段でmissing検知して停止）
    q = f"""
    SELECT
      CAST(`得意先コード` AS STRING) AS 得意先コード,
      `得意先名` AS 得意先名,
      CAST(`YJCode` AS STRING) AS YJコード,
      CAST(`JAN` AS STRING) AS JANコード,
      CAST(`商品コード` AS STRING) AS 商品コード,
      `商品名称` AS 商品名,
      `包装単位` AS 包装単位,
      CAST(`販売日` AS DATE) AS 売上日,
      CAST(`販売数量` AS FLOAT64) AS 数量,
      CAST(`合計金額` AS FLOAT64) AS 合計金額,
      CAST(`粗利` AS FLOAT64) AS 粗利
    FROM `{TABLE_INC}`
    WHERE CAST(`販売日` AS DATE) >= DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH)
    """

    # もし列名が違う場合はここでBadRequestになるので、SQLとエラーが画面に出る
    df = run_query_df(q, "INCREMENTAL")

    required = ["得意先コード","得意先名","売上日","YJコード","数量","商品名","合計金額","粗利"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        st.error(f"[INCREMENTAL] 必須列不足: {miss}")
        st.write("取得列:", list(df.columns))
        st.stop()

    df["売上日"] = pd.to_datetime(df["売上日"], errors="coerce").dt.date
    df = df[df["売上日"].notna()].copy()

    df["売上額"] = pd.to_numeric(df["合計金額"], errors="coerce").fillna(0)
    df["利益"] = pd.to_numeric(df["粗利"], errors="coerce").fillna(0)

    df["年度"] = df["売上日"].apply(fy_year)
    df["売上月キー"] = pd.to_datetime(df["売上日"]).dt.strftime("%Y-%m")

    df["得意先コード"] = df["得意先コード"].astype(str)
    df["YJコード"] = df["YJコード"].astype(str)
    df["ユニークコード_YJ"] = df["得意先コード"] + "_" + df["YJコード"]

    df["利益率"] = df.apply(lambda r: (r["利益"] / r["売上額"]) if r["売上額"] else 0, axis=1)
    return df


@st.cache_data(ttl=1800)
def load_sales_merged():
    today = datetime.now().date()
    m0 = month_start(today)

    snap = load_snapshot_2y()
    snap = snap[snap["売上日"] < m0].copy()

    inc = load_incremental_this_month()

    merged = pd.concat([snap, inc], ignore_index=True)

    # 念のため重複排除
    subset = ["得意先コード","売上日","YJコード","商品コード","数量","合計金額"]
    subset = [c for c in subset if c in merged.columns]
    if subset:
        merged = merged.drop_duplicates(subset=subset, keep="last")

    return merged, len(snap), len(inc)

# =========================
# New delivery flag
# =========================
def add_new_delivery_flag_by_unique_yj(df: pd.DataFrame, lookback_days=365) -> pd.DataFrame:
    d = df.sort_values(["ユニークコード_YJ", "売上日"]).copy()
    d["prev_date"] = d.groupby("ユニークコード_YJ")["売上日"].shift(1)
    d["gap_days"] = (pd.to_datetime(d["売上日"]) - pd.to_datetime(d["prev_date"])).dt.days
    d["is_new_delivery"] = d["prev_date"].isna() | (d["gap_days"] > lookback_days)
    return d

# =========================
# MAIN
# =========================
df_sales, n_snap, n_inc = load_sales_merged()
df_sales = add_new_delivery_flag_by_unique_yj(df_sales, LOOKBACK_DAYS_NEW)

st.title("Strategic Sales Console")
c0, c1, c2 = st.columns(3)
c0.metric("スナップ（当月除外）", f"{n_snap:,}")
c1.metric("当月（洗い替え）", f"{n_inc:,}")
c2.metric("統合後（重複排除）", f"{len(df_sales):,}")

if df_sales.empty:
    st.error("データが空です。")
    st.stop()

today = datetime.now().date()
yesterday = today - timedelta(days=1)
start_week = today - timedelta(days=today.weekday())
start_month = today.replace(day=1)
start_fy = fy_start(today)

fy0_start = start_fy
fy0_end = today
fy1_start = date(fy_year(today)-1, 4, 1)
fy1_end = same_day_last_year(today)

st.sidebar.title("🎮 フィルタ")
mode = st.sidebar.radio("モード", ["管理者モード", "営業員モード"])
df_view = df_sales.copy()

if mode == "営業員モード" and "担当社員名" in df_view.columns:
    staff = st.sidebar.selectbox("担当者", sorted(df_view["担当社員名"].dropna().unique()))
    df_view = df_view[df_view["担当社員名"] == staff]

# ① FY-to-date ranking
st.header("① 年度内 売上・利益・利益率 / 昨年比較（得意先ランキング）")
df_fy0 = df_view[(df_view["売上日"] >= fy0_start) & (df_view["売上日"] <= fy0_end)].copy()
df_fy1 = df_view[(df_view["売上日"] >= fy1_start) & (df_view["売上日"] <= fy1_end)].copy()

a1, a2, a3, a4 = st.columns(4)
a1.metric("売上（今年度内）", yen(df_fy0["売上額"].sum()))
a2.metric("利益（今年度内）", yen(df_fy0["利益"].sum()))
a3.metric("利益率（今年度内）", f"{(df_fy0['利益'].sum()/df_fy0['売上額'].sum()*100) if df_fy0['売上額'].sum() else 0:.2f}%")
a4.metric("売上前年差", yen(df_fy0["売上額"].sum() - df_fy1["売上額"].sum()))

topn = st.slider("表示件数", 10, 100, 30)

s0 = df_fy0.groupby("得意先名", dropna=False).agg(売上=("売上額","sum"), 利益=("利益","sum")).reset_index()
s1 = df_fy1.groupby("得意先名", dropna=False).agg(売上_昨年=("売上額","sum")).reset_index()
rank = s0.merge(s1, on="得意先名", how="left").fillna(0)
rank["利益率"] = rank.apply(lambda r: (r["利益"]/r["売上"]) if r["売上"] else 0, axis=1)
rank["前年差"] = rank["売上"] - rank["売上_昨年"]
rank = rank.sort_values("売上", ascending=False).head(topn)

st.dataframe(
    rank.style.format({"売上":"¥{:,.0f}","利益":"¥{:,.0f}","利益率":"{:.2%}","売上_昨年":"¥{:,.0f}","前年差":"¥{:,.0f}"}),
    use_container_width=True
)

# ② New delivery summary
st.divider()
st.header("② 新規納品サマリー（得意先×YJ / 過去1年売上なし）")
period = st.radio("期間", ["昨日","今週","今月","年度内"], horizontal=True)

if period == "昨日":
    base = df_view[df_view["売上日"] == yesterday]
elif period == "今週":
    base = df_view[df_view["売上日"] >= start_week]
elif period == "今月":
    base = df_view[df_view["売上日"] >= start_month]
else:
    base = df_view[df_view["売上日"] >= start_fy]

new_df = base[base["is_new_delivery"]].copy()

b1, b2, b3, b4 = st.columns(4)
b1.metric("軒数（得意先数）", f"{new_df['得意先コード'].nunique():,}")
b2.metric("金額（売上）", yen(new_df["売上額"].sum()))
b3.metric("品目数（YJ数）", f"{new_df['YJコード'].nunique():,}")
b4.metric("利益率", f"{(new_df['利益'].sum()/new_df['売上額'].sum()*100) if new_df['売上額'].sum() else 0:.2f}%")

# ③ diff
st.divider()
st.header("③ 下降 / 上昇 得意先差額ランキング（年度内 vs 昨年同日まで）")
c0_ = df_fy0.groupby("得意先名", dropna=False)["売上額"].sum().reset_index().rename(columns={"売上額":"売上_今年"})
c1_ = df_fy1.groupby("得意先名", dropna=False)["売上額"].sum().reset_index().rename(columns={"売上額":"売上_昨年"})
cd = c0_.merge(c1_, on="得意先名", how="outer").fillna(0)
cd["差額"] = cd["売上_今年"] - cd["売上_昨年"]

tab_l, tab_g = st.tabs(["🔻 下落", "🔼 上昇"])
with tab_l:
    st.dataframe(cd.sort_values("差額", ascending=True).head(30).style.format({"売上_今年":"¥{:,.0f}","売上_昨年":"¥{:,.0f}","差額":"¥{:,.0f}"}), use_container_width=True)
with tab_g:
    st.dataframe(cd.sort_values("差額", ascending=False).head(30).style.format({"売上_今年":"¥{:,.0f}","売上_昨年":"¥{:,.0f}","差額":"¥{:,.0f}"}), use_container_width=True)

st.caption("※BadRequestが出る場合、画面にSQLとエラーを表示します。まずそれを貼ってください。")
