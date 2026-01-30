# app.py
# ============================================================
# SFA Sales OS - 入口高速版（判断専用ミニ版 / 確定）
# - FYTD（年度累計：4月〜当月末）サマリー（前年差・昨対）
# - 当月：前年同月比ランキング（上位/下位 + 比較不能）
# - 新規納品サマリー（月間）※まずは軽量カウント中心
# - 権限分岐（admin_view/admin_edit/sales_view）
# - 日本語表示（英語ラベル排除）
# - 得意先検索：部分一致→候補→選択（コード/名称）
#
# 重要方針（超重要）：
# - 入口で叩くクエリは「最大3本」(FYTD/当月YoY/新規納品)
# - それ以外は “押した人だけ” 後読み（クリック後ロード）
# - Streamlit cache で「DataFrameだけ」返す（client等は返さない）
# ============================================================

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st

from google.cloud import bigquery

# -----------------------------
# Config
# -----------------------------
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "salesdb-479915")
DATASET_ID = os.getenv("BQ_DATASET_ID", "sales_data")
LOCATION = os.getenv("BQ_LOCATION", "asia-northeast1")

# 参照：この3つだけが「入口クエリ」
VIEW_SYS_CURRENT_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sys_current_month`"
FACT_LOGIN_DAILY = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_fact_login_jan_daily`"
NEW_DELIV_MONTHLY = f"`{PROJECT_ID}.{DATASET_ID}.v_new_deliveries_realized_monthly`"

# 権限
TBL_STAFF_ROLE = f"`{PROJECT_ID}.{DATASET_ID}.dim_staff_role`"

# 得意先候補（検索用）
# adminは全得意先候補、salesは自分担当優先（あれば）。
# 無い環境でも落ちないように optional として扱う。
VIEW_CUSTOMER_STAFF = f"`{PROJECT_ID}.{DATASET_ID}.dim_customer_staff_current`"

# 表示設定
TOP_N = 30
MAX_SUGGEST = 30

# -----------------------------
# Helpers
# -----------------------------
def jpy(n: Optional[float]) -> str:
    if n is None or pd.isna(n):
        return ""
    return f"¥{int(round(n)):,}"

def pct(n: Optional[float]) -> str:
    if n is None or pd.isna(n):
        return ""
    return f"{n*100:.1f}%"

def safe_lower(s: str) -> str:
    return (s or "").lower()

def now_jst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))

@dataclass
class LoginContext:
    user_email: str
    role_admin_view: bool
    role_admin_edit: bool
    role_sales_view: bool
    role_tier: str
    area_name: str

def get_query_param_user_email() -> str:
    qp = st.query_params
    v = qp.get("user_email", "")
    if isinstance(v, list):
        v = v[0] if v else ""
    return (v or "").strip()

def _bq_client() -> bigquery.Client:
    # クライアントはキャッシュ対象にしない（unserializable対策）
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)

@st.cache_data(ttl=3600, show_spinner=False)
def bq_query_df(sql: str, params: Tuple[Tuple[str, str, Any], ...] = ()) -> pd.DataFrame:
    """
    BigQuery -> DataFrame
    params: (name, type, value) tuples. type is BigQuery scalar type string.
    NOTE: DataFrameだけ返す（client/job/iteratorを返さない）
    """
    client = _bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(name, typ, val) for (name, typ, val) in params
        ]
    )
    job = client.query(sql, job_config=job_config, location=LOCATION)
    return job.result().to_dataframe(create_bqstorage_client=True)

def bq_try_query_df(sql: str, params: Tuple[Tuple[str, str, Any], ...] = ()) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        df = bq_query_df(sql, params)
        return df, None
    except Exception as e:
        return None, str(e)

@st.cache_data(ttl=3600, show_spinner=False)
def get_current_month() -> pd.Timestamp:
    df = bq_query_df(f"SELECT current_month FROM {VIEW_SYS_CURRENT_MONTH} LIMIT 1")
    if df.empty:
        raise RuntimeError("v_sys_current_month が空です")
    return pd.to_datetime(df.loc[0, "current_month"]).normalize()

@st.cache_data(ttl=3600, show_spinner=False)
def get_login_context(user_email: str) -> LoginContext:
    # 役割が未登録でも落とさず、sales_view を True に倒す（暫定運用）
    sql = f"""
    SELECT
      login_email,
      COALESCE(role_admin_view, FALSE) AS role_admin_view,
      COALESCE(role_admin_edit, FALSE) AS role_admin_edit,
      COALESCE(role_sales_view, TRUE)  AS role_sales_view,
      COALESCE(role_tier, "SALES")     AS role_tier,
      COALESCE(area_name, "未分類")    AS area_name
    FROM {TBL_STAFF_ROLE}
    WHERE login_email = @user_email
    LIMIT 1
    """
    df = bq_query_df(sql, (("user_email", "STRING", user_email),))
    if df.empty:
        return LoginContext(
            user_email=user_email,
            role_admin_view=False,
            role_admin_edit=False,
            role_sales_view=True,
            role_tier="SALES",
            area_name="未分類",
        )
    r = df.iloc[0].to_dict()
    return LoginContext(
        user_email=user_email,
        role_admin_view=bool(r["role_admin_view"]),
        role_admin_edit=bool(r["role_admin_edit"]),
        role_sales_view=bool(r["role_sales_view"]),
        role_tier=str(r["role_tier"] or "SALES"),
        area_name=str(r["area_name"] or "未分類"),
    )

# -----------------------------
# Core SQL (入口3本)
# -----------------------------
def sql_fytd_org_summary() -> str:
    # FYTD: 4/1〜当月末（current_month の月末まで）
    # FYTD比較: 前年度同期間（昨年度4/1〜昨年当月末）
    return f"""
    WITH sys AS (
      SELECT
        current_month,
        EXTRACT(YEAR FROM DATE_SUB(current_month, INTERVAL 3 MONTH)) AS fiscal_year_apr,
        DATE(EXTRACT(YEAR FROM DATE_SUB(current_month, INTERVAL 3 MONTH)), 4, 1) AS fy_start,
        DATE_ADD(current_month, INTERVAL 1 MONTH) AS next_month
      FROM {VIEW_SYS_CURRENT_MONTH}
    ),
    bounds AS (
      SELECT
        current_month,
        fy_start,
        DATE_SUB(next_month, INTERVAL 1 DAY) AS as_of_date,
        DATE_SUB(fy_start, INTERVAL 1 YEAR) AS fy_start_py,
        DATE_SUB(DATE_SUB(next_month, INTERVAL 1 DAY), INTERVAL 1 YEAR) AS as_of_date_py
      FROM sys
    ),
    fact AS (
      SELECT sales_date, sales_amount, gross_profit
      FROM {FACT_LOGIN_DAILY}
      WHERE sales_date IS NOT NULL
    )
    SELECT
      b.current_month AS 対象月,
      b.fy_start AS 年度開始日,
      b.as_of_date AS 集計終端日,
      SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.sales_amount, 0)) AS 売上_年度累計,
      SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.gross_profit, 0)) AS 粗利_年度累計,
      SAFE_DIVIDE(
        SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.gross_profit, 0)),
        NULLIF(SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.sales_amount, 0)), 0)
      ) AS 粗利率_年度累計,

      SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.sales_amount, 0)) AS 売上_前年差_比較対象,
      SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.gross_profit, 0)) AS 粗利_前年差_比較対象,

      (SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.sales_amount, 0))
        - SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.sales_amount, 0))) AS 売上_前年差,
      (SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.gross_profit, 0))
        - SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.gross_profit, 0))) AS 粗利_前年差,

      SAFE_DIVIDE(
        SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.sales_amount, 0)),
        NULLIF(SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.sales_amount, 0)), 0)
      ) - 1 AS 売上_昨対_年度累計,

      SAFE_DIVIDE(
        SUM(IF(f.sales_date BETWEEN b.fy_start AND b.as_of_date, f.gross_profit, 0)),
        NULLIF(SUM(IF(f.sales_date BETWEEN b.fy_start_py AND b.as_of_date_py, f.gross_profit, 0)), 0)
      ) - 1 AS 粗利_昨対_年度累計
    FROM bounds b
    CROSS JOIN fact f
    GROUP BY 対象月, 年度開始日, 集計終端日
    """

def sql_monthly_yoy_customer_rank(valid_only: bool) -> str:
    # 当月（current_month） vs 前年同月
    # valid_only=True: 比較可能（前年同月あり）
    # valid_only=False: 比較不能（前年同月が0/NULL）
    cond = "py_sales IS NOT NULL AND py_sales != 0" if valid_only else "(py_sales IS NULL OR py_sales = 0)"
    return f"""
    WITH sys AS (
      SELECT current_month FROM {VIEW_SYS_CURRENT_MONTH}
    ),
    base AS (
      SELECT
        f.customer_code,
        f.customer_name,
        SUM(IF(f.month = s.current_month, f.sales_amount, 0)) AS cy_sales,
        SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.sales_amount, 0)) AS py_sales,
        SUM(IF(f.month = s.current_month, f.gross_profit, 0)) AS cy_gp,
        SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.gross_profit, 0)) AS py_gp
      FROM {FACT_LOGIN_DAILY} f
      CROSS JOIN sys s
      WHERE f.month IN (s.current_month, DATE_SUB(s.current_month, INTERVAL 1 YEAR))
      GROUP BY customer_code, customer_name
    )
    SELECT
      customer_code AS 得意先コード,
      customer_name AS 得意先名,
      cy_sales AS 売上_当月,
      py_sales AS 売上_前年同月,
      (cy_sales - py_sales) AS 売上_前年差,
      SAFE_DIVIDE(cy_sales, NULLIF(py_sales,0)) - 1 AS 売上_前年同月比,
      cy_gp AS 粗利_当月,
      py_gp AS 粗利_前年同月,
      (cy_gp - py_gp) AS 粗利_前年差,
      SAFE_DIVIDE(cy_gp, NULLIF(py_gp,0)) - 1 AS 粗利_前年同月比
    FROM base
    WHERE {cond}
    """

def sql_new_deliveries_monthly_summary() -> str:
    # 入口は軽量に：月次の「新規納品（realized）」件数の概況だけ
    # ※列が環境差で違っても落ちないように、最低限の列でまず取得
    return f"""
    SELECT
      month_start AS 月,
      COUNT(DISTINCT customer_code) AS 新規納品_得意先数,
      COUNT(DISTINCT yj_code) AS 新規納品_YJ数
    FROM {NEW_DELIV_MONTHLY}
    GROUP BY 月
    ORDER BY 月 DESC
    LIMIT 24
    """

# -----------------------------
# Customer search (Python side, zero-BQ after initial load)
# -----------------------------
@st.cache_data(ttl=24*3600, show_spinner=False)
def load_customer_candidates() -> pd.DataFrame:
    # adminでもsalesでも「候補リスト」にはこれを使う（軽い）
    # ※存在しない環境でも落ちないように try で返す
    sql = f"""
    SELECT
      CAST(customer_code AS STRING) AS customer_code,
      customer_name,
      staff_name,
      branch_name
    FROM {VIEW_CUSTOMER_STAFF}
    """
    df, err = bq_try_query_df(sql)
    if err or df is None or df.empty:
        # 最低限、factから候補を作る（やや重いが1日キャッシュ）
        sql2 = f"""
        SELECT
          customer_code,
          ANY_VALUE(customer_name) AS customer_name,
          ANY_VALUE(staff_name) AS staff_name,
          ANY_VALUE(branch_name) AS branch_name
        FROM {FACT_LOGIN_DAILY}
        GROUP BY customer_code
        """
        df2 = bq_query_df(sql2)
        df2["customer_code"] = df2["customer_code"].astype(str)
        return df2

    df["customer_code"] = df["customer_code"].astype(str)
    return df

def suggest_customers(df: pd.DataFrame, q: str) -> pd.DataFrame:
    q = (q or "").strip()
    if not q:
        return df.head(0)
    ql = safe_lower(q)
    # 部分一致（名前・コード・担当者・支店名）
    mask = (
        df["customer_name"].fillna("").str.lower().str.contains(ql)
        | df["customer_code"].fillna("").str.lower().str.contains(ql)
        | df["staff_name"].fillna("").str.lower().str.contains(ql)
        | df["branch_name"].fillna("").str.lower().str.contains(ql)
    )
    out = df.loc[mask].copy()
    # ざっくり並び：名前一致優先 → それ以外
    out["__rank"] = out["customer_name"].fillna("").str.lower().apply(lambda s: 0 if ql in s else 1)
    out = out.sort_values(["__rank", "customer_name"]).drop(columns=["__rank"])
    return out.head(MAX_SUGGEST)

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="SFA Sales OS（入口）", layout="wide")
st.title("SFA Sales OS（入口）")

# --- login ---
user_email = get_query_param_user_email()
if not user_email:
    st.error("URLに user_email を付けてください。例： ?user_email=okazaki@shinrai8.by-works.com")
    st.stop()

ctx = get_login_context(user_email)

# ヘッダ表示（英語ラベル排除）
colA, colB, colC, colD = st.columns([2.2, 1.2, 1.2, 1.4])
with colA:
    st.write(f"**ログイン：{ctx.user_email}**")
with colB:
    st.write(f"**権限：{'管理者' if (ctx.role_admin_view or ctx.role_admin_edit) else '担当者'}**")
with colC:
    st.write(f"**ロール：{ctx.role_tier or 'SALES'}**")
with colD:
    st.write(f"**エリア：{ctx.area_name or '未分類'}**")

# --- current month ---
try:
    current_month = get_current_month()
except Exception as e:
    st.error(f"current_month の取得に失敗しました：{e}")
    st.stop()

st.caption(f"対象月：{current_month.strftime('%Y-%m')}（月初: {current_month.date()}）")

# ============================================================
# 入口クエリ（最大3本）を “順番に” 実行（同時多発させない）
# ============================================================
with st.spinner("入口データを読み込み中…（高速版）"):
    df_fytd, err_fytd = bq_try_query_df(sql_fytd_org_summary())
    # 当月YoY：valid
    df_yoy_valid, err_yoy_valid = bq_try_query_df(sql_monthly_yoy_customer_rank(valid_only=True))
    # 新規納品（月次）
    df_new_month, err_new_month = bq_try_query_df(sql_new_deliveries_monthly_summary())

# ============================================================
# 管理者：全体判断（FYTD → 当月YoY → 新規納品）
# 担当者：同じ入口は見せるが、ランキングは“選択後に絞る”導線
# ============================================================

is_admin = bool(ctx.role_admin_view or ctx.role_admin_edit)

# -----------------------------
# A) 年度累計（4月〜当月末）サマリー
# -----------------------------
st.subheader("A) 年度累計（4月〜当月末）")

if err_fytd or df_fytd is None or df_fytd.empty:
    st.error(f"FYTDサマリー取得エラー：{err_fytd}")
else:
    r = df_fytd.iloc[0].to_dict()

    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1.2, 1.2, 1.2])
    with c1:
        st.metric("売上（年度累計）", jpy(r.get("売上_年度累計")))
    with c2:
        st.metric("粗利（年度累計）", jpy(r.get("粗利_年度累計")))
    with c3:
        st.metric("粗利率（年度累計）", pct(r.get("粗利率_年度累計")))
    with c4:
        st.metric("前年差（売上）", jpy(r.get("売上_前年差")))
    with c5:
        st.metric("前年差（粗利）", jpy(r.get("粗利_前年差")))

    d1, d2, d3 = st.columns([1.2, 1.2, 1.6])
    with d1:
        st.metric("昨対（売上：年度累計）", pct(r.get("売上_昨対_年度累計")))
    with d2:
        st.metric("昨対（粗利：年度累計）", pct(r.get("粗利_昨対_年度累計")))
    with d3:
        st.caption(f"集計期間：{pd.to_datetime(r.get('年度開始日')).date()} 〜 {pd.to_datetime(r.get('集計終端日')).date()}")

# -----------------------------
# B) 当月：前年同月比（上位/下位 + 比較不能）
# -----------------------------
st.subheader("B) 当月：前年同月比（上がった先・下がった先）")

if err_yoy_valid or df_yoy_valid is None:
    st.error(f"当月YoY（比較可能）取得エラー：{err_yoy_valid}")
else:
    dfv = df_yoy_valid.copy()

    # 表示用整形（重い処理はしない：列追加だけ）
    dfv["売上_当月"] = dfv["売上_当月"].astype(float)
    dfv["売上_前年差"] = dfv["売上_前年差"].astype(float)
    dfv["粗利_当月"] = dfv["粗利_当月"].astype(float)
    dfv["粗利_前年差"] = dfv["粗利_前年差"].astype(float)

    # 並び：入口は粗利額差 → 粗利率差 → 売上差（OS方針）
    # ただし “率” はpy=0で不安定なため valid のみで扱う
    df_top = dfv.sort_values(["粗利_前年差", "売上_前年差"], ascending=[False, False]).head(TOP_N)
    df_bot = dfv.sort_values(["粗利_前年差", "売上_前年差"], ascending=[True, True]).head(TOP_N)

    t1, t2 = st.columns(2)
    with t1:
        st.write("**上がった先（当月）**")
        show = df_top[[
            "得意先コード","得意先名",
            "売上_当月","売上_前年差","売上_前年同月比",
            "粗利_当月","粗利_前年差","粗利_前年同月比"
        ]].copy()
        # 表示整形（文字列化）
        show["売上_当月"] = show["売上_当月"].map(jpy)
        show["売上_前年差"] = show["売上_前年差"].map(jpy)
        show["売上_前年同月比"] = show["売上_前年同月比"].map(pct)
        show["粗利_当月"] = show["粗利_当月"].map(jpy)
        show["粗利_前年差"] = show["粗利_前年差"].map(jpy)
        show["粗利_前年同月比"] = show["粗利_前年同月比"].map(pct)
        st.dataframe(show, use_container_width=True, hide_index=True)

    with t2:
        st.write("**下がった先（当月）**")
        show = df_bot[[
            "得意先コード","得意先名",
            "売上_当月","売上_前年差","売上_前年同月比",
            "粗利_当月","粗利_前年差","粗利_前年同月比"
        ]].copy()
        show["売上_当月"] = show["売上_当月"].map(jpy)
        show["売上_前年差"] = show["売上_前年差"].map(jpy)
        show["売上_前年同月比"] = show["売上_前年同月比"].map(pct)
        show["粗利_当月"] = show["粗利_当月"].map(jpy)
        show["粗利_前年差"] = show["粗利_前年差"].map(jpy)
        show["粗利_前年同月比"] = show["粗利_前年同月比"].map(pct)
        st.dataframe(show, use_container_width=True, hide_index=True)

# 比較不能（入口で “別枠” 表示）
with st.expander("比較不能（前年同月が無い・0）", expanded=False):
    df_yoy_invalid, err_yoy_invalid = bq_try_query_df(sql_monthly_yoy_customer_rank(valid_only=False))
    if err_yoy_invalid or df_yoy_invalid is None:
        st.error(f"比較不能リスト取得エラー：{err_yoy_invalid}")
    else:
        dfi = df_yoy_invalid.copy()
        dfi = dfi.sort_values(["粗利_当月", "売上_当月"], ascending=[False, False]).head(TOP_N)
        show = dfi[["得意先コード","得意先名","売上_当月","粗利_当月"]].copy()
        show["売上_当月"] = show["売上_当月"].map(jpy)
        show["粗利_当月"] = show["粗利_当月"].map(jpy)
        st.dataframe(show, use_container_width=True, hide_index=True)

# -----------------------------
# C) 新規納品サマリー（月間）
# -----------------------------
st.subheader("C) 新規納品サマリー（月間）")

if err_new_month or df_new_month is None:
    st.error(f"新規納品（月次）取得エラー：{err_new_month}")
else:
    # 入口は軽量：24ヶ月の件数推移だけ
    show = df_new_month.copy()
    show["月"] = pd.to_datetime(show["月"]).dt.strftime("%Y-%m")
    st.dataframe(show, use_container_width=True, hide_index=True)

    # クリック後ロード（重い日次/週次は後に拡張）
    st.caption("※「昨日・週間・年間」は次フェーズで “押した人だけ” 取得にします（入口は固定で軽くします）。")

# ============================================================
# 得意先検索（候補→選択）  ※入口の基本UX（OS確定）
# ============================================================
st.subheader("得意先検索（候補から選択）")

cand = load_customer_candidates()

q = st.text_input("得意先名 / 一部文字（例：熊谷、循環器）または 得意先コード", value="")
sug = suggest_customers(cand, q)

if q and sug.empty:
    st.info("一致する候補がありません。別の文字で試してください。")
elif not sug.empty:
    # 候補を「コード + 名称 + 担当/支店」で表示
    options = []
    for _, row in sug.iterrows():
        code = str(row.get("customer_code", "") or "")
        name = str(row.get("customer_name", "") or "")
        staff = str(row.get("staff_name", "") or "")
        branch = str(row.get("branch_name", "") or "")
        label = f"{code}｜{name}"
        tail = " / ".join([x for x in [staff, branch] if x])
        if tail:
            label += f"（{tail}）"
        options.append((label, code))

    labels = [x[0] for x in options]
    sel = st.selectbox("候補一覧（選択してください）", options=[""] + labels, index=0)
    if sel:
        code = dict(options).get(sel)
        st.success(f"選択：{sel}")

        # クリック後ロード（ここから先は “必要な人だけ”）
        c1, c2 = st.columns([1, 2])
        with c1:
            do_customer_month = st.button("この得意先の当月状況を見る（軽量）")
        with c2:
            st.caption("※ここは入口を重くしないため、ボタンを押した時だけ集計します。")

        if do_customer_month:
            # 1本だけ：当月＆前年同月をその得意先に限定
            sql = f"""
            WITH sys AS (SELECT current_month FROM {VIEW_SYS_CURRENT_MONTH})
            SELECT
              f.customer_code AS 得意先コード,
              ANY_VALUE(f.customer_name) AS 得意先名,
              SUM(IF(f.month = s.current_month, f.sales_amount, 0)) AS 売上_当月,
              SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.sales_amount, 0)) AS 売上_前年同月,
              (SUM(IF(f.month = s.current_month, f.sales_amount, 0))
                - SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.sales_amount, 0))) AS 売上_前年差,
              SAFE_DIVIDE(
                SUM(IF(f.month = s.current_month, f.sales_amount, 0)),
                NULLIF(SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.sales_amount, 0)), 0)
              ) - 1 AS 売上_前年同月比,

              SUM(IF(f.month = s.current_month, f.gross_profit, 0)) AS 粗利_当月,
              SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.gross_profit, 0)) AS 粗利_前年同月,
              (SUM(IF(f.month = s.current_month, f.gross_profit, 0))
                - SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.gross_profit, 0))) AS 粗利_前年差,
              SAFE_DIVIDE(
                SUM(IF(f.month = s.current_month, f.gross_profit, 0)),
                NULLIF(SUM(IF(f.month = DATE_SUB(s.current_month, INTERVAL 1 YEAR), f.gross_profit, 0)), 0)
              ) - 1 AS 粗利_前年同月比
            FROM {FACT_LOGIN_DAILY} f
            CROSS JOIN sys s
            WHERE f.customer_code = @customer_code
              AND f.month IN (s.current_month, DATE_SUB(s.current_month, INTERVAL 1 YEAR))
            GROUP BY 得意先コード
            """
            df, err = bq_try_query_df(sql, (("customer_code", "STRING", code),))
            if err or df is None or df.empty:
                st.error(f"得意先集計エラー：{err}")
            else:
                r = df.iloc[0].to_dict()
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric("売上（当月）", jpy(r.get("売上_当月")))
                with m2:
                    st.metric("売上前年差", jpy(r.get("売上_前年差")))
                with m3:
                    st.metric("粗利（当月）", jpy(r.get("粗利_当月")))
                with m4:
                    st.metric("粗利前年差", jpy(r.get("粗利_前年差")))

                m5, m6 = st.columns(2)
                with m5:
                    st.metric("売上前年同月比", pct(r.get("売上_前年同月比")))
                with m6:
                    st.metric("粗利前年同月比", pct(r.get("粗利_前年同月比")))

# ============================================================
# チェック機構（軽量）
# ============================================================
with st.expander("チェック（軽量）", expanded=False):
    st.write("入口が重くならない範囲で、最低限の健全性だけ確認します。")

    # Fact row count / date range（あなたのログで使っていたのに合わせる）
    sql_check = f"""
    SELECT
      COUNT(1) AS n_rows,
      MIN(sales_date) AS min_date,
      MAX(sales_date) AS max_date
    FROM {FACT_LOGIN_DAILY}
    """
    df, err = bq_try_query_df(sql_check)
    if err or df is None or df.empty:
        st.error(f"Fact健全性チェック失敗：{err}")
    else:
        r = df.iloc[0].to_dict()
        st.write(f"- 行数: **{int(r.get('n_rows',0)):,}**")
        st.write(f"- 期間: **{pd.to_datetime(r.get('min_date')).date()} 〜 {pd.to_datetime(r.get('max_date')).date()}**")
        st.write(f"- current_month: **{current_month.date()}**")

st.caption("入口高速版：最初の表示は最大3クエリに制限。重い詳細はクリック後ロード。")
