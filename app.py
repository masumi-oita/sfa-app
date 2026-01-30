# app.py
# ============================================================
# SFA Sales OS（入口・判断専用／高速版）
# - FYTDサマリー（年度累計：4月〜当月）
# - 当月YoYランキング（前年同月比）
# - 新規納品サマリー（昨日/週間/月間/年間）
# - 権限分岐（dim_staff_role）
# - 日本語表示
# - 得意先検索（部分一致→候補→選択）
#
# 方針：入口は「BigQuery 側で計算済みVIEW」を参照し、Python側は表示に徹する
# ============================================================

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery

# -----------------------------
# 基本設定（環境に合わせて変更）
# -----------------------------
BQ_PROJECT = os.getenv("BQ_PROJECT", "salesdb-479915")
BQ_DATASET = os.getenv("BQ_DATASET", "sales_data")

# 入口で使う「計算済みVIEW」
VIEW_SYS_CURRENT_MONTH = f"`{BQ_PROJECT}.{BQ_DATASET}.v_sys_current_month`"

VIEW_ADMIN_ORG_FYTD = f"`{BQ_PROJECT}.{BQ_DATASET}.v_admin_org_fytd_summary_scoped`"
VIEW_ADMIN_CUSTOMER_FYTD_TOP = f"`{BQ_PROJECT}.{BQ_DATASET}.v_admin_customer_fytd_top_named_scoped`"
VIEW_ADMIN_CUSTOMER_FYTD_BOTTOM = f"`{BQ_PROJECT}.{BQ_DATASET}.v_admin_customer_fytd_bottom_named_scoped`"

# 当月YoYランキング（既存の月次VIEWセットを利用）
VIEW_MONTH_YOY_TOP = f"`{BQ_PROJECT}.{BQ_DATASET}.v_sales_customer_yoy_top_current_month`"
VIEW_MONTH_YOY_BOTTOM = f"`{BQ_PROJECT}.{BQ_DATASET}.v_sales_customer_yoy_bottom_current_month`"
VIEW_MONTH_YOY_UNCOMPARABLE = f"`{BQ_PROJECT}.{BQ_DATASET}.v_sales_customer_yoy_uncomparable_current_month`"

# 新規納品（Realized）系（存在する前提：OSで確定済み）
VIEW_NEW_DELIVERIES_MONTHLY = f"`{BQ_PROJECT}.{BQ_DATASET}.v_new_deliveries_realized_monthly`"
VIEW_NEW_DELIVERIES_DAILY_FACT = f"`{BQ_PROJECT}.{BQ_DATASET}.v_new_deliveries_realized_daily_fact_all_months`"

# ロール（権限）テーブル
TBL_STAFF_ROLE = f"`{BQ_PROJECT}.{BQ_DATASET}.dim_staff_role`"

# 得意先マスタ（検索用：部分一致で候補提示）
TBL_CUSTOMER_MASTER = f"`{BQ_PROJECT}.{BQ_DATASET}.m_customer_master`"

# 得意先→担当付与済みDIM（担当者名表示の補助に使う場合）
VIEW_DIM_CUSTOMER_STAFF = f"`{BQ_PROJECT}.{BQ_DATASET}.v_dim_customer_staff_current_norm`"

# 入口は重いので、表示件数は制限
DEFAULT_TOP_N = 30

# -----------------------------
# Streamlit 画面設定
# -----------------------------
st.set_page_config(
    page_title="SFA Sales OS（入口）",
    page_icon="📊",
    layout="wide",
)

st.title("SFA Sales OS（入口）")

# 英語っぽい表示は避ける（画面に余計な文字を出さない）
st.caption("年度累計（FYTD）→ 当月の前年同月比（YoY）→ 新規納品（昨日/週/月/年） の順で確認できます。")


# -----------------------------
# BigQuery クライアント
# -----------------------------
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


# -----------------------------
# 共通：安全なクエリ実行（キャッシュ）
# ※ st.cache_data の中で session_state を触ると StreamlitAPIException になります。
# ※ 戻り値は DataFrame のみにし、Client等の非シリアライズ物は返さない。
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def cached_query_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig()

    query_params: List[bigquery.ScalarQueryParameter] = []
    if params:
        for k, v in params.items():
            if isinstance(v, bool):
                query_params.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
            elif isinstance(v, int):
                query_params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            elif isinstance(v, float):
                query_params.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
            elif isinstance(v, (date, datetime)):
                query_params.append(bigquery.ScalarQueryParameter(k, "DATE", v if isinstance(v, date) else v.date()))
            else:
                query_params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    if query_params:
        job_config.query_parameters = query_params

    df = client.query(sql, job_config=job_config).to_dataframe(create_bqstorage_client=True)
    return df


def query_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    # 画面側でスピナーを統一表示
    with st.spinner("読み込み中..."):
        return cached_query_df(sql, params=params)


# -----------------------------
# ログイン（user_email）
# - URLクエリ ?user_email=xxx
# - 画面入力でも可
# -----------------------------
def normalize_email(x: str) -> str:
    x = (x or "").strip()
    return x.lower()


def get_login_email() -> str:
    qp = st.query_params
    q_email = normalize_email(qp.get("user_email", "")) if isinstance(qp, dict) else ""
    if q_email:
        return q_email

    # UI入力（クエリが無いとき用）
    with st.sidebar:
        st.subheader("ログイン（暫定）")
        email = st.text_input("あなたのメールアドレス（user_email）", value="", placeholder="例：okazaki@shinrai8.by-works.com")
        return normalize_email(email)


# -----------------------------
# 権限（dim_staff_role）
# -----------------------------
def get_role_row(login_email: str) -> Optional[Dict[str, Any]]:
    if not login_email:
        return None

    sql = f"""
    SELECT
      login_email,
      role_tier,
      area_name,
      scope_type,
      scope_branches,
      role_admin_view,
      role_admin_edit,
      role_sales_view,
      decided_at,
      source,
      can_manage_roles
    FROM {TBL_STAFF_ROLE}
    WHERE login_email = @login_email
    QUALIFY ROW_NUMBER() OVER (PARTITION BY login_email ORDER BY decided_at DESC) = 1
    """
    df = query_df(sql, {"login_email": login_email})
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    return row


def require_login_and_role(login_email: str) -> Dict[str, Any]:
    if not login_email:
        st.error("ログインメール（user_email）が未指定です。URLに ?user_email=... を付けて開いてください。")
        st.stop()

    role = get_role_row(login_email)
    if role is None:
        st.error("権限が未登録です（dim_staff_role に存在しません）。管理者に登録を依頼してください。")
        st.stop()

    # NULL事故を防ぐ（テーブル定義上 not null でも念のため）
    for k in ["role_admin_view", "role_admin_edit", "role_sales_view"]:
        if role.get(k) is None:
            st.error(f"権限データが不正です：{k} が NULL です。dim_staff_role を修正してください。")
            st.stop()

    return role


# -----------------------------
# current_month の取得（v_sys_current_month）
# -----------------------------
def get_current_month() -> date:
    df = query_df(f"SELECT current_month FROM {VIEW_SYS_CURRENT_MONTH} LIMIT 1")
    if df.empty or "current_month" not in df.columns:
        st.error("current_month が取得できません（v_sys_current_month）。")
        st.stop()
    cm = df.loc[0, "current_month"]
    if isinstance(cm, pd.Timestamp):
        return cm.date()
    if isinstance(cm, datetime):
        return cm.date()
    if isinstance(cm, date):
        return cm
    # 文字列など
    return pd.to_datetime(cm).date()


# -----------------------------
# 表示ユーティリティ（日本語・整形）
# -----------------------------
def yen_fmt(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, int) and pd.isna(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return str(x)
    return f"{v:,.0f}円"


def pct_fmt(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return str(x)
    return f"{v*100:.1f}%"


def num_fmt(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return str(x)
    return f"{v:,.0f}"


def safe_col(df: pd.DataFrame, col: str, default=None):
    return df[col] if col in df.columns else default


def rename_if_exists(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    m2 = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=m2) if m2 else df


# -----------------------------
# 得意先検索（部分一致 → 候補 → 選択）
# -----------------------------
def search_customers(keyword: str, limit: int = 30) -> pd.DataFrame:
    kw = (keyword or "").strip()
    if not kw:
        return pd.DataFrame()

    # 2語以上は AND に寄せる（熊谷 循環器 など）
    tokens = [t for t in re.split(r"\s+", kw) if t]
    # 正規表現でLIKEっぽく
    # NOTE: BigQuery の LIKE は全角/半角揺れは吸収しないので、ここは最小実装
    where_parts = []
    params = {}
    for i, t in enumerate(tokens[:3]):  # 無限に増やさない
        p = f"t{i}"
        params[p] = f"%{t}%"
        where_parts.append(f"CAST(`得意先名` AS STRING) LIKE @{p}")

    where_sql = " AND ".join(where_parts) if where_parts else "FALSE"

    sql = f"""
    SELECT
      CAST(`得意先コード` AS STRING) AS customer_code,
      `得意先名` AS customer_name,
      `支店名` AS branch_name,
      `担当者名` AS staff_name,
      `EMail` AS staff_email
    FROM {TBL_CUSTOMER_MASTER}
    WHERE {where_sql}
    ORDER BY customer_name
    LIMIT {int(limit)}
    """
    return query_df(sql, params)


# -----------------------------
# ロールに応じた「見える範囲」の絞り
# 入口高速版は、原則 “scoped VIEW” を使う（BQ側で範囲制御）
# ただし、月次YoYなど scoped が無いVIEWはここで軽く絞る
# -----------------------------
def apply_scope_filter(df: pd.DataFrame, role: Dict[str, Any]) -> pd.DataFrame:
    # scope_type == "BRANCH" の場合、branch_name でフィルタ
    scope_type = (role.get("scope_type") or "").strip()
    branches = role.get("scope_branches")

    if scope_type.upper() == "BRANCH":
        if isinstance(branches, list) and branches:
            if "branch_name" in df.columns:
                return df[df["branch_name"].isin(branches)].copy()
            if "支店名" in df.columns:
                return df[df["支店名"].isin(branches)].copy()

    # ALL なら無加工
    return df


# -----------------------------
# FYTD（年度累計）表示：組織
# -----------------------------
def render_fytd_org(role: Dict[str, Any], current_month: date):
    st.subheader("年度累計（4月〜当月まで）")

    # 入口は「計算済みVIEW」参照のみ
    df = query_df(f"SELECT * FROM {VIEW_ADMIN_ORG_FYTD}")
    if df.empty:
        st.warning("年度累計サマリーが空です。VIEWの中身を確認してください。")
        return

    # 代表的な列名を想定しつつ、存在するものだけ表示
    # （VIEW側の列命名が揺れても壊れにくく）
    df = rename_if_exists(df, {
        "sales_amount_fytd": "売上（年度累計）",
        "gross_profit_fytd": "粗利（年度累計）",
        "gp_rate_fytd": "粗利率（年度累計）",
        "sales_amount_py_fytd": "売上（前年差年度累計：前年）",
        "gross_profit_py_fytd": "粗利（前年差年度累計：前年）",
        "sales_diff_fytd": "売上前年差（年度累計）",
        "gp_diff_fytd": "粗利前年差（年度累計）",
        "forecast_sales_full_year": "着地予測（売上）",
        "forecast_gp_full_year": "着地予測（粗利）",
        "current_month": "当月",
        "fiscal_year": "年度",
    })

    # 数値整形（存在する列だけ）
    metrics_cols = []
    for col in ["売上（年度累計）", "粗利（年度累計）", "売上前年差（年度累計）", "粗利前年差（年度累計）", "着地予測（売上）", "着地予測（粗利）"]:
        if col in df.columns:
            metrics_cols.append(col)

    # 1行想定
    row = df.iloc[0].to_dict()

    c1, c2, c3, c4 = st.columns(4)
    if "売上（年度累計）" in row:
        c1.metric("売上（年度累計）", yen_fmt(row["売上（年度累計）"]))
    if "粗利（年度累計）" in row:
        c2.metric("粗利（年度累計）", yen_fmt(row["粗利（年度累計）"]))
    if "粗利率（年度累計）" in row:
        c3.metric("粗利率（年度累計）", pct_fmt(row["粗利率（年度累計）"]))
    # 差額があれば4つ目に
    if "売上前年差（年度累計）" in row:
        c4.metric("売上前年差（年度累計）", yen_fmt(row["売上前年差（年度累計）"]))

    # 追加（差額や着地予測がある場合）
    extra_cols = []
    for col in ["粗利前年差（年度累計）", "着地予測（売上）", "着地予測（粗利）"]:
        if col in df.columns:
            extra_cols.append(col)

    if extra_cols:
        st.write(" ")
        ex = df[[c for c in extra_cols if c in df.columns]].copy()
        for c in ex.columns:
            if "率" in c:
                ex[c] = ex[c].apply(pct_fmt)
            else:
                ex[c] = ex[c].apply(yen_fmt)
        st.dataframe(ex, use_container_width=True, hide_index=True)


# -----------------------------
# FYTD（年度累計）上位/下位：得意先
# -----------------------------
def render_fytd_customer_rank(role: Dict[str, Any], top_n: int):
    st.subheader("年度累計：伸びている先 / 下がっている先（得意先）")

    colA, colB = st.columns(2)

    with colA:
        st.markdown("#### 伸びている先（年度累計・前年差）")
        df_top = query_df(f"SELECT * FROM {VIEW_ADMIN_CUSTOMER_FYTD_TOP} LIMIT {int(top_n)}")
        if df_top.empty:
            st.info("データがありません。")
        else:
            df_top = rename_if_exists(df_top, {
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "branch_name": "支店名",
                "sales_amount_fytd": "売上（年度累計）",
                "gross_profit_fytd": "粗利（年度累計）",
                "gp_rate_fytd": "粗利率（年度累計）",
                "sales_diff_fytd": "売上前年差（年度累計）",
                "gp_diff_fytd": "粗利前年差（年度累計）",
                "staff_name": "担当者名",
            })

            show_cols = [c for c in ["支店名", "得意先名", "売上（年度累計）", "粗利（年度累計）", "粗利率（年度累計）", "売上前年差（年度累計）", "粗利前年差（年度累計）", "担当者名"] if c in df_top.columns]
            df_show = df_top[show_cols].copy()

            for c in df_show.columns:
                if "率" in c:
                    df_show[c] = df_show[c].apply(pct_fmt)
                elif "売上" in c or "粗利" in c:
                    df_show[c] = df_show[c].apply(yen_fmt)

            st.dataframe(df_show, use_container_width=True, hide_index=True)

    with colB:
        st.markdown("#### 下がっている先（年度累計・前年差）")
        df_bottom = query_df(f"SELECT * FROM {VIEW_ADMIN_CUSTOMER_FYTD_BOTTOM} LIMIT {int(top_n)}")
        if df_bottom.empty:
            st.info("データがありません。")
        else:
            df_bottom = rename_if_exists(df_bottom, {
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "branch_name": "支店名",
                "sales_amount_fytd": "売上（年度累計）",
                "gross_profit_fytd": "粗利（年度累計）",
                "gp_rate_fytd": "粗利率（年度累計）",
                "sales_diff_fytd": "売上前年差（年度累計）",
                "gp_diff_fytd": "粗利前年差（年度累計）",
                "staff_name": "担当者名",
            })

            show_cols = [c for c in ["支店名", "得意先名", "売上（年度累計）", "粗利（年度累計）", "粗利率（年度累計）", "売上前年差（年度累計）", "粗利前年差（年度累計）", "担当者名"] if c in df_bottom.columns]
            df_show = df_bottom[show_cols].copy()

            for c in df_show.columns:
                if "率" in c:
                    df_show[c] = df_show[c].apply(pct_fmt)
                elif "売上" in c or "粗利" in c:
                    df_show[c] = df_show[c].apply(yen_fmt)

            st.dataframe(df_show, use_container_width=True, hide_index=True)


# -----------------------------
# 当月：前年同月比（YoY）ランキング
# -----------------------------
def render_month_yoy_rank(role: Dict[str, Any], top_n: int):
    st.subheader("当月：前年同月比（伸びている先 / 下がっている先）")

    # ここは scoped でないVIEWが混ざる可能性があるので、roleで軽くフィルタできるようにする
    # （ただしまずは “現状のVIEW” を信じて最小限）
    colA, colB, colC = st.columns([1, 1, 1])

    with colA:
        st.markdown("#### 伸びている先（当月・前年同月比）")
        df = query_df(f"SELECT * FROM {VIEW_MONTH_YOY_TOP} LIMIT {int(top_n)}")
        if df.empty:
            st.info("データがありません。")
        else:
            df = apply_scope_filter(df, role)
            df = rename_if_exists(df, {
                "branch_name": "支店名",
                "customer_name": "得意先名",
                "sales_amount": "売上（当月）",
                "gross_profit": "粗利（当月）",
                "gp_rate": "粗利率（当月）",
                "sales_amount_py": "売上（前年同月）",
                "gross_profit_py": "粗利（前年同月）",
                "sales_diff": "売上前年差",
                "gp_diff": "粗利前年差",
                "staff_name": "担当者名",
            })
            show_cols = [c for c in ["支店名", "得意先名", "売上（当月）", "粗利（当月）", "粗利率（当月）", "売上前年差", "粗利前年差", "担当者名"] if c in df.columns]
            df_show = df[show_cols].copy()
            for c in df_show.columns:
                if "率" in c:
                    df_show[c] = df_show[c].apply(pct_fmt)
                elif "売上" in c or "粗利" in c:
                    df_show[c] = df_show[c].apply(yen_fmt)
            st.dataframe(df_show, use_container_width=True, hide_index=True)

    with colB:
        st.markdown("#### 下がっている先（当月・前年同月比）")
        df = query_df(f"SELECT * FROM {VIEW_MONTH_YOY_BOTTOM} LIMIT {int(top_n)}")
        if df.empty:
            st.info("データがありません。")
        else:
            df = apply_scope_filter(df, role)
            df = rename_if_exists(df, {
                "branch_name": "支店名",
                "customer_name": "得意先名",
                "sales_amount": "売上（当月）",
                "gross_profit": "粗利（当月）",
                "gp_rate": "粗利率（当月）",
                "sales_amount_py": "売上（前年同月）",
                "gross_profit_py": "粗利（前年同月）",
                "sales_diff": "売上前年差",
                "gp_diff": "粗利前年差",
                "staff_name": "担当者名",
            })
            show_cols = [c for c in ["支店名", "得意先名", "売上（当月）", "粗利（当月）", "粗利率（当月）", "売上前年差", "粗利前年差", "担当者名"] if c in df.columns]
            df_show = df[show_cols].copy()
            for c in df_show.columns:
                if "率" in c:
                    df_show[c] = df_show[c].apply(pct_fmt)
                elif "売上" in c or "粗利" in c:
                    df_show[c] = df_show[c].apply(yen_fmt)
            st.dataframe(df_show, use_container_width=True, hide_index=True)

    with colC:
        st.markdown("#### 比較不能（前年同月が無い等）")
        df = query_df(f"SELECT * FROM {VIEW_MONTH_YOY_UNCOMPARABLE} LIMIT {int(top_n)}")
        if df.empty:
            st.info("データがありません。")
        else:
            df = apply_scope_filter(df, role)
            df = rename_if_exists(df, {
                "branch_name": "支店名",
                "customer_name": "得意先名",
                "sales_amount": "売上（当月）",
                "gross_profit": "粗利（当月）",
                "gp_rate": "粗利率（当月）",
                "staff_name": "担当者名",
            })
            show_cols = [c for c in ["支店名", "得意先名", "売上（当月）", "粗利（当月）", "粗利率（当月）", "担当者名"] if c in df.columns]
            df_show = df[show_cols].copy()
            for c in df_show.columns:
                if "率" in c:
                    df_show[c] = df_show[c].apply(pct_fmt)
                elif "売上" in c or "粗利" in c:
                    df_show[c] = df_show[c].apply(yen_fmt)
            st.dataframe(df_show, use_container_width=True, hide_index=True)


# -----------------------------
# 新規納品サマリー（昨日/週/月/年）
# - 入口は「小さな集計結果だけ」を取る（fact全件は取らない）
# -----------------------------
def render_new_deliveries_summary(role: Dict[str, Any], current_month: date):
    st.subheader("新規納品（Realized）サマリー")

    # 集計期間の定義（日本語でそのまま）
    today = date.today()
    yesterday = today - timedelta(days=1)
    last7_start = today - timedelta(days=7)

    # 年度（4月開始）で「年度累計」の期間を作る
    fy_start = date(current_month.year if current_month.month >= 4 else current_month.year - 1, 4, 1)
    # current_month が 2026-01-01 の場合 FY開始は 2025-04-01
    # 期間終端は「最新データ日」（BQ側 min/max を尊重）
    # ここでは current_month から「当月末」ではなく「データがある範囲」を集計対象とする

    # 新規納品はVIEW側で定義済み（返品だけの日除外など）
    # ここでは「新規納品が起きた日（realized_date 等）」を前提に集計する
    # v_new_deliveries_realized_daily_fact_all_months の列が揺れる可能性があるため、
    # “存在しやすい列” を仮定してSQLを作る（必要に応じてVIEW側を合わせる）
    #
    # 期待列（例）：
    # - realized_date（DATE） or sales_date（DATE）
    # - customer_code / customer_name
    # - yj_code
    #
    # もし列名が違う場合は、VIEWの列名に合わせてこのSQLだけ差し替えればOK

    sql_base = f"""
    WITH base AS (
      SELECT
        -- 日付列（どちらかが存在する想定）
        COALESCE(CAST(realized_date AS DATE), CAST(sales_date AS DATE)) AS d,
        CAST(customer_code AS STRING) AS customer_code,
        CAST(customer_name AS STRING) AS customer_name,
        CAST(yj_code AS STRING) AS yj_code,
        CAST(branch_name AS STRING) AS branch_name
      FROM {VIEW_NEW_DELIVERIES_DAILY_FACT}
      WHERE COALESCE(CAST(realized_date AS DATE), CAST(sales_date AS DATE)) IS NOT NULL
    ),
    scoped AS (
      SELECT * FROM base
    ),
    agg AS (
      SELECT
        '昨日' AS period,
        COUNT(DISTINCT customer_code) AS customer_cnt,
        COUNT(DISTINCT yj_code) AS item_cnt
      FROM scoped
      WHERE d = @yesterday

      UNION ALL
      SELECT
        '週間（直近7日）' AS period,
        COUNT(DISTINCT customer_code) AS customer_cnt,
        COUNT(DISTINCT yj_code) AS item_cnt
      FROM scoped
      WHERE d >= @last7_start AND d <= @today

      UNION ALL
      SELECT
        '当月（{current_month.strftime("%Y-%m")}）' AS period,
        COUNT(DISTINCT customer_code) AS customer_cnt,
        COUNT(DISTINCT yj_code) AS item_cnt
      FROM scoped
      WHERE DATE_TRUNC(d, MONTH) = @current_month

      UNION ALL
      SELECT
        '年度累計（4月〜）' AS period,
        COUNT(DISTINCT customer_code) AS customer_cnt,
        COUNT(DISTINCT yj_code) AS item_cnt
      FROM scoped
      WHERE d >= @fy_start
    )
    SELECT * FROM agg
    """

    df = query_df(sql_base, {
        "yesterday": yesterday,
        "last7_start": last7_start,
        "today": today,
        "current_month": current_month,
        "fy_start": fy_start,
    })

    if df.empty:
        st.warning("新規納品サマリーが取得できません。VIEWの列名（realized_date/sales_date 等）を確認してください。")
        return

    # 表示
    df_show = df.copy()
    df_show = rename_if_exists(df_show, {
        "period": "期間",
        "customer_cnt": "得意先数",
        "item_cnt": "品目数（YJ）",
    })
    if "得意先数" in df_show.columns:
        df_show["得意先数"] = df_show["得意先数"].apply(num_fmt)
    if "品目数（YJ）" in df_show.columns:
        df_show["品目数（YJ）"] = df_show["品目数（YJ）"].apply(num_fmt)

    st.dataframe(df_show, use_container_width=True, hide_index=True)


# -----------------------------
# 得意先検索 → 選択 → 簡易ドリル（最小）
# ※ 詳細ドリル（品目/明細）は次フェーズでも良いが、入口として最低限の導線だけ置く
# -----------------------------
def render_customer_search_and_minidrill(current_month: date):
    st.subheader("得意先検索（候補から選択）")

    kw = st.text_input("得意先名（部分一致）", value="", placeholder="例：熊谷 / 循環器 / 熊谷 循環器")
    if not kw.strip():
        st.info("キーワードを入力すると候補が表示されます。")
        return

    cand = search_customers(kw, limit=50)
    if cand.empty:
        st.warning("候補が見つかりませんでした。")
        return

    # 候補表示（先に一覧を見せる）
    show = cand.copy()
    show = rename_if_exists(show, {
        "customer_code": "得意先コード",
        "customer_name": "得意先名",
        "branch_name": "支店名",
        "staff_name": "担当者名",
        "staff_email": "担当者メール",
    })
    st.dataframe(show[["支店名", "得意先名", "得意先コード", "担当者名"]], use_container_width=True, hide_index=True)

    # 選択（候補→選択）
    options = [
        f'{r["customer_name"]}（{r["customer_code"]} / {r.get("branch_name","")}）'
        for _, r in cand.iterrows()
    ]
    selected = st.selectbox("候補から選択", options=options, index=0)
    # code抽出
    m = re.search(r"（(.+?)\s*/", selected)
    if not m:
        return
    customer_code = m.group(1).strip()

    # 簡易ドリル：当月の売上・粗利（JAN粒度ではなく「得意先合計」だけ）
    # ※ 詳細は次段で v_sales_fact_login_jan_daily から掘る
    sql = f"""
    SELECT
      customer_name,
      branch_name,
      SUM(sales_amount) AS sales_amount,
      SUM(gross_profit) AS gross_profit,
      SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gp_rate
    FROM `{BQ_PROJECT}.{BQ_DATASET}.v_sales_fact_canonical_jan_daily`
    WHERE customer_code = @customer_code
      AND month = @current_month
    GROUP BY customer_name, branch_name
    """
    df = query_df(sql, {"customer_code": customer_code, "current_month": current_month})
    if df.empty:
        st.info("当月の売上が見つかりませんでした。")
        return

    row = df.iloc[0].to_dict()
    c1, c2, c3 = st.columns(3)
    c1.metric("当月 売上", yen_fmt(row.get("sales_amount")))
    c2.metric("当月 粗利", yen_fmt(row.get("gross_profit")))
    c3.metric("当月 粗利率", pct_fmt(row.get("gp_rate")))


# -----------------------------
# メイン
# -----------------------------
login_email = get_login_email()
role = require_login_and_role(login_email)
current_month = get_current_month()

# サイドバー（情報だけ）
with st.sidebar:
    st.subheader("あなた")
    st.write(f"メール：{login_email}")
    tier = (role.get("role_tier") or "").strip()
    area = (role.get("area_name") or "").strip()
    st.write(f"ロール：{tier or '（未分類）'}")
    if area:
        st.write(f"エリア：{area}")
    st.write(f"当月：{current_month.strftime('%Y-%m')}")
    st.divider()
    st.subheader("表示件数")
    top_n = st.slider("ランキング表示件数", min_value=10, max_value=80, value=DEFAULT_TOP_N, step=5)

# 管理者入口（判断専用）
# OS上の「管理者」は現場担当ではなく、現場を動かすための分析者（統括・管理）
if bool(role.get("role_admin_view")):
    st.markdown("---")
    render_fytd_org(role, current_month)

    st.markdown("---")
    render_fytd_customer_rank(role, top_n)

    st.markdown("---")
    render_month_yoy_rank(role, top_n)

    st.markdown("---")
    render_new_deliveries_summary(role, current_month)

    st.markdown("---")
    render_customer_search_and_minidrill(current_month)

else:
    st.warning("あなたは管理者閲覧権限（role_admin_view）がありません。")
    st.stop()
