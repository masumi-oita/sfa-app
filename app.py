# app.py
# =============================================================================
# SFA Sales OS（入口・高速版 / 判断専用）— 確定版
# - FYTDサマリー（年度累計＋昨年度累計＋前年差＋粗利）
# - 当月 YoYランキング（上がり/下がり）
# - 新規納品サマリー（昨日/直近7日/月間/FYTD）
# - 権限分岐（dim_staff_role を参照）
# - 日本語表示のみ
# - 得意先検索（名称入力→候補→選択）
#
# 重要：高速化のため「重い計算はすべて BigQuery VIEW 側」で実施する前提。
# =============================================================================

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st
from google.cloud import bigquery

# =============================================================================
# 設定（あなたの環境に合わせて必要なら変更）
# =============================================================================

@dataclass(frozen=True)
class BQViews:
    # 現在月（月初 DATE を返す）
    v_sys_current_month: str = "salesdb-479915.sales_data.v_sys_current_month"

    # 管理者（全社）FYTDサマリー（scoped版）
    v_admin_org_fytd_summary_scoped: str = "salesdb-479915.sales_data.v_admin_org_fytd_summary_scoped"

    # 管理者（得意先 FYTD / 当月YoY 上下：担当者名付き + スコープ済み）
    v_admin_customer_fytd_top_named_scoped: str = "salesdb-479915.sales_data.v_admin_customer_fytd_top_named_scoped"
    v_admin_customer_fytd_bottom_named_scoped: str = "salesdb-479915.sales_data.v_admin_customer_fytd_bottom_named_scoped"

    # 売上（得意先×月）YoY系（担当者入口にも使える想定）
    v_sales_customer_monthly_yoy_valid: str = "salesdb-479915.sales_data.v_sales_customer_monthly_yoy_valid"
    v_sales_customer_fytd_yoy_valid: str = "salesdb-479915.sales_data.v_sales_customer_fytd_yoy_valid"
    v_sales_customer_fytd_base: str = "salesdb-479915.sales_data.v_sales_customer_fytd_base"

    # 新規納品（月次 Realized）
    v_new_deliveries_realized_monthly: str = "salesdb-479915.sales_data.v_new_deliveries_realized_monthly"

    # 役割（最終テーブル）
    dim_staff_role: str = "salesdb-479915.sales_data.dim_staff_role"

    # 得意先×担当（検索用：2,000件程度なら一括取得してPythonフィルタが最速）
    dim_customer_staff_current: str = "salesdb-479915.sales_data.dim_customer_staff_current"


V = BQViews()

APP_TITLE = "SFA Sales OS（入口）"
TZ = "Asia/Tokyo"


# =============================================================================
# Streamlit 基本設定
# =============================================================================

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
)

st.title(APP_TITLE)
st.caption("判断専用（入口高速版）")


# =============================================================================
# ユーティリティ
# =============================================================================

def norm_email(x: str) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", "", str(x)).strip().lower()

def safe_int(x: Any) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None

def safe_float(x: Any) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

def money_fmt(x: Any) -> str:
    v = safe_float(x)
    if v is None:
        return ""
    return f"{v:,.0f}円"

def pct_fmt(x: Any) -> str:
    v = safe_float(x)
    if v is None:
        return ""
    return f"{v*100:,.1f}%"

def df_has_cols(df: pd.DataFrame, cols: Tuple[str, ...]) -> bool:
    return all(c in df.columns for c in cols)

def only_existing_cols(df: pd.DataFrame, cols: Tuple[str, ...]) -> pd.DataFrame:
    return df[[c for c in cols if c in df.columns]].copy()


# =============================================================================
# BigQuery（高速 & 安定：cache_resource で client を保持）
# =============================================================================

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    # Streamlit Cloud では secrets / サービスアカウントで自動認証される前提
    return bigquery.Client()

def _to_df(job: bigquery.job.QueryJob, use_bqstorage: bool) -> pd.DataFrame:
    # BigQuery Storage API が使えるなら高速
    # 使えない環境でも落ちないようにフォールバックする
    if use_bqstorage:
        try:
            return job.to_dataframe(create_bqstorage_client=True)
        except Exception:
            return job.to_dataframe()
    return job.to_dataframe()

@st.cache_data(ttl=600, show_spinner=False)
def query_df(sql: str, params: Tuple[Tuple[str, str, Any], ...], use_bqstorage: bool) -> pd.DataFrame:
    """
    st.cache_data で DataFrame をキャッシュ（返り値がシリアライズ可能）
    params: (name, type, value) のタプル配列
    """
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(n, t, v) for (n, t, v) in params]
    )
    job = client.query(sql, job_config=job_config)
    df = _to_df(job, use_bqstorage=use_bqstorage)
    return df


# =============================================================================
# ログイン（user_email）
# =============================================================================

with st.sidebar:
    st.subheader("ログイン")
    # URLクエリ ?user_email=... 対応
    q = st.query_params
    default_email = norm_email(q.get("user_email", "")) if isinstance(q, dict) else ""
    user_email = norm_email(st.text_input("メールアドレス（user_email）", value=default_email))
    use_bqstorage = st.toggle("高速モード（BQ Storage API）", value=True)
    st.divider()
    st.caption("※ 英語の見出しは出しません（日本語のみ）")

if not user_email:
    st.info("左のサイドバーでメールアドレス（user_email）を入力してください。")
    st.stop()


# =============================================================================
# 役割取得（dim_staff_role）
# =============================================================================

ROLE_SQL = f"""
SELECT
  login_email,
  role_tier,
  area_name,
  scope_type,
  scope_branches,
  role_admin_view,
  role_admin_edit,
  role_sales_view
FROM `{V.dim_staff_role}`
WHERE LOWER(TRIM(login_email)) = @email
LIMIT 1
"""

role_df = query_df(
    ROLE_SQL,
    params=(("email", "STRING", user_email),),
    use_bqstorage=use_bqstorage
)

if role_df.empty:
    st.error("権限テーブルにログインメールが登録されていません（dim_staff_role）。")
    st.stop()

role = role_df.iloc[0].to_dict()
is_admin_view = bool(role.get("role_admin_view", False))
is_admin_edit = bool(role.get("role_admin_edit", False))
is_sales_view = bool(role.get("role_sales_view", False))

# あなたの運用：未分類・全員統括でもOK（ただしrole_admin_viewが優先）
role_tier = str(role.get("role_tier", "") or "")
area_name = str(role.get("area_name", "") or "")

# 表示上はシンプルに
with st.sidebar:
    st.subheader("権限")
    tags = []
    if is_admin_view:
        tags.append("管理（閲覧）")
    if is_admin_edit:
        tags.append("管理（編集）")
    if is_sales_view:
        tags.append("現場（閲覧）")
    st.write(" / ".join(tags) if tags else "権限なし")
    if role_tier:
        st.caption(f"ロール：{role_tier}")
    if area_name:
        st.caption(f"エリア：{area_name}")


# =============================================================================
# 現在月取得（v_sys_current_month）
# =============================================================================

CUR_SQL = f"SELECT current_month FROM `{V.v_sys_current_month}` LIMIT 1"
cur_df = query_df(CUR_SQL, params=tuple(), use_bqstorage=use_bqstorage)
if cur_df.empty or "current_month" not in cur_df.columns:
    st.error("current_month が取得できません（v_sys_current_month）。")
    st.stop()

current_month = pd.to_datetime(cur_df.loc[0, "current_month"]).date()

st.caption(f"対象月：{current_month.strftime('%Y-%m')}（月初基準）")


# =============================================================================
# 得意先検索（名称入力→候補→選択）— 入口の必須UI
# =============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def load_customers_for_search(use_bqstorage: bool) -> pd.DataFrame:
    # 2,000件規模なので一括取得→Python側で部分一致が最速
    sql = f"""
    SELECT
      CAST(customer_code AS STRING) AS customer_code,
      customer_name,
      staff_name,
      branch_name
    FROM `{V.dim_customer_staff_current}`
    """
    df = query_df(sql, params=tuple(), use_bqstorage=use_bqstorage)
    # 正規化列（検索用）
    df["customer_name_norm"] = df["customer_name"].astype(str)
    return df

customers_df = load_customers_for_search(use_bqstorage=use_bqstorage)

with st.expander("得意先を検索（名称入力→候補選択）", expanded=True):
    kw = st.text_input("得意先名（例：熊谷 / 循環器）", value="", placeholder="キーワードを入力すると候補を表示します")
    cand = customers_df
    if kw.strip():
        pat = re.escape(kw.strip())
        # 大小文字はそのまま（日本語前提）、部分一致
        cand = cand[cand["customer_name_norm"].str.contains(pat, na=False)]
    # 候補数を絞る（多すぎると見づらい）
    cand = cand.head(50).copy()

    if cand.empty:
        st.warning("候補がありません。別のキーワードで検索してください。")
        selected_customer_code = None
    else:
        options = [
            f'{r["customer_name"]}（{r["branch_name"]} / {r["staff_name"]}）｜{r["customer_code"]}'
            for _, r in cand.iterrows()
        ]
        sel = st.selectbox("候補リスト（選択してください）", options=options, index=0)
        selected_customer_code = sel.split("｜")[-1].strip()

    # 選択結果は session_state に保持
    if selected_customer_code:
        st.session_state["selected_customer_code"] = selected_customer_code


# =============================================================================
# 表示コンポーネント（数字カード）
# =============================================================================

def metric_row(cols, labels, values, fmts):
    for c, lab, val, fmt in zip(cols, labels, values, fmts):
        if fmt == "円":
            c.metric(lab, money_fmt(val))
        elif fmt == "%":
            c.metric(lab, pct_fmt(val))
        else:
            c.metric(lab, "" if pd.isna(val) else str(val))


# =============================================================================
# 1) FYTD サマリー（管理者は全社、担当は自分だけ）
# =============================================================================

def load_fytd_summary_admin() -> pd.DataFrame:
    sql = f"""
    SELECT * FROM `{V.v_admin_org_fytd_summary_scoped}`
    WHERE current_month = @current_month
    LIMIT 1
    """
    return query_df(sql, params=(("current_month", "DATE", str(current_month)),), use_bqstorage=use_bqstorage)

def load_fytd_summary_sales() -> pd.DataFrame:
    # 担当者入口用：FYTDベース（自分の担当分）を v_sales_customer_fytd_base から集計してもよいが
    # “入口高速”のため、まずは VIEW 側に集計済みがあるのが理想。
    # ここでは暫定として v_sales_customer_fytd_base から自分の担当だけ集計する（軽めの集計）。
    # ※重く感じる場合は「v_sales_staff_fytd_summary_scoped」を作って差し替えてください。
    sql = f"""
    SELECT
      @current_month AS current_month,
      SUM(fytd_sales_amount) AS fytd_sales_amount,
      SUM(pytd_sales_amount) AS pytd_sales_amount,
      SUM(fytd_gross_profit) AS fytd_gross_profit,
      SAFE_DIVIDE(SUM(fytd_gross_profit), NULLIF(SUM(fytd_sales_amount),0)) AS fytd_gross_profit_rate
    FROM `{V.v_sales_customer_fytd_base}`
    WHERE current_month = @current_month
      AND LOWER(TRIM(login_email)) = @email
    """
    return query_df(
        sql,
        params=(
            ("current_month", "DATE", str(current_month)),
            ("email", "STRING", user_email),
        ),
        use_bqstorage=use_bqstorage
    )

st.subheader("年度累計（4月〜当月まで）")

if is_admin_view:
    fy = load_fytd_summary_admin()
else:
    fy = load_fytd_summary_sales()

if fy.empty:
    st.warning("年度累計サマリーのデータがありません。")
else:
    row = fy.iloc[0].to_dict()

    # 期待カラム（管理者VIEW側の命名が異なる場合は、ここを合わせてください）
    # 推奨：fytd_sales_amount / pytd_sales_amount / fytd_gross_profit / fytd_gross_profit_rate
    fytd_sales = row.get("fytd_sales_amount", row.get("sales_amount_fytd", None))
    pytd_sales = row.get("pytd_sales_amount", row.get("sales_amount_pytd", None))
    diff_sales = None if (pd.isna(fytd_sales) or pd.isna(pytd_sales)) else (float(fytd_sales) - float(pytd_sales))

    fytd_gp = row.get("fytd_gross_profit", row.get("gross_profit_fytd", None))
    fytd_gpr = row.get("fytd_gross_profit_rate", row.get("gross_profit_rate_fytd", None))

    c1, c2, c3, c4, c5 = st.columns(5)
    metric_row(
        [c1, c2, c3, c4, c5],
        ["年度累計 売上", "昨年度累計 売上", "前年差（売上）", "年度累計 粗利額", "年度累計 粗利率"],
        [fytd_sales, pytd_sales, diff_sales, fytd_gp, fytd_gpr],
        ["円", "円", "円", "円", "%"]
    )


# =============================================================================
# 2) 当月 YoY ランキング（上がり/下がり）
# =============================================================================

st.subheader("当月（前年同月比）— 上がり／下がり")

def load_monthly_yoy_top_bottom_admin(limit_n: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # 既に scoped の上位/下位VIEWがある前提（最速）
    top_sql = f"""
    SELECT * FROM `{V.v_admin_customer_fytd_top_named_scoped}`
    WHERE current_month = @current_month
    LIMIT {limit_n}
    """
    bottom_sql = f"""
    SELECT * FROM `{V.v_admin_customer_fytd_bottom_named_scoped}`
    WHERE current_month = @current_month
    LIMIT {limit_n}
    """
    top = query_df(top_sql, params=(("current_month", "DATE", str(current_month)),), use_bqstorage=use_bqstorage)
    bottom = query_df(bottom_sql, params=(("current_month", "DATE", str(current_month)),), use_bqstorage=use_bqstorage)
    return top, bottom

def load_monthly_yoy_top_bottom_sales(limit_n: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # 担当者入口：v_sales_customer_monthly_yoy_valid を自分で絞って差額順（軽め）
    sql_base = f"""
    SELECT
      customer_code,
      customer_name,
      staff_name,
      branch_name,
      sales_amount AS 当月売上,
      py_sales_amount AS 前年同月売上,
      (sales_amount - py_sales_amount) AS 差額,
      gross_profit AS 当月粗利額
    FROM `{V.v_sales_customer_monthly_yoy_valid}`
    WHERE month = @current_month
      AND LOWER(TRIM(login_email)) = @email
    """
    top_sql = f"""
    WITH t AS ({sql_base})
    SELECT * FROM t
    ORDER BY 差額 DESC
    LIMIT {limit_n}
    """
    bottom_sql = f"""
    WITH t AS ({sql_base})
    SELECT * FROM t
    ORDER BY 差額 ASC
    LIMIT {limit_n}
    """
    top = query_df(top_sql, params=(("current_month", "DATE", str(current_month)), ("email", "STRING", user_email)), use_bqstorage=use_bqstorage)
    bottom = query_df(bottom_sql, params=(("current_month", "DATE", str(current_month)), ("email", "STRING", user_email)), use_bqstorage=use_bqstorage)
    return top, bottom

if is_admin_view:
    top_df, bottom_df = load_monthly_yoy_top_bottom_admin(limit_n=30)
else:
    top_df, bottom_df = load_monthly_yoy_top_bottom_sales(limit_n=30)

# 表示する列を日本語に寄せる（VIEW側で日本語列を持てるならそれが最速）
def normalize_rank_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # よくある列名を吸収
    rename_map = {
        "sales_amount": "当月売上",
        "py_sales_amount": "前年同月売上",
        "diff_sales_amount": "差額",
        "gross_profit": "当月粗利額",
        "staff_name": "担当者",
        "branch_name": "支店",
        "customer_name": "得意先名",
        "customer_code": "得意先コード",
    }
    df2 = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}).copy()
    # 表示列（最小）
    cols = ("得意先名", "支店", "担当者", "当月売上", "前年同月売上", "差額", "当月粗利額", "得意先コード")
    return only_existing_cols(df2, cols)

t1, t2 = st.columns(2)

with t1:
    st.markdown("#### 上がっている得意先（当月）")
    d = normalize_rank_df(top_df)
    if d.empty:
        st.write("データなし")
    else:
        st.dataframe(
            d,
            use_container_width=True,
            hide_index=True,
            column_config={
                "当月売上": st.column_config.NumberColumn("当月売上", format="%,d"),
                "前年同月売上": st.column_config.NumberColumn("前年同月売上", format="%,d"),
                "差額": st.column_config.NumberColumn("差額", format="%,d"),
                "当月粗利額": st.column_config.NumberColumn("当月粗利額", format="%,d"),
            },
        )

with t2:
    st.markdown("#### 下がっている得意先（当月）")
    d = normalize_rank_df(bottom_df)
    if d.empty:
        st.write("データなし")
    else:
        st.dataframe(
            d,
            use_container_width=True,
            hide_index=True,
            column_config={
                "当月売上": st.column_config.NumberColumn("当月売上", format="%,d"),
                "前年同月売上": st.column_config.NumberColumn("前年同月売上", format="%,d"),
                "差額": st.column_config.NumberColumn("差額", format="%,d"),
                "当月粗利額": st.column_config.NumberColumn("当月粗利額", format="%,d"),
            },
        )


# =============================================================================
# 3) 新規納品サマリー（昨日/7日/月間/FYTD）
# =============================================================================

st.subheader("新規納品サマリー（Realized）")

# 基本：v_new_deliveries_realized_monthly がある前提（最速）
# ただし「昨日」「直近7日」は日次factが必要。
# ここでは既存の月次VIEWから
# - 月間（当月）
# - FYTD（4月〜当月）
# を確定表示し、
# 「昨日/7日」は別VIEWがある場合だけ表示（無ければ “準備中” とする）。
#
# あなたは既に日次 fact（v_new_deliveries_realized_daily_fact_all_months）を持っているので、
# それが存在する前提で昨日/7日も追加します。

DAILY_VIEW = "salesdb-479915.sales_data.v_new_deliveries_realized_daily_fact_all_months"

def load_new_deliveries_monthly_summary(scope_email: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # 当月（月間）
    month_sql = f"""
    SELECT
      COUNT(DISTINCT customer_code) AS 新規得意先数,
      COUNT(DISTINCT yj_code) AS 新規品目数,
      SUM(sales_amount) AS 売上,
      SUM(gross_profit) AS 粗利
    FROM `{V.v_new_deliveries_realized_monthly}`
    WHERE month_start = @current_month
    {"" if scope_email is None else "AND LOWER(TRIM(login_email)) = @email"}
    """
    # FYTD（4月〜当月）
    fy_sql = f"""
    SELECT
      COUNT(DISTINCT customer_code) AS 新規得意先数,
      COUNT(DISTINCT yj_code) AS 新規品目数,
      SUM(sales_amount) AS 売上,
      SUM(gross_profit) AS 粗利
    FROM `{V.v_new_deliveries_realized_monthly}`
    WHERE month_start BETWEEN DATE_TRUNC(DATE_SUB(@current_month, INTERVAL 9 MONTH), MONTH) AND @current_month
      AND EXTRACT(MONTH FROM @current_month) <= 3
      OR (month_start BETWEEN DATE_TRUNC(@current_month, YEAR) + INTERVAL 3 MONTH AND @current_month
          AND EXTRACT(MONTH FROM @current_month) >= 4)
    {"" if scope_email is None else "AND LOWER(TRIM(login_email)) = @email"}
    """
    # FY開始（4/1）を正確に出すため、current_monthからFY開始日を算出して BETWEEN する方が安全
    fy_sql = f"""
    WITH p AS (
      SELECT
        @current_month AS current_month,
        CASE
          WHEN EXTRACT(MONTH FROM @current_month) >= 4 THEN DATE(EXTRACT(YEAR FROM @current_month), 4, 1)
          ELSE DATE(EXTRACT(YEAR FROM @current_month) - 1, 4, 1)
        END AS fy_start
    )
    SELECT
      COUNT(DISTINCT customer_code) AS 新規得意先数,
      COUNT(DISTINCT yj_code) AS 新規品目数,
      SUM(sales_amount) AS 売上,
      SUM(gross_profit) AS 粗利
    FROM `{V.v_new_deliveries_realized_monthly}` n
    CROSS JOIN p
    WHERE n.month_start BETWEEN DATE_TRUNC(p.fy_start, MONTH) AND p.current_month
    {"" if scope_email is None else "AND LOWER(TRIM(n.login_email)) = @email"}
    """

    params = [("current_month", "DATE", str(current_month))]
    if scope_email is not None:
        params.append(("email", "STRING", scope_email))

    m = query_df(month_sql, params=tuple(params), use_bqstorage=use_bqstorage)
    f = query_df(fy_sql, params=tuple(params), use_bqstorage=use_bqstorage)
    return m, f

def load_new_deliveries_daily_summary(scope_email: Optional[str]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    # 昨日 / 直近7日（v_new_deliveries_realized_daily_fact_all_months が必要）
    # もし VIEW が無い/権限が無い場合は None を返す
    base = f"""
    SELECT
      COUNT(DISTINCT customer_code) AS 新規得意先数,
      COUNT(DISTINCT yj_code) AS 新規品目数,
      SUM(sales_amount) AS 売上,
      SUM(gross_profit) AS 粗利
    FROM `{DAILY_VIEW}`
    WHERE 1=1
    {"" if scope_email is None else "AND LOWER(TRIM(login_email)) = @email"}
    """
    y_sql = f"""
    WITH p AS (SELECT DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 1 DAY) AS d)
    {base}
    AND sales_date = (SELECT d FROM p)
    """
    w_sql = f"""
    WITH p AS (
      SELECT
        DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 7 DAY) AS d0,
        DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 1 DAY) AS d1
    )
    {base}
    AND sales_date BETWEEN (SELECT d0 FROM p) AND (SELECT d1 FROM p)
    """
    params = []
    if scope_email is not None:
        params.append(("email", "STRING", scope_email))

    try:
        y = query_df(y_sql, params=tuple(params), use_bqstorage=use_bqstorage)
        w = query_df(w_sql, params=tuple(params), use_bqstorage=use_bqstorage)
        return y, w
    except Exception:
        return None, None


scope_email = None if is_admin_view else user_email

m_month, m_fy = load_new_deliveries_monthly_summary(scope_email=scope_email)
d_y, d_w = load_new_deliveries_daily_summary(scope_email=scope_email)

# 並び順：月間→直近7日→昨日→年度累計
c1, c2, c3, c4 = st.columns(4)

def show_new_box(col, title: str, df: Optional[pd.DataFrame]):
    col.markdown(f"#### {title}")
    if df is None or df.empty:
        col.write("準備中 / データなし")
        return
    r = df.iloc[0].to_dict()
    col.metric("新規得意先数", "" if pd.isna(r.get("新規得意先数")) else f'{int(r["新規得意先数"]):,}社')
    col.metric("新規品目数", "" if pd.isna(r.get("新規品目数")) else f'{int(r["新規品目数"]):,}品目')
    col.metric("売上", money_fmt(r.get("売上")))
    col.metric("粗利", money_fmt(r.get("粗利")))

show_new_box(c1, "月間（当月）", m_month)
show_new_box(c2, "直近7日", d_w)
show_new_box(c3, "昨日", d_y)
show_new_box(c4, "年度累計（FYTD）", m_fy)


# =============================================================================
# 選択した得意先の「入口ドリル（軽量）」：FYTD + 当月の概況だけ
# =============================================================================

st.subheader("選択した得意先（概況）")

selected_code = st.session_state.get("selected_customer_code", None)
if not selected_code:
    st.info("上の『得意先検索』から得意先を選択してください。")
else:
    # FYTD（得意先単体）
    fy_sql = f"""
    SELECT
      customer_code,
      customer_name,
      staff_name,
      branch_name,
      fytd_sales_amount AS 年度累計売上,
      pytd_sales_amount AS 昨年度累計売上,
      (fytd_sales_amount - pytd_sales_amount) AS 年度累計前年差,
      fytd_gross_profit AS 年度累計粗利額,
      SAFE_DIVIDE(fytd_gross_profit, NULLIF(fytd_sales_amount,0)) AS 年度累計粗利率
    FROM `{V.v_sales_customer_fytd_yoy_valid}`
    WHERE current_month = @current_month
      AND customer_code = @customer_code
      {"" if is_admin_view else "AND LOWER(TRIM(login_email)) = @email"}
    LIMIT 1
    """
    params = [
        ("current_month", "DATE", str(current_month)),
        ("customer_code", "STRING", str(selected_code)),
    ]
    if not is_admin_view:
        params.append(("email", "STRING", user_email))

    fy_one = query_df(fy_sql, params=tuple(params), use_bqstorage=use_bqstorage)

    # 当月YoY（得意先単体）
    m_sql = f"""
    SELECT
      customer_code,
      customer_name,
      sales_amount AS 当月売上,
      py_sales_amount AS 前年同月売上,
      (sales_amount - py_sales_amount) AS 当月前年差,
      gross_profit AS 当月粗利額,
      SAFE_DIVIDE(gross_profit, NULLIF(sales_amount,0)) AS 当月粗利率
    FROM `{V.v_sales_customer_monthly_yoy_valid}`
    WHERE month = @current_month
      AND customer_code = @customer_code
      {"" if is_admin_view else "AND LOWER(TRIM(login_email)) = @email"}
    LIMIT 1
    """
    m_one = query_df(m_sql, params=tuple(params), use_bqstorage=use_bqstorage)

    left, right = st.columns(2)

    with left:
        st.markdown("#### 年度累計（得意先）")
        if fy_one.empty:
            st.write("データなし")
        else:
            r = fy_one.iloc[0].to_dict()
            st.write(f'**{r.get("customer_name","")}**（{r.get("branch_name","")} / {r.get("staff_name","")}）')
            cc1, cc2, cc3 = st.columns(3)
            metric_row(
                [cc1, cc2, cc3],
                ["年度累計 売上", "年度累計 粗利額", "年度累計 粗利率"],
                [r.get("年度累計売上"), r.get("年度累計粗利額"), r.get("年度累計粗利率")],
                ["円", "円", "%"]
            )
            st.caption(f'前年差（年度累計売上）：{money_fmt(r.get("年度累計前年差"))}')

    with right:
        st.markdown("#### 当月（前年同月比）— 得意先")
        if m_one.empty:
            st.write("データなし")
        else:
            r = m_one.iloc[0].to_dict()
            cc1, cc2, cc3 = st.columns(3)
            metric_row(
                [cc1, cc2, cc3],
                ["当月 売上", "当月 粗利額", "当月 粗利率"],
                [r.get("当月売上"), r.get("当月粗利額"), r.get("当月粗利率")],
                ["円", "円", "%"]
            )
            st.caption(f'前年差（当月売上）：{money_fmt(r.get("当月前年差"))}')

st.divider()

# =============================================================================
# 注意：この app.py は “入口高速” のため、ここで明細は出しません。
# 明細（品目・日次）はドリル画面（別セクション）で追加してください。
# =============================================================================

st.caption("入口は判断専用です。根拠（明細）はドリル側で確認します。")
