# app.py
# SFA Sales OS (入口) - Admin (Org summary + Top/Bottom) + Drill + Perf Logs
# - Uses non-scoped views to avoid role/area gating (as requested: 未分類OK・全員統括OK)
# - Adds check mechanisms: SQL timing, timeout, cache control, query logging, parameterized queries
# - Designed to be pasted/replaced as-is in your Streamlit repo

import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account


# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
DATASET_ID = "sales_data"
LOCATION = "asia-northeast1"

# Non-scoped (stable) views for Admin
VIEW_SYS_CURRENT_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sys_current_month`"
VIEW_ADMIN_ORG_FYTD = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_org_fytd_summary`"
VIEW_ADMIN_TOP = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_top_named`"
VIEW_ADMIN_BOTTOM = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_bottom_named`"

# Drill views (you already have these)
VIEW_DRILL_CUST_ITEM_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_detail_by_customer_item_month`"
VIEW_DRILL_CUST_YJ_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_detail_by_customer_yj_month`"

DEFAULT_TIMEOUT_SEC = 60  # UI-level timeout target (BQ job can still run; we handle UX)
DEFAULT_LIMIT = 200

st.set_page_config(
    page_title="SFA Sales OS（入口）",
    page_icon="📊",
    layout="wide",
)

# =========================
# STATE / LOGGING
# =========================
if "query_logs" not in st.session_state:
    st.session_state.query_logs = []  # List[dict]
if "cache_buster" not in st.session_state:
    st.session_state.cache_buster = 0


@dataclass
class QueryResult:
    df: pd.DataFrame
    elapsed_s: float
    bytes_processed_gb: float
    rows: int
    job_id: Optional[str]
    sql: str
    ok: bool
    error: Optional[str] = None


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_query(name: str, res: QueryResult):
    st.session_state.query_logs.append(
        {
            "ts": _now_ts(),
            "name": name,
            "ok": res.ok,
            "elapsed_s": round(res.elapsed_s, 3),
            "bytes_gb": round(res.bytes_processed_gb, 3),
            "rows": int(res.rows),
            "job_id": res.job_id or "",
            "error": res.error or "",
            "sql": res.sql if len(res.sql) <= 4000 else (res.sql[:4000] + "\n-- (truncated)"),
        }
    )


# =========================
# AUTH / CLIENT
# =========================
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """
    Uses st.secrets["gcp_service_account"] if present (recommended).
    """
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(project=PROJECT_ID, credentials=creds, location=LOCATION)
    # fallback: default credentials
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def _use_bqstorage_api() -> bool:
    return bool(st.session_state.get("use_bqstorage", False))


def _show_sql() -> bool:
    return bool(st.session_state.get("show_sql", False))


def _enable_perf_log() -> bool:
    return bool(st.session_state.get("enable_perf_log", True))


def _cache_key_suffix() -> int:
    # increments when user hits "キャッシュ無効化"
    return int(st.session_state.get("cache_buster", 0))


def _safe_float_gb(x: Optional[int]) -> float:
    if not x:
        return 0.0
    return float(x) / (1024**3)


def run_bq_query(
    name: str,
    sql: str,
    params: Optional[List[bigquery.ScalarQueryParameter]] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    use_cache: bool = True,
) -> QueryResult:
    """
    Executes BigQuery SQL with optional query parameters (prevents illegal character / injection).
    Adds timing + bytes processed + job id.
    """
    client = get_bq_client()

    job_config = bigquery.QueryJobConfig(use_query_cache=use_cache)
    if params:
        job_config.query_parameters = params

    t0 = time.time()
    job_id = None
    try:
        job = client.query(sql, job_config=job_config)
        job_id = job.job_id

        # We don't hard-cancel BQ job; we use a UI timeout for responsiveness
        # but still try to fetch within timeout.
        df = job.result(timeout=timeout_sec).to_dataframe(
            create_bqstorage_client=_use_bqstorage_api()
        )
        elapsed = time.time() - t0

        bytes_gb = _safe_float_gb(getattr(job, "total_bytes_processed", None))
        rows = int(len(df))

        res = QueryResult(
            df=df,
            elapsed_s=elapsed,
            bytes_processed_gb=bytes_gb,
            rows=rows,
            job_id=job_id,
            sql=sql,
            ok=True,
            error=None,
        )
        if _enable_perf_log():
            log_query(name, res)
        return res

    except Exception as e:
        elapsed = time.time() - t0
        res = QueryResult(
            df=pd.DataFrame(),
            elapsed_s=elapsed,
            bytes_processed_gb=0.0,
            rows=0,
            job_id=job_id,
            sql=sql,
            ok=False,
            error=str(e),
        )
        if _enable_perf_log():
            log_query(name, res)
        return res


# Cache layer (data)
@st.cache_data(show_spinner=False, ttl=300)
def cached_query(
    cache_buster: int,
    name: str,
    sql: str,
    params_tuples: Tuple[Tuple[str, str, Any], ...],
    timeout_sec: int,
    use_cache: bool,
    use_bqstorage: bool,
) -> QueryResult:
    # Rebuild params objects inside cache function
    params: List[bigquery.ScalarQueryParameter] = []
    for ptype, pname, pval in params_tuples:
        params.append(bigquery.ScalarQueryParameter(pname, ptype, pval))
    # use_bqstorage is read from st.session_state normally, but passed here to bind cache key
    st.session_state["use_bqstorage"] = use_bqstorage
    return run_bq_query(name=name, sql=sql, params=params or None, timeout_sec=timeout_sec, use_cache=use_cache)


def query_df(
    name: str,
    sql: str,
    params: Optional[List[Tuple[str, str, Any]]] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    use_cache: bool = True,
) -> QueryResult:
    """
    Wrapper that applies st.cache_data if enabled by UI.
    params: list of tuples (type, name, value) e.g. ("STRING","customer_code","123")
    """
    params = params or []
    params_tuples = tuple((t, n, v) for (t, n, v) in params)

    if st.session_state.get("disable_data_cache", False):
        # no cache
        bq_params = [bigquery.ScalarQueryParameter(n, t, v) for (t, n, v) in params]
        return run_bq_query(name, sql, bq_params or None, timeout_sec=timeout_sec, use_cache=use_cache)

    return cached_query(
        _cache_key_suffix(),
        name,
        sql,
        params_tuples,
        timeout_sec,
        use_cache,
        _use_bqstorage_api(),
    )


# =========================
# UI HELPERS
# =========================
def jp_col_rename(df: pd.DataFrame) -> pd.DataFrame:
    """
    Best-effort Japanese labels for common columns.
    If your views use different column names, they will still display as-is.
    """
    mapping = {
        "customer_code": "得意先コード",
        "customer_name": "得意先名",
        "branch_code": "支店コード",
        "branch_name": "支店名",
        "staff_code": "担当者コード",
        "staff_name": "担当者名",
        "sales_amount": "売上",
        "gross_profit": "粗利",
        "gross_profit_rate": "粗利率",
        "gp_rate": "粗利率",
        "yoy_sales_amount": "前年比（売上）",
        "yoy_gross_profit": "前年比（粗利）",
        "mom_sales_amount": "前月差（売上）",
        "mom_gross_profit": "前月差（粗利）",
        "rank": "順位",
        "fiscal_year": "年度",
        "fiscal_month": "月",
        "month": "月",
        "ym": "年月",
        "item_name": "品目名",
        "item_code": "品目コード",
        "yj_code": "YJコード",
        "jan": "JAN",
        "quantity": "数量",
    }
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols)


def render_result_header(name: str, res: QueryResult):
    if res.ok:
        st.caption(f"[{name}] query={res.elapsed_s:.1f}s / bytes={res.bytes_processed_gb:.2f}GB / rows={res.rows}")
        if _show_sql():
            with st.expander(f"SQL: {name}", expanded=False):
                st.code(res.sql, language="sql")
    else:
        st.error(f"[{name}] ERROR: {res.error}")
        if _show_sql():
            with st.expander(f"SQL: {name}", expanded=True):
                st.code(res.sql, language="sql")


# =========================
# SIDEBAR (Login / Controls)
# =========================
st.sidebar.title("ログイン")
user_email = st.sidebar.text_input("user_email（メール）", value=st.query_params.get("user_email", ""))

st.sidebar.markdown("---")
st.sidebar.toggle("高速転送（Storage API）を試す", value=False, key="use_bqstorage")
st.sidebar.toggle("チェック表示（SQL計測を表示）", value=True, key="enable_perf_log")
st.sidebar.toggle("SQLを表示（デバッグ用）", value=False, key="show_sql")
st.sidebar.toggle("データキャッシュ無効化", value=False, key="disable_data_cache")

timeout_sec = st.sidebar.number_input("BQ timeout（秒）", min_value=10, max_value=300, value=DEFAULT_TIMEOUT_SEC, step=10)

c1, c2 = st.sidebar.columns(2)
if c1.button("計測ログをクリア"):
    st.session_state.query_logs = []
    st.toast("計測ログをクリアしました")
if c2.button("キャッシュ無効化"):
    st.session_state.cache_buster += 1
    st.toast("キャッシュキーを更新しました（次回から再取得）")

st.sidebar.markdown("---")
st.sidebar.caption("※遅い/固まる時：\n- Storage API ON\n- データキャッシュ無効化 ON\n- SQL表示 ON で原因特定")

# =========================
# HEADER / HEALTH
# =========================
st.title("SFA Sales OS（入口）")

# Current month (sys)
res_month = query_df(
    name="sys_current_month",
    sql=f"SELECT * FROM {VIEW_SYS_CURRENT_MONTH} LIMIT 1",
    timeout_sec=timeout_sec,
    use_cache=True,
)
colA, colB, colC, colD = st.columns([1, 2, 1, 1])
with colA:
    render_result_header("sys_current_month", res_month)
    if res_month.ok and not res_month.df.empty:
        current_month = str(res_month.df.iloc[0, 0])
        st.metric("Current month", current_month)
    else:
        st.metric("Current month", "—")

with colB:
    st.metric("ログイン氏名", user_email if user_email else "（未入力）")

# Role display: per request, treat everyone as HQ_ADMIN (統括). Still show what table says if available.
role_tier = "HQ_ADMIN"
area_name = "統括"
with colC:
    st.metric("role_tier", role_tier)
with colD:
    st.metric("area", area_name)

st.markdown("---")

# =========================
# MAIN TABS
# =========================
tab_admin, tab_drill, tab_logs = st.tabs(["管理者入口（分析）", "ドリル（明細）", "計測ログ（遅い原因）"])


# =========================
# ADMIN: FYTD summary + Top/Bottom
# =========================
with tab_admin:
    st.subheader("A) 年度累計（FYTD）")

    res_org = query_df(
        name="admin_org_fytd_summary",
        sql=f"SELECT * FROM {VIEW_ADMIN_ORG_FYTD}",
        timeout_sec=timeout_sec,
        use_cache=True,
    )
    render_result_header("admin_org_fytd_summary", res_org)

    if res_org.ok and not res_org.df.empty:
        df_org = jp_col_rename(res_org.df.copy())

        # Show KPI-like metrics if columns exist
        # We do best-effort; if not found, show the table.
        cols = [c.lower() for c in res_org.df.columns]
        # find likely columns
        def pick(*cands: str) -> Optional[str]:
            for x in cands:
                if x in cols:
                    return res_org.df.columns[cols.index(x)]
            return None

        sales_col = pick("sales_amount", "sales", "sales_total", "amount")
        gp_col = pick("gross_profit", "gp", "profit")
        gpr_col = pick("gross_profit_rate", "gp_rate", "profit_rate")

        m1, m2, m3 = st.columns(3)
        if sales_col:
            m1.metric("売上（FYTD）", f"{res_org.df[sales_col].fillna(0).sum():,.0f}")
        else:
            m1.metric("売上（FYTD）", "—")
        if gp_col:
            m2.metric("粗利（FYTD）", f"{res_org.df[gp_col].fillna(0).sum():,.0f}")
        else:
            m2.metric("粗利（FYTD）", "—")
        if gpr_col:
            try:
                v = float(res_org.df[gpr_col].dropna().iloc[0])
                m3.metric("粗利率（FYTD）", f"{v*100:.1f}%")
            except Exception:
                m3.metric("粗利率（FYTD）", "—")
        else:
            m3.metric("粗利率（FYTD）", "—")

        st.dataframe(df_org, use_container_width=True, height=220)
    else:
        st.warning("FYTDサマリーが取得できません（VIEWを確認）")

    st.markdown("---")
    st.subheader("B) FYTD MoM（前月差）ランキング（得意先）")

    topN = st.slider("表示件数", min_value=10, max_value=200, value=50, step=10)

    cL, cR = st.columns(2)

    with cL:
        st.markdown("### 📉 下落（FYTD 前月差）")
        res_bottom = query_df(
            name="admin_customer_fytd_bottom_named",
            sql=f"SELECT * FROM {VIEW_ADMIN_BOTTOM} LIMIT @lim",
            params=[("INT64", "lim", int(topN))],
            timeout_sec=timeout_sec,
            use_cache=True,
        )
        render_result_header("admin_customer_fytd_bottom_named", res_bottom)

        if res_bottom.ok and not res_bottom.df.empty:
            df_b = jp_col_rename(res_bottom.df.copy())
            st.dataframe(df_b, use_container_width=True, height=420)
        else:
            st.info("下落データがありません。")

    with cR:
        st.markdown("### 📈 伸長（FYTD 前月差）")
        res_top = query_df(
            name="admin_customer_fytd_top_named",
            sql=f"SELECT * FROM {VIEW_ADMIN_TOP} LIMIT @lim",
            params=[("INT64", "lim", int(topN))],
            timeout_sec=timeout_sec,
            use_cache=True,
        )
        render_result_header("admin_customer_fytd_top_named", res_top)

        if res_top.ok and not res_top.df.empty:
            df_t = jp_col_rename(res_top.df.copy())
            st.dataframe(df_t, use_container_width=True, height=420)
        else:
            st.info("伸長データがありません。")

    st.caption("※ここは“管理者（統括）”前提で、scoped VIEW を通さずに安定稼働させています。")


# =========================
# DRILL: customer x item/yj month
# =========================
with tab_drill:
    st.subheader("ドリル（得意先 → 月次 → 品目/YJ）")

    st.caption("※ここは必ずパラメータSQLで実行します（Illegal input character 対策）")

    # customer picker from top/bottom if available
    cust_candidates: List[Tuple[str, str]] = []  # (code, name)
    for res in [locals().get("res_top"), locals().get("res_bottom")]:
        if isinstance(res, QueryResult) and res.ok and not res.df.empty:
            cols = [c.lower() for c in res.df.columns]
            if "customer_code" in cols and "customer_name" in cols:
                ccode = res.df[res.df.columns[cols.index("customer_code")]].astype(str)
                cname = res.df[res.df.columns[cols.index("customer_name")]].astype(str)
                cust_candidates += list(zip(ccode.tolist(), cname.tolist()))
    cust_candidates = list(dict.fromkeys(cust_candidates))  # dedup preserve order

    left, right = st.columns([2, 1])
    with left:
        if cust_candidates:
            label_map = {f"{n}（{c}）": (c, n) for c, n in cust_candidates}
            pick_label = st.selectbox("得意先（ランキングから選択）", options=list(label_map.keys()))
            customer_code, customer_name = label_map[pick_label]
        else:
            customer_code = st.text_input("得意先コード（直接入力）", value="")
            customer_name = st.text_input("得意先名（任意）", value="")
    with right:
        drill_mode = st.radio("ドリル軸", options=["得意先×品目（月次）", "得意先×YJ（月次）"], horizontal=False)

    # period controls (month start/end as DATE)
    p1, p2, p3 = st.columns([1, 1, 2])
    with p1:
        start_date = st.date_input("開始日", value=date(2025, 4, 1))
    with p2:
        end_date = st.date_input("終了日", value=date.today())
    with p3:
        limit = st.number_input("最大行数", min_value=50, max_value=5000, value=DEFAULT_LIMIT, step=50)

    run = st.button("ドリル実行", type="primary", disabled=not bool(customer_code))

    if run and customer_code:
        if drill_mode == "得意先×品目（月次）":
            sql = f"""
            SELECT *
            FROM {VIEW_DRILL_CUST_ITEM_MONTH}
            WHERE customer_code = @customer_code
              AND sales_month >= @start_date
              AND sales_month <= @end_date
            ORDER BY sales_month DESC
            LIMIT @lim
            """
        else:
            sql = f"""
            SELECT *
            FROM {VIEW_DRILL_CUST_YJ_MONTH}
            WHERE customer_code = @customer_code
              AND sales_month >= @start_date
              AND sales_month <= @end_date
            ORDER BY sales_month DESC
            LIMIT @lim
            """

        res_drill = query_df(
            name="drill",
            sql=sql,
            params=[
                ("STRING", "customer_code", str(customer_code)),
                ("DATE", "start_date", start_date),
                ("DATE", "end_date", end_date),
                ("INT64", "lim", int(limit)),
            ],
            timeout_sec=timeout_sec,
            use_cache=True,
        )
        render_result_header("drill", res_drill)
        if res_drill.ok:
            st.dataframe(jp_col_rename(res_drill.df), use_container_width=True, height=520)
        else:
            st.error("ドリル取得に失敗しました。上のエラーとSQLを確認してください。")


# =========================
# PERF LOGS
# =========================
with tab_logs:
    st.subheader("計測ログ（どのSQLが遅いか・失敗したか）")
    logs = st.session_state.query_logs
    if not logs:
        st.info("まだログはありません。管理者入口/ドリルを実行すると記録されます。")
    else:
        df_log = pd.DataFrame(logs)
        st.dataframe(df_log, use_container_width=True, height=420)

        # quick summary
        st.markdown("#### 直近の傾向")
        ok_rate = (df_log["ok"].sum() / len(df_log)) * 100
        st.write(f"- 成功率: {ok_rate:.1f}%")
        st.write(f"- 最大時間: {df_log['elapsed_s'].max():.2f}s")
        st.write(f"- 最大bytes: {df_log['bytes_gb'].max():.2f}GB")

        if st.button("ログCSVダウンロード用に表示（コピー）"):
            st.code(df_log.to_csv(index=False), language="text")
