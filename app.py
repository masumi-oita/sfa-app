# app.py
# SFA Sales OS (入口) - Admin + Drill + Perf Logs
# FIX-1: Do NOT mutate st.session_state inside cached funcs
# FIX-2: st.cache_data return value must be serializable -> return (df, meta_dict)

import time
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

VIEW_SYS_CURRENT_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sys_current_month`"
VIEW_ADMIN_ORG_FYTD = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_org_fytd_summary`"
VIEW_ADMIN_TOP = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_top_named`"
VIEW_ADMIN_BOTTOM = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_bottom_named`"

VIEW_DRILL_CUST_ITEM_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_detail_by_customer_item_month`"
VIEW_DRILL_CUST_YJ_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_detail_by_customer_yj_month`"

DEFAULT_TIMEOUT_SEC = 60
DEFAULT_LIMIT = 200

st.set_page_config(page_title="SFA Sales OS（入口）", page_icon="📊", layout="wide")


# =========================
# STATE
# =========================
if "query_logs" not in st.session_state:
    st.session_state.query_logs = []
if "cache_buster" not in st.session_state:
    st.session_state.cache_buster = 0


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_float_gb(x: Optional[int]) -> float:
    if not x:
        return 0.0
    return float(x) / (1024**3)


def log_query(name: str, ok: bool, elapsed_s: float, bytes_gb: float, rows: int, job_id: str, error: str, sql: str):
    st.session_state.query_logs.append(
        {
            "ts": _now_ts(),
            "name": name,
            "ok": ok,
            "elapsed_s": round(float(elapsed_s), 3),
            "bytes_gb": round(float(bytes_gb), 3),
            "rows": int(rows),
            "job_id": job_id or "",
            "error": error or "",
            "sql": sql if len(sql) <= 4000 else (sql[:4000] + "\n-- (truncated)"),
        }
    )


def perf_enabled() -> bool:
    return bool(st.session_state.get("enable_perf_log", True))


def show_sql() -> bool:
    return bool(st.session_state.get("show_sql", False))


# =========================
# AUTH / CLIENT
# =========================
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(project=PROJECT_ID, credentials=creds, location=LOCATION)
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def run_bq_query_df(
    name: str,
    sql: str,
    params: Optional[List[bigquery.ScalarQueryParameter]] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    use_cache: bool = True,
    use_bqstorage: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Returns (df, meta_dict) where meta_dict is JSON-serializable.
    """
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(use_query_cache=use_cache)
    if params:
        job_config.query_parameters = params

    t0 = time.time()
    job_id = ""
    try:
        job = client.query(sql, job_config=job_config)
        job_id = job.job_id

        df = job.result(timeout=timeout_sec).to_dataframe(create_bqstorage_client=use_bqstorage)

        elapsed = time.time() - t0
        bytes_gb = _safe_float_gb(getattr(job, "total_bytes_processed", None))
        rows = int(len(df))

        meta = {
            "ok": True,
            "elapsed_s": float(elapsed),
            "bytes_gb": float(bytes_gb),
            "rows": int(rows),
            "job_id": job_id,
            "error": "",
            "sql": sql,
        }

        # NOTE: logging is outside cache function, but run_bq_query_df can be used non-cached too
        return df, meta

    except Exception as e:
        elapsed = time.time() - t0
        meta = {
            "ok": False,
            "elapsed_s": float(elapsed),
            "bytes_gb": 0.0,
            "rows": 0,
            "job_id": job_id,
            "error": str(e),
            "sql": sql,
        }
        return pd.DataFrame(), meta


# =========================
# CACHE LAYER
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def cached_query_df(
    cache_buster: int,
    name: str,
    sql: str,
    params_tuples: Tuple[Tuple[str, str, Any], ...],
    timeout_sec: int,
    use_cache: bool,
    use_bqstorage: bool,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Cached function must return serializable values -> (DataFrame, dict)
    """
    params: List[bigquery.ScalarQueryParameter] = []
    for ptype, pname, pval in params_tuples:
        params.append(bigquery.ScalarQueryParameter(pname, ptype, pval))

    df, meta = run_bq_query_df(
        name=name,
        sql=sql,
        params=params or None,
        timeout_sec=timeout_sec,
        use_cache=use_cache,
        use_bqstorage=use_bqstorage,
    )
    # meta is dict, serializable
    return df, meta


def query_df(
    name: str,
    sql: str,
    params: Optional[List[Tuple[str, str, Any]]] = None,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    use_cache: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    params = params or []
    params_tuples = tuple((t, n, v) for (t, n, v) in params)

    use_bqstorage = bool(st.session_state.get("use_bqstorage", False))
    disable_cache = bool(st.session_state.get("disable_data_cache", False))
    cache_buster = int(st.session_state.get("cache_buster", 0))

    if disable_cache:
        bq_params = [bigquery.ScalarQueryParameter(n, t, v) for (t, n, v) in params]
        df, meta = run_bq_query_df(
            name=name,
            sql=sql,
            params=bq_params or None,
            timeout_sec=timeout_sec,
            use_cache=use_cache,
            use_bqstorage=use_bqstorage,
        )
    else:
        df, meta = cached_query_df(
            cache_buster,
            name,
            sql,
            params_tuples,
            timeout_sec,
            use_cache,
            use_bqstorage,
        )

    # logging here (outside cache) is safe
    if perf_enabled():
        log_query(
            name=name,
            ok=bool(meta.get("ok")),
            elapsed_s=float(meta.get("elapsed_s", 0.0)),
            bytes_gb=float(meta.get("bytes_gb", 0.0)),
            rows=int(meta.get("rows", 0)),
            job_id=str(meta.get("job_id", "")),
            error=str(meta.get("error", "")),
            sql=str(meta.get("sql", "")),
        )

    return df, meta


# =========================
# UI HELPERS
# =========================
def jp_col_rename(df: pd.DataFrame) -> pd.DataFrame:
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
        "sales_month": "売上月",
    }
    return df.rename(columns={c: mapping.get(c, c) for c in df.columns})


def render_result_header(name: str, meta: Dict[str, Any]):
    if meta.get("ok"):
        st.caption(
            f"[{name}] query={float(meta.get('elapsed_s', 0.0)):.1f}s / "
            f"bytes={float(meta.get('bytes_gb', 0.0)):.2f}GB / rows={int(meta.get('rows', 0))}"
        )
        if show_sql():
            with st.expander(f"SQL: {name}", expanded=False):
                st.code(str(meta.get("sql", "")), language="sql")
    else:
        st.error(f"[{name}] ERROR: {meta.get('error')}")
        if show_sql():
            with st.expander(f"SQL: {name}", expanded=True):
                st.code(str(meta.get("sql", "")), language="sql")


# =========================
# SIDEBAR
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
if c2.button("キャッシュ無効化（再取得）"):
    st.session_state.cache_buster += 1
    st.toast("キャッシュキーを更新しました（次回から再取得）")

st.sidebar.markdown("---")
st.sidebar.caption("※遅い/固まる時：Storage API ON / キャッシュ無効化 ON / SQL表示 ON で原因特定")


# =========================
# HEADER
# =========================
st.title("SFA Sales OS（入口）")

df_month, meta_month = query_df(
    name="sys_current_month",
    sql=f"SELECT * FROM {VIEW_SYS_CURRENT_MONTH} LIMIT 1",
    timeout_sec=timeout_sec,
    use_cache=True,
)

colA, colB, colC, colD = st.columns([1, 2, 1, 1])
with colA:
    render_result_header("sys_current_month", meta_month)
    if meta_month.get("ok") and not df_month.empty:
        current_month = str(df_month.iloc[0, 0])
        st.metric("Current month", current_month)
    else:
        st.metric("Current month", "—")

with colB:
    st.metric("ログイン氏名", user_email if user_email else "（未入力）")

# 方針: 未分類・全員統括でよい（表示上の固定）
with colC:
    st.metric("role_tier", "HQ_ADMIN")
with colD:
    st.metric("area", "統括")

st.markdown("---")

tab_admin, tab_drill, tab_logs = st.tabs(["管理者入口（分析）", "ドリル（明細）", "計測ログ（遅い原因）"])


# =========================
# ADMIN
# =========================
with tab_admin:
    st.subheader("A) 年度累計（FYTD）")

    df_org, meta_org = query_df(
        name="admin_org_fytd_summary",
        sql=f"SELECT * FROM {VIEW_ADMIN_ORG_FYTD}",
        timeout_sec=timeout_sec,
        use_cache=True,
    )
    render_result_header("admin_org_fytd_summary", meta_org)

    if meta_org.get("ok") and not df_org.empty:
        df_org_jp = jp_col_rename(df_org.copy())

        cols_lower = [c.lower() for c in df_org.columns]

        def pick(*cands: str) -> Optional[str]:
            for x in cands:
                if x in cols_lower:
                    return df_org.columns[cols_lower.index(x)]
            return None

        sales_col = pick("sales_amount", "sales", "sales_total", "amount")
        gp_col = pick("gross_profit", "gp", "profit")
        gpr_col = pick("gross_profit_rate", "gp_rate", "profit_rate")

        m1, m2, m3 = st.columns(3)
        m1.metric("売上（FYTD）", f"{df_org[sales_col].fillna(0).sum():,.0f}" if sales_col else "—")
        m2.metric("粗利（FYTD）", f"{df_org[gp_col].fillna(0).sum():,.0f}" if gp_col else "—")
        if gpr_col:
            try:
                v = float(df_org[gpr_col].dropna().iloc[0])
                m3.metric("粗利率（FYTD）", f"{v*100:.1f}%")
            except Exception:
                m3.metric("粗利率（FYTD）", "—")
        else:
            m3.metric("粗利率（FYTD）", "—")

        st.dataframe(df_org_jp, use_container_width=True, height=220)
    else:
        st.warning("FYTDサマリーが取得できません（VIEWを確認）")

    st.markdown("---")
    st.subheader("B) FYTD MoM（前月差）ランキング（得意先）")

    topN = st.slider("表示件数", min_value=10, max_value=200, value=50, step=10)

    cL, cR = st.columns(2)

    with cL:
        st.markdown("### 📉 下落（FYTD 前月差）")
        df_bottom, meta_bottom = query_df(
            name="admin_customer_fytd_bottom_named",
            sql=f"SELECT * FROM {VIEW_ADMIN_BOTTOM} LIMIT @lim",
            params=[("INT64", "lim", int(topN))],
            timeout_sec=timeout_sec,
            use_cache=True,
        )
        render_result_header("admin_customer_fytd_bottom_named", meta_bottom)
        if meta_bottom.get("ok") and not df_bottom.empty:
            st.dataframe(jp_col_rename(df_bottom.copy()), use_container_width=True, height=420)
        else:
            st.info("下落データがありません。")

    with cR:
        st.markdown("### 📈 伸長（FYTD 前月差）")
        df_top, meta_top = query_df(
            name="admin_customer_fytd_top_named",
            sql=f"SELECT * FROM {VIEW_ADMIN_TOP} LIMIT @lim",
            params=[("INT64", "lim", int(topN))],
            timeout_sec=timeout_sec,
            use_cache=True,
        )
        render_result_header("admin_customer_fytd_top_named", meta_top)
        if meta_top.get("ok") and not df_top.empty:
            st.dataframe(jp_col_rename(df_top.copy()), use_container_width=True, height=420)
        else:
            st.info("伸長データがありません。")

    st.caption("※ここは“全員統括”前提で、scoped VIEW を通さずに安定稼働させています。")


# =========================
# DRILL
# =========================
with tab_drill:
    st.subheader("ドリル（得意先 → 月次 → 品目/YJ）")
    st.caption("※パラメータSQLで実行（Illegal input character 対策）")

    cust_candidates: List[Tuple[str, str]] = []
    for df in [locals().get("df_top"), locals().get("df_bottom")]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            cols = [c.lower() for c in df.columns]
            if "customer_code" in cols and "customer_name" in cols:
                ccode = df[df.columns[cols.index("customer_code")]].astype(str)
                cname = df[df.columns[cols.index("customer_name")]].astype(str)
                cust_candidates += list(zip(ccode.tolist(), cname.tolist()))
    cust_candidates = list(dict.fromkeys(cust_candidates))

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

        df_drill, meta_drill = query_df(
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
        render_result_header("drill", meta_drill)
        if meta_drill.get("ok"):
            st.dataframe(jp_col_rename(df_drill.copy()), use_container_width=True, height=520)
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

        st.markdown("#### 直近の傾向")
        ok_rate = (df_log["ok"].sum() / len(df_log)) * 100
        st.write(f"- 成功率: {ok_rate:.1f}%")
        st.write(f"- 最大時間: {df_log['elapsed_s'].max():.2f}s")
        st.write(f"- 最大bytes: {df_log['bytes_gb'].max():.2f}GB")

        if st.button("ログCSV（コピー用）"):
            st.code(df_log.to_csv(index=False), language="text")
