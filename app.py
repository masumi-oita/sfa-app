from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


# ============================================================
# CONFIG
# ============================================================
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "salesdb-479915")
DATASET = os.getenv("BQ_DATASET", "sales_data")

V_SYS_MONTH = f"`{PROJECT_ID}.{DATASET}.v_sys_current_month`"

V_ADMIN_ORG_FYTD_SCOPED = f"`{PROJECT_ID}.{DATASET}.v_admin_org_fytd_summary_scoped`"
V_ADMIN_FYTD_MOM_TOP_SCOPED = f"`{PROJECT_ID}.{DATASET}.v_admin_customer_fytd_top_named_scoped`"
V_ADMIN_FYTD_MOM_BOTTOM_SCOPED = f"`{PROJECT_ID}.{DATASET}.v_admin_customer_fytd_bottom_named_scoped`"

V_YOY_TOP = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_top_current_month`"
V_YOY_BOTTOM = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_bottom_current_month`"
V_YOY_INVALID = f"`{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_uncomparable_current_month`"

V_FACT = f"`{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily`"
V_STAFF_EMAIL_NAME = f"`{PROJECT_ID}.{DATASET}.v_staff_email_name`"
DIM_STAFF_ROLE = f"`{PROJECT_ID}.{DATASET}.dim_staff_role`"

# BigQuery timeout（SQL実行）
BQ_TIMEOUT_SEC = int(os.getenv("BQ_TIMEOUT_SEC", "60"))
# BigQuery client 初期化 timeout（★ここが今回の主犯）
BQ_CLIENT_INIT_TIMEOUT_SEC = int(os.getenv("BQ_CLIENT_INIT_TIMEOUT_SEC", "10"))


# ============================================================
# UI
# ============================================================
st.set_page_config(page_title="SFA Sales OS（入口）", layout="wide")
st.title("SFA Sales OS（入口）")  # 真っ黒回避


# ============================================================
# Utils
# ============================================================
def yen(x: Any) -> str:
    try:
        if pd.isna(x):
            return ""
        return f"¥{int(round(float(x))):,}"
    except Exception:
        return ""


def pct(x: Any) -> str:
    try:
        if pd.isna(x):
            return ""
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return ""


def safe_lower(s: Any) -> str:
    return str(s).strip().lower() if s is not None else ""


def parse_code_from_label(label: str) -> str:
    m = re.search(r"\((.+?)\)\s*$", label)
    return m.group(1) if m else label


# ============================================================
# Perf log
# ============================================================
@dataclass
class QueryPerf:
    name: str
    ok: bool
    query_sec: float
    df_sec: float
    total_sec: float
    bytes_gb: float
    rows: int
    job_id: str
    note: str


if "perf_logs" not in st.session_state:
    st.session_state.perf_logs: List[QueryPerf] = []

if "cache_bust" not in st.session_state:
    st.session_state.cache_bust = 0


# ============================================================
# BigQuery Client (★ここを強化：secrets明示 + init timeout)
# ============================================================
def _build_bq_client_strict() -> bigquery.Client:
    """
    Streamlit Cloudで bigquery.Client() のADC探索がハングすることがあるため
    st.secrets の service account を優先して明示的に作る。
    """
    # 1) Streamlit secrets に service account JSON がある場合（推奨）
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)

    # 2) 無ければ従来のADC（ただしここがハングしやすい）
    return bigquery.Client(project=PROJECT_ID)


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """
    ★ client生成を別スレッドで行い、timeoutで必ず落とす
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_build_bq_client_strict)
        try:
            return fut.result(timeout=BQ_CLIENT_INIT_TIMEOUT_SEC)
        except FuturesTimeoutError:
            raise RuntimeError(
                f"BigQuery client初期化が {BQ_CLIENT_INIT_TIMEOUT_SEC}s を超えてタイムアウトしました。"
                "（ADC探索ハングの可能性大。st.secrets['gcp_service_account'] の設定を確認してください）"
            )


def _make_job_config(params: Optional[Dict[str, Any]]) -> bigquery.QueryJobConfig:
    qps = []
    if params:
        for k, v in params.items():
            qps.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return bigquery.QueryJobConfig(query_parameters=qps)


def _to_df(result, prefer_storage_api: bool) -> Tuple[pd.DataFrame, float, str]:
    t1 = time.time()
    note = ""
    try:
        if prefer_storage_api:
            df = result.to_dataframe(create_bqstorage_client=True)
            note = "df:StorageAPI"
        else:
            df = result.to_dataframe()
            note = "df:REST"
    except Exception as e:
        df = result.to_dataframe()
        note = f"df:fallback(REST) ({type(e).__name__})"
    t2 = time.time()
    return df, (t2 - t1), note


@st.cache_data(ttl=300, show_spinner=False)
def qdf_cached(
    sql: str,
    params: Optional[Dict[str, Any]],
    prefer_storage_api: bool,
    cache_bust: int,
    timeout_sec: int,
) -> Tuple[pd.DataFrame, QueryPerf]:
    t0 = time.time()

    # ★ここで get_bq_client() が止まるなら、必ず例外で画面に出す
    client = get_bq_client()

    job = client.query(sql, job_config=_make_job_config(params))
    try:
        result = job.result(timeout=timeout_sec)
        t_query_done = time.time()

        df, df_sec, df_note = _to_df(result, prefer_storage_api=prefer_storage_api)
        t_end = time.time()

        bytes_gb = float(job.total_bytes_processed or 0) / 1e9
        perf = QueryPerf(
            name="",
            ok=True,
            query_sec=(t_query_done - t0),
            df_sec=df_sec,
            total_sec=(t_end - t0),
            bytes_gb=bytes_gb,
            rows=int(df.shape[0]),
            job_id=str(job.job_id),
            note=df_note,
        )
        return df, perf

    except Exception as e:
        t_end = time.time()
        perf = QueryPerf(
            name="",
            ok=False,
            query_sec=0.0,
            df_sec=0.0,
            total_sec=(t_end - t0),
            bytes_gb=0.0,
            rows=0,
            job_id=str(getattr(job, "job_id", "")),
            note=f"ERROR: {type(e).__name__}: {e}",
        )
        return pd.DataFrame(), perf


def qdf(
    name: str,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    prefer_storage_api: bool = True,
    show_check: bool = True,
) -> pd.DataFrame:
    df, perf = qdf_cached(
        sql=sql,
        params=params,
        prefer_storage_api=prefer_storage_api,
        cache_bust=st.session_state.cache_bust,
        timeout_sec=BQ_TIMEOUT_SEC,
    )
    perf.name = name
    st.session_state.perf_logs.append(perf)

    if show_check:
        if perf.ok:
            st.caption(
                f"✅ [{perf.name}] query={perf.query_sec:.1f}s / df={perf.df_sec:.1f}s / total={perf.total_sec:.1f}s "
                f"| bytes={perf.bytes_gb:.2f}GB | rows={perf.rows:,} | {perf.note}"
            )
        else:
            st.error(f"❌ [{perf.name}] {perf.note}")

    return df


# ============================================================
# Role / Name
# ============================================================
@st.cache_data(ttl=600, show_spinner=False)
def get_staff_name_norm(login_email: str) -> str:
    df, _ = qdf_cached(
        sql=f"""
            SELECT staff_name_norm
            FROM {V_STAFF_EMAIL_NAME}
            WHERE LOWER(login_email)=@email
            LIMIT 1
        """,
        params={"email": login_email},
        prefer_storage_api=True,
        cache_bust=0,
        timeout_sec=BQ_TIMEOUT_SEC,
    )
    if df.empty:
        return login_email
    return str(df.iloc[0]["staff_name_norm"])


@st.cache_data(ttl=600, show_spinner=False)
def get_role_scope(login_email: str) -> Dict[str, Any]:
    df, _ = qdf_cached(
        sql=f"""
            SELECT role_tier, area_name, scope_type
            FROM {DIM_STAFF_ROLE}
            WHERE LOWER(login_email)=@email
            LIMIT 1
        """,
        params={"email": login_email},
        prefer_storage_api=True,
        cache_bust=0,
        timeout_sec=BQ_TIMEOUT_SEC,
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# ============================================================
# Sidebar（rerun loop対策済み）
# ============================================================
with st.sidebar:
    st.header("ログイン")

    qp_email = safe_lower(st.query_params.get("user_email", ""))
    user_email = safe_lower(st.text_input("user_email（メール）", value=qp_email))

    if user_email and qp_email != user_email:
        st.query_params["user_email"] = user_email

    prefer_storage_api = st.toggle("高速転送（Storage API）を試す", value=True)
    show_checks = st.toggle("チェック表示（SQL計測を表示）", value=True)

    col_a, col_b = st.columns(2)
    if col_a.button("計測ログをクリア"):
        st.session_state.perf_logs = []
    if col_b.button("キャッシュ無効化"):
        st.session_state.cache_bust += 1
        st.session_state.perf_logs = []

    st.divider()
    st.caption("※真っ黒/固まる時：チェック表示ONでどこで止まってるか確認できます。")

if not user_email:
    st.info("左のサイドバーで user_email を入力してください。")
    st.stop()


# ============================================================
# Header（必ず表示）
# ============================================================
left, right = st.columns([2, 1])
with left:
    st.subheader(f"ログイン: {user_email}")
with right:
    st.caption(f"BQ timeout: {BQ_TIMEOUT_SEC}s / client init timeout: {BQ_CLIENT_INIT_TIMEOUT_SEC}s")


# ============================================================
# ★ client health check（ここで止まるなら原因は認証/ネットワーク）
# ============================================================
with st.spinner("BigQuery client 初期化チェック..."):
    try:
        _ = get_bq_client()
        st.success("BigQuery client OK")
    except Exception as e:
        st.error(f"BigQuery client 初期化に失敗: {e}")
        st.stop()


# ============================================================
# current_month 取得
# ============================================================
with st.spinner("sys_current_month 取得中..."):
    sys_df = qdf(
        name="sys_current_month",
        sql=f"SELECT * FROM {V_SYS_MONTH} LIMIT 1",
        params=None,
        prefer_storage_api=prefer_storage_api,
        show_check=show_checks,
    )

if not sys_df.empty and "current_month" in sys_df.columns:
    current_month = str(sys_df.iloc[0]["current_month"])
else:
    current_month = "2026-01-01"

staff_name = get_staff_name_norm(user_email)
role = get_role_scope(user_email)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Current month", current_month)
c2.metric("ログイン氏名", staff_name)
c3.metric("role_tier", role.get("role_tier", "-"))
c4.metric("area", role.get("area_name", "-"))

st.divider()

# ============================================================
# Tabs
# ============================================================
tab_admin, tab_drill, tab_perf = st.tabs(["管理者入口（分析）", "ドリル（明細）", "計測ログ（遅い原因）"])

with tab_admin:
    st.subheader("A) 年度累計（FYTD）")

    with st.spinner("FYTDサマリー取得中..."):
        org = qdf(
            name="admin_org_fytd_summary_scoped",
            sql=f"""
                SELECT sales_amount_fytd, gross_profit_fytd, gross_profit_rate_fytd
                FROM {V_ADMIN_ORG_FYTD_SCOPED}
                WHERE viewer_email=@email
                LIMIT 1
            """,
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )

    if org.empty:
        st.warning("FYTDサマリーが取得できません（role/area/scoped を確認）")
    else:
        r = org.iloc[0]
        k1, k2, k3 = st.columns(3)
        k1.metric("FYTD 売上", yen(r.get("sales_amount_fytd")))
        k2.metric("FYTD 粗利", yen(r.get("gross_profit_fytd")))
        k3.metric("FYTD 粗利率", pct(r.get("gross_profit_rate_fytd")))

    st.divider()
    st.subheader("B) FYTD MoM（前月差）ランキング")
    left, right = st.columns(2)

    with left:
        st.markdown("### 📉 下落（FYTD 前月差）")
        with st.spinner("下落ランキング取得中..."):
            bottom = qdf(
                name="admin_customer_fytd_bottom_named_scoped",
                sql=f"""
                    SELECT
                      得意先コード,
                      得意先名,
                      支店名,
                      sales_amount_fytd,
                      gross_profit_fytd,
                      sales_diff_mom,
                      gross_profit_diff_mom
                    FROM {V_ADMIN_FYTD_MOM_BOTTOM_SCOPED}
                    WHERE viewer_email=@email
                    ORDER BY sales_diff_mom ASC
                    LIMIT 50
                """,
                params={"email": user_email},
                prefer_storage_api=prefer_storage_api,
                show_check=show_checks,
            )
        st.dataframe(bottom, use_container_width=True, height=520)

    with right:
        st.markdown("### 📈 伸長（FYTD 前月差）")
        with st.spinner("伸長ランキング取得中..."):
            top = qdf(
                name="admin_customer_fytd_top_named_scoped",
                sql=f"""
                    SELECT
                      得意先コード,
                      得意先名,
                      支店名,
                      sales_amount_fytd,
                      gross_profit_fytd,
                      sales_diff_mom,
                      gross_profit_diff_mom
                    FROM {V_ADMIN_FYTD_MOM_TOP_SCOPED}
                    WHERE viewer_email=@email
                    ORDER BY sales_diff_mom DESC
                    LIMIT 50
                """,
                params={"email": user_email},
                prefer_storage_api=prefer_storage_api,
                show_check=show_checks,
            )
        st.dataframe(top, use_container_width=True, height=520)

    st.divider()
    st.subheader("C) 当月 YoY（前年比較）")
    t1, t2, t3 = st.tabs(["下落（YoY valid）", "伸長（YoY valid）", "比較不能（YoY invalid）"])

    with t1:
        yoy_bottom = qdf(
            name="sales_customer_yoy_bottom_current_month",
            sql=f"SELECT * FROM {V_YOY_BOTTOM} WHERE login_email=@email LIMIT 200",
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_bottom, use_container_width=True, height=520)

    with t2:
        yoy_top = qdf(
            name="sales_customer_yoy_top_current_month",
            sql=f"SELECT * FROM {V_YOY_TOP} WHERE login_email=@email LIMIT 200",
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_top, use_container_width=True, height=520)

    with t3:
        yoy_inv = qdf(
            name="sales_customer_yoy_uncomparable_current_month",
            sql=f"SELECT * FROM {V_YOY_INVALID} WHERE login_email=@email LIMIT 200",
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_inv, use_container_width=True, height=520)

with tab_drill:
    st.subheader("得意先 → 当月 日次明細（JAN粒度）")

    with st.form("drill_form", clear_on_submit=False):
        kw = st.text_input("得意先名（部分一致）", value="")
        limit_candidates = st.slider("候補件数", 10, 200, 50)
        run = st.form_submit_button("検索 → 候補表示")

    if run and kw.strip():
        cand = qdf(
            name="drill_candidates",
            sql=f"""
                SELECT DISTINCT customer_code, customer_name
                FROM {V_FACT}
                WHERE login_email=@email
                  AND month=DATE(@m)
                  AND customer_name LIKE CONCAT('%', @kw, '%')
                LIMIT {int(limit_candidates)}
            """,
            params={"email": user_email, "m": current_month, "kw": kw.strip()},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )

        if cand.empty:
            st.info("候補なし")
        else:
            labels = cand.apply(lambda r: f"{r['customer_name']} ({r['customer_code']})", axis=1).tolist()
            pick = st.selectbox("得意先選択", labels)
            code = parse_code_from_label(pick)

            with st.form("detail_form", clear_on_submit=False):
                limit_rows = st.slider("表示行数", 100, 5000, 800)
                run_detail = st.form_submit_button("当月の明細を表示")

            if run_detail:
                detail = qdf(
                    name="drill_detail_daily",
                    sql=f"""
                        SELECT
                          sales_date AS 日付,
                          item_name  AS 商品名,
                          pack_unit  AS 包装,
                          jan        AS JAN,
                          yj_code    AS YJ,
                          quantity   AS 数量,
                          sales_amount AS 売上,
                          gross_profit AS 粗利
                        FROM {V_FACT}
                        WHERE login_email=@email
                          AND customer_code=@code
                          AND month=DATE(@m)
                        ORDER BY sales_date DESC
                        LIMIT {int(limit_rows)}
                    """,
                    params={"email": user_email, "code": code, "m": current_month},
                    prefer_storage_api=prefer_storage_api,
                    show_check=show_checks,
                )
                st.dataframe(detail, use_container_width=True, height=640)

with tab_perf:
    st.subheader("このセッションで実行されたクエリ計測ログ")

    logs = st.session_state.perf_logs
    if not logs:
        st.info("まだ計測ログがありません。")
    else:
        df_log = pd.DataFrame([{
            "name": x.name,
            "ok": x.ok,
            "query_sec": round(x.query_sec, 2),
            "df_sec": round(x.df_sec, 2),
            "total_sec": round(x.total_sec, 2),
            "bytes_gb": round(x.bytes_gb, 3),
            "rows": x.rows,
            "job_id": x.job_id,
            "note": x.note,
        } for x in logs]).sort_values(["total_sec", "bytes_gb"], ascending=[False, False])

        st.dataframe(df_log, use_container_width=True, height=520)
        st.markdown("### 見分け方")
        st.write(
            "- query_sec が長い → BigQuery側が重い（VIEWのJOIN/集計/スキャン）\n"
            "- df_sec が長い → 転送が重い（Storage API未導入 or 結果が大きすぎる）\n"
            "- bytes_gb が大きい → SELECT * / 絞り込み不足 / 不要JOIN の可能性"
        )
