# ============================================================
# SFA Sales OS (入口) - app.py  フルコード（チェック機構入り）
# ------------------------------------------------------------
# 目的:
#  - 「どのSQLが遅いか」を画面上に可視化（query時間 / df転送時間 / bytes processed）
#  - BigQuery Storage API を使える場合は自動利用（df転送高速化）
#  - Streamlit rerun 連発でBQを叩かない（cache + form submit）
#  - 管理者: FYTD構造 → FYTD MoM → 当月YoY → ドリル
#  - 表示は日本語、担当者は email→氏名
# ------------------------------------------------------------
# 注意:
#  - requirements.txt に推奨: google-cloud-bigquery-storage>=2.24.0
#    （無くても動くが遅くなりやすい）
# ============================================================

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st
from google.cloud import bigquery

# ----------------------------
# 基本設定
# ----------------------------
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "salesdb-479915")
DATASET = os.getenv("BQ_DATASET", "sales_data")

# VIEW / TABLE（あなたの環境に存在している前提）
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

# ----------------------------
# ページ設定
# ----------------------------
st.set_page_config(page_title="SFA Sales OS（入口）", layout="wide")

# ----------------------------
# 表示補助
# ----------------------------
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


# ----------------------------
# チェック/計測構造体
# ----------------------------
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


# セッションに計測ログを保持
if "perf_logs" not in st.session_state:
    st.session_state.perf_logs: List[QueryPerf] = []

if "cache_bust" not in st.session_state:
    st.session_state.cache_bust = 0  # 手動でキャッシュ無効化したい時に使う

# ----------------------------
# BigQuery Client（キャッシュ）
# ----------------------------
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


# ----------------------------
# BigQuery → DataFrame（チェック機構入り）
# ----------------------------
def _make_job_config(params: Optional[Dict[str, Any]]) -> bigquery.QueryJobConfig:
    qps = []
    if params:
        for k, v in params.items():
            # いったん STRING で統一（必要なら型を増やす）
            qps.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))
    return bigquery.QueryJobConfig(query_parameters=qps)


def _to_df(result, prefer_storage_api: bool) -> Tuple[pd.DataFrame, float, str]:
    """
    to_dataframe の時間と、Storage API 利用状況のnoteを返す
    """
    t1 = time.time()
    note = ""
    try:
        # Storage API を使えると転送が速い（bigquery-storage が無いと例外になることがある）
        if prefer_storage_api:
            df = result.to_dataframe(create_bqstorage_client=True)
            note = "df:StorageAPI"
        else:
            df = result.to_dataframe()
            note = "df:REST"
    except Exception as e:
        # Storage API が無い等で落ちたら REST にフォールバック
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
) -> Tuple[pd.DataFrame, QueryPerf]:
    """
    cache_data 対応（返り値に DataFrame + 計測結果を含める）
    """
    client = get_bq_client()
    t0 = time.time()

    job = client.query(sql, job_config=_make_job_config(params))
    try:
        result = job.result()  # ここが長いなら BQ 計算が重い
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
    """
    画面表示用ラッパ: 計測結果を session_state に積む + 必要なら表示
    """
    df, perf = qdf_cached(
        sql=sql,
        params=params,
        prefer_storage_api=prefer_storage_api,
        cache_bust=st.session_state.cache_bust,
    )
    perf.name = name
    st.session_state.perf_logs.append(perf)

    if show_check:
        # 画面に軽く出す（重いときの犯人特定）
        if perf.ok:
            st.caption(
                f"✅ [{perf.name}] "
                f"query={perf.query_sec:.1f}s / df={perf.df_sec:.1f}s / total={perf.total_sec:.1f}s "
                f"| bytes={perf.bytes_gb:.2f}GB | rows={perf.rows:,} | {perf.note}"
            )
        else:
            st.caption(f"❌ [{perf.name}] {perf.note} (total={perf.total_sec:.1f}s)")
    return df


# ----------------------------
# 権限・氏名解決
# ----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_staff_name_norm(login_email: str) -> str:
    df = qdf_cached(
        sql=f"""
            SELECT staff_name_norm
            FROM {V_STAFF_EMAIL_NAME}
            WHERE LOWER(login_email)=@email
            LIMIT 1
        """,
        params={"email": login_email},
        prefer_storage_api=True,
        cache_bust=0,
    )[0]
    if df.empty:
        return login_email
    return str(df.iloc[0]["staff_name_norm"])


@st.cache_data(ttl=600, show_spinner=False)
def get_role_scope(login_email: str) -> Dict[str, Any]:
    df = qdf_cached(
        sql=f"""
            SELECT role_tier, area_name, scope_type
            FROM {DIM_STAFF_ROLE}
            WHERE LOWER(login_email)=@email
            LIMIT 1
        """,
        params={"email": login_email},
        prefer_storage_api=True,
        cache_bust=0,
    )[0]
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# ----------------------------
# UI: サイドバー（ログイン/設定/チェック）
# ----------------------------
with st.sidebar:
    st.header("ログイン")

    user_email = safe_lower(st.text_input("user_email（メール）", value=safe_lower(st.query_params.get("user_email", ""))))
    if user_email:
        st.query_params["user_email"] = user_email

    prefer_storage_api = st.toggle("高速転送（Storage API）を試す", value=True)
    show_checks = st.toggle("チェック表示（SQL計測を表示）", value=True)

    col_a, col_b = st.columns(2)
    if col_a.button("計測ログをクリア"):
        st.session_state.perf_logs = []
    if col_b.button("キャッシュ無効化"):
        # キャッシュキーに噛ませて強制再取得
        st.session_state.cache_bust += 1
        st.session_state.perf_logs = []

    st.divider()
    st.caption("※5分以上遅い場合、ここをONにして『どのSQLが遅いか』を特定します。")

if not user_email:
    st.title("SFA Sales OS（入口）")
    st.info("左のサイドバーで user_email を入力してください。")
    st.stop()

# ----------------------------
# ヘッダ（対象月/氏名/ロール）
# ----------------------------
st.title("SFA Sales OS（入口）")

sys_df = qdf(
    name="sys_current_month",
    sql=f"SELECT * FROM {V_SYS_MONTH} LIMIT 1",
    params=None,
    prefer_storage_api=prefer_storage_api,
    show_check=show_checks,
)
current_month = str(sys_df.iloc[0]["current_month"]) if not sys_df.empty and "current_month" in sys_df.columns else "2026-01-01"

staff_name = get_staff_name_norm(user_email)
role = get_role_scope(user_email)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Current month", current_month)
c2.metric("ログイン", staff_name)
c3.metric("role_tier", role.get("role_tier", "-"))
c4.metric("area", role.get("area_name", "-"))

st.divider()

# ----------------------------
# タブ構成
# ----------------------------
tab_admin, tab_drill, tab_perf = st.tabs(["管理者入口（分析）", "ドリル（明細）", "計測ログ（遅い原因）"])

# ============================================================
# 管理者入口（分析）
# ============================================================
with tab_admin:
    st.subheader("A) 年度累計（FYTD）")

    # FYTD 組織サマリー（scoped）
    org = qdf(
        name="admin_org_fytd_summary_scoped",
        sql=f"""
            SELECT
              sales_amount_fytd,
              gross_profit_fytd,
              gross_profit_rate_fytd
            FROM {V_ADMIN_ORG_FYTD_SCOPED}
            WHERE viewer_email=@email
            LIMIT 1
        """,
        params={"email": user_email},
        prefer_storage_api=prefer_storage_api,
        show_check=show_checks,
    )

    if org.empty:
        st.warning("FYTDサマリーが取得できません（scoped/role を確認してください）")
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

        if not bottom.empty:
            show = bottom.copy()
            # 日本語整形
            show.rename(columns={
                "sales_amount_fytd": "FYTD売上",
                "gross_profit_fytd": "FYTD粗利",
                "sales_diff_mom": "前月差_売上",
                "gross_profit_diff_mom": "前月差_粗利",
            }, inplace=True)
            st.dataframe(show, use_container_width=True, height=520)
        else:
            st.info("データなし")

    with right:
        st.markdown("### 📈 伸長（FYTD 前月差）")
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

        if not top.empty:
            show = top.copy()
            show.rename(columns={
                "sales_amount_fytd": "FYTD売上",
                "gross_profit_fytd": "FYTD粗利",
                "sales_diff_mom": "前月差_売上",
                "gross_profit_diff_mom": "前月差_粗利",
            }, inplace=True)
            st.dataframe(show, use_container_width=True, height=520)
        else:
            st.info("データなし")

    st.divider()
    st.subheader("C) 当月 YoY（前年比較）")

    t1, t2, t3 = st.tabs(["下落（YoY valid）", "伸長（YoY valid）", "比較不能（YoY invalid）"])

    with t1:
        yoy_bottom = qdf(
            name="sales_customer_yoy_bottom_current_month",
            sql=f"""
                SELECT * FROM {V_YOY_BOTTOM}
                WHERE login_email=@email
                LIMIT 200
            """,
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_bottom, use_container_width=True, height=520)

    with t2:
        yoy_top = qdf(
            name="sales_customer_yoy_top_current_month",
            sql=f"""
                SELECT * FROM {V_YOY_TOP}
                WHERE login_email=@email
                LIMIT 200
            """,
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_top, use_container_width=True, height=520)

    with t3:
        yoy_inv = qdf(
            name="sales_customer_yoy_uncomparable_current_month",
            sql=f"""
                SELECT * FROM {V_YOY_INVALID}
                WHERE login_email=@email
                LIMIT 200
            """,
            params={"email": user_email},
            prefer_storage_api=prefer_storage_api,
            show_check=show_checks,
        )
        st.dataframe(yoy_inv, use_container_width=True, height=520)

# ============================================================
# ドリル（明細）
#   ※ rerun連発を避けるため form + submit 方式
# ============================================================
with tab_drill:
    st.subheader("得意先 → 当月 日次明細（JAN粒度）")

    with st.form("drill_form", clear_on_submit=False):
        kw = st.text_input("得意先名（部分一致）", value="")
        limit_candidates = st.slider("候補件数", 10, 200, 50)
        run = st.form_submit_button("検索 → 候補表示")

    if run and kw.strip():
        # 候補検索は「当月固定」＋「login_email固定」でスキャンを減らす
        # さらに DISTINCT の列も最小限に
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

# ============================================================
# 計測ログ（遅い原因の特定）
# ============================================================
with tab_perf:
    st.subheader("このセッションで実行されたクエリ計測ログ")

    logs = st.session_state.perf_logs
    if not logs:
        st.info("まだ計測ログがありません。左で「チェック表示」をONにして操作してください。")
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
        } for x in logs])

        # 遅い順に並べる
        df_log = df_log.sort_values(["total_sec", "bytes_gb"], ascending=[False, False])

        st.dataframe(df_log, use_container_width=True, height=520)

        st.markdown("### 判定ガイド（犯人の見分け方）")
        st.write(
            "- **query_sec が長い** → BigQuery側（VIEWのJOIN/集計/スキャン）が重い\n"
            "- **df_sec が長い** → 転送が重い（Storage API未使用の可能性大 / 結果が大きすぎる）\n"
            "- **bytes_gb が大きい** → SELECT * / month固定なし / 不要列 / 不要JOIN の可能性\n"
            "- **同じnameが何度も出る** → rerunで同じクエリを連発（form化/キャッシュ見直し）"
        )
        st.markdown("### すぐ効く改善（優先順）")
        st.write(
            "1) requirements.txt に `google-cloud-bigquery-storage` を追加して df転送高速化\n"
            "2) `SELECT *` をやめて必要列だけ\n"
            "3) ドリル候補は month/login_email で必ず絞る（このコードは対応済み）\n"
            "4) 管理者ランキング/サマリーは materialize（テーブル化）して薄いVIEWを叩く"
        )
