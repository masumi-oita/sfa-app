# app.py
# =============================================================================
# SFA Sales OS（入口高速版 / 判断専用）
# - FYTDサマリー（今年度累計 vs 昨年度累計）
# - 当月YoYランキング（前年同月比：上がった先 / 下がった先）
# - 新規納品サマリー（昨日 / 直近7日 / 当月 / FYTD）
# - 権限分岐（dim_staff_role）
# - 日本語表示（英語ラベル排除）
# - 得意先：検索→候補→選択（コード＋名称）
# - “ずっと読み込み”対策：遅延ロード＋タイムアウト＋Storage API切替＋ヘルスチェック
# =============================================================================

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ID = "salesdb-479915"
DATASET_ID = "sales_data"

# 入口で使う「計算済み」View（BigQuery側で事前に用意している前提）
VIEW_SYS_CURRENT_MONTH = f"`{PROJECT_ID}.{DATASET_ID}.v_sys_current_month`"

# FYTD（得意先ベース・正本）
VIEW_SALES_FYTD_BASE = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_fytd_base`"
VIEW_SALES_FYTD_YOY_VALID = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_fytd_yoy_valid`"
VIEW_SALES_FYTD_YOY_INVALID = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_fytd_yoy_invalid`"

# 管理者入口（組織サマリー / TOP/BOTTOM）
# ※あなたの環境で作成済み：
# v_admin_org_fytd_summary_scoped
# v_admin_customer_fytd_top_named_scoped
# v_admin_customer_fytd_bottom_named_scoped
VIEW_ADMIN_ORG_FYTD_SCOPED = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_org_fytd_summary_scoped`"
VIEW_ADMIN_CUST_FYTD_TOP_SCOPED = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_top_named_scoped`"
VIEW_ADMIN_CUST_FYTD_BOTTOM_SCOPED = f"`{PROJECT_ID}.{DATASET_ID}.v_admin_customer_fytd_bottom_named_scoped`"

# 当月YoY（上がった/下がった/比較不能）※月次入口
VIEW_MONTH_TOP = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_yoy_top_current_month`"
VIEW_MONTH_BOTTOM = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_yoy_bottom_current_month`"
VIEW_MONTH_UNCOMPARABLE = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_customer_yoy_uncomparable_current_month`"

# 日次Fact（ドリル/新規納品集計用）
VIEW_FACT_LOGIN_JAN_DAILY = f"`{PROJECT_ID}.{DATASET_ID}.v_sales_fact_login_jan_daily`"
VIEW_NEW_DELIVERIES_DAILY_ALL = f"`{PROJECT_ID}.{DATASET_ID}.v_new_deliveries_realized_daily_fact_all_months`"

# 権限
TABLE_STAFF_ROLE = f"`{PROJECT_ID}.{DATASET_ID}.dim_staff_role`"
# dedupがあるなら優先
VIEW_STAFF_ROLE_DEDUP = f"`{PROJECT_ID}.{DATASET_ID}.v_dim_staff_role_dedup`"

# 連絡先/氏名（表示用）
VIEW_STAFF_EMAIL_NAME = f"`{PROJECT_ID}.{DATASET_ID}.v_staff_email_name`"

# 得意先DIM（検索用）
VIEW_DIM_CUSTOMER_STAFF = f"`{PROJECT_ID}.{DATASET_ID}.dim_customer_staff_current`"

APP_TITLE = "SFA Sales OS（入口）"
TZ = "Asia/Tokyo"


# =============================================================================
# UI HELPERS
# =============================================================================

def yen(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"¥{int(round(float(x))):,}"
    except Exception:
        return str(x)

def pct(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{float(x)*100:.1f}%"
    except Exception:
        return str(x)

def num(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        if abs(float(x)) >= 1000:
            return f"{float(x):,.0f}"
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)

def to_date(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    try:
        return datetime.fromisoformat(str(x)).date()
    except Exception:
        return None


# =============================================================================
# AUTH / ROLE
# =============================================================================

@dataclass
class RoleInfo:
    login_email: str
    role_tier: str
    area_name: str
    scope_type: str
    scope_branches: List[str]
    role_admin_view: bool
    role_admin_edit: bool
    role_sales_view: bool
    can_manage_roles: bool


def get_login_email() -> str:
    # URLクエリ ?user_email=xxx の暫定ログインを正式採用（OS方針）
    qs = st.query_params
    user_email = (qs.get("user_email") or qs.get("login_email") or qs.get("email") or "").strip()
    if user_email:
        return user_email

    # UI入力（URLが無い場合の保険）
    st.warning("ログインメールが指定されていません。URLに ?user_email=... を付与するか、下で入力してください。")
    user_email = st.text_input("ログインメール（user_email）", value="", placeholder="例：okazaki@shinrai8.by-works.com").strip()
    return user_email


# =============================================================================
# BIGQUERY
# =============================================================================

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    # Streamlit Cloud: st.secrets に service_account を入れている前提
    # secrets例：
    # [gcp_service_account]
    # type="service_account"
    # project_id="..."
    # private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
    sa = st.secrets.get("gcp_service_account", None)
    if sa is None:
        # ローカル/別環境
        return bigquery.Client(project=PROJECT_ID)

    creds = service_account.Credentials.from_service_account_info(dict(sa))
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


@st.cache_data(ttl=600, show_spinner=False)
def cached_query_df(sql: str, params: Optional[Dict[str, Any]], use_bqstorage: bool, timeout_sec: int) -> pd.DataFrame:
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
                dv = v if isinstance(v, date) else v.date()
                query_params.append(bigquery.ScalarQueryParameter(k, "DATE", dv))
            else:
                query_params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))

    if query_params:
        job_config.query_parameters = query_params

    job = client.query(sql, job_config=job_config)

    # “ずっと読み込み”防止：結果待ちにタイムアウトを入れる
    job.result(timeout=timeout_sec)

    # DataFrame化：Storage API は詰まる環境があるので切替可能にする
    df = job.to_dataframe(create_bqstorage_client=use_bqstorage)
    return df


def query_df(sql: str, params: Optional[Dict[str, Any]] = None, label: str = "クエリ") -> pd.DataFrame:
    use_bqstorage = st.session_state.get("use_bqstorage", False)
    timeout_sec = int(st.session_state.get("timeout_sec", 90))

    with st.status(f"{label} 実行中...", expanded=False) as status:
        t0 = time.time()
        try:
            df = cached_query_df(sql, params=params, use_bqstorage=use_bqstorage, timeout_sec=timeout_sec)
            dt = time.time() - t0
            status.update(label=f"{label} 完了（{dt:.1f}秒）", state="complete")
            return df
        except Exception as e:
            dt = time.time() - t0
            status.update(label=f"{label} 失敗（{dt:.1f}秒）", state="error")
            st.error(f"{label} でエラー：{e}")
            raise


def table_exists(fully_qualified: str) -> bool:
    # fully_qualified: `project.dataset.table`
    # INFORMATION_SCHEMA を叩かずに軽く検査：SELECT 1 LIMIT 1 をtry
    try:
        _ = query_df(f"SELECT 1 AS ok FROM {fully_qualified} LIMIT 1", label=f"存在チェック {fully_qualified}")
        return True
    except Exception:
        return False


def load_role(login_email: str) -> RoleInfo:
    # dedup view があれば優先
    role_source = VIEW_STAFF_ROLE_DEDUP if table_exists(VIEW_STAFF_ROLE_DEDUP) else TABLE_STAFF_ROLE

    sql = f"""
    SELECT
      login_email,
      COALESCE(role_tier, 'SALES') AS role_tier,
      COALESCE(area_name, '未分類') AS area_name,
      COALESCE(scope_type, 'ALL') AS scope_type,
      COALESCE(scope_branches, []) AS scope_branches,
      COALESCE(role_admin_view, FALSE) AS role_admin_view,
      COALESCE(role_admin_edit, FALSE) AS role_admin_edit,
      COALESCE(role_sales_view, TRUE) AS role_sales_view,
      COALESCE(can_manage_roles, FALSE) AS can_manage_roles
    FROM {role_source}
    WHERE login_email = @login_email
    LIMIT 1
    """
    df = query_df(sql, params={"login_email": login_email}, label="権限取得")
    if df.empty:
        # 未登録は SALES として最低限閲覧にする（OSの方針に合わせ、後で閉じる運用に移行可）
        return RoleInfo(
            login_email=login_email,
            role_tier="SALES",
            area_name="未分類",
            scope_type="ALL",
            scope_branches=[],
            role_admin_view=False,
            role_admin_edit=False,
            role_sales_view=True,
            can_manage_roles=False,
        )

    r = df.iloc[0].to_dict()
    return RoleInfo(
        login_email=str(r["login_email"]),
        role_tier=str(r["role_tier"]),
        area_name=str(r["area_name"]),
        scope_type=str(r["scope_type"]),
        scope_branches=list(r["scope_branches"]) if isinstance(r["scope_branches"], (list, tuple)) else [],
        role_admin_view=bool(r["role_admin_view"]),
        role_admin_edit=bool(r["role_admin_edit"]),
        role_sales_view=bool(r["role_sales_view"]),
        can_manage_roles=bool(r["can_manage_roles"]),
    )


def get_current_month() -> date:
    df = query_df(f"SELECT current_month FROM {VIEW_SYS_CURRENT_MONTH}", label="current_month取得")
    if df.empty:
        # 保険：今月の月初
        today = date.today()
        return date(today.year, today.month, 1)
    d = to_date(df.loc[0, "current_month"])
    return d if d else date.today().replace(day=1)


def get_latest_sales_date(current_month: date) -> date:
    sql = f"""
    SELECT MAX(sales_date) AS max_date
    FROM {VIEW_FACT_LOGIN_JAN_DAILY}
    WHERE sales_date >= @current_month
    """
    df = query_df(sql, params={"current_month": current_month}, label="最新売上日取得")
    d = to_date(df.loc[0, "max_date"]) if not df.empty else None
    return d if d else current_month


# =============================================================================
# RENDERERS (ADMIN)
# =============================================================================

def render_admin_header(role: RoleInfo, current_month: date, latest_date: date) -> None:
    st.subheader("管理者（判断専用）")
    st.caption(f"ログイン：{role.login_email} / 権限：{role.role_tier} / エリア：{role.area_name}")
    st.caption(f"current_month：{current_month.isoformat()} / 最新売上日：{latest_date.isoformat()}")


def render_fytd_org_summary(role: RoleInfo) -> None:
    st.markdown("### 年度累計サマリー（今年度累計 vs 昨年度累計）")

    # scoped view が “権限で絞り済み” の前提（role.login_email によるフィルタ）
    sql = f"""
    SELECT *
    FROM {VIEW_ADMIN_ORG_FYTD_SCOPED}
    WHERE login_email = @login_email
    LIMIT 1
    """
    df = query_df(sql, params={"login_email": role.login_email}, label="年度累計（組織）")

    if df.empty:
        st.info("データがありません（権限スコープまたは集計ビューの中身を確認してください）。")
        return

    row = df.iloc[0].to_dict()

    # 代表列名は環境差があり得るので、存在するものだけ拾う
    # よくある列名候補（あなたのViewに合わせてBigQuery側で整備推奨）
    keys = {
        "sales_fytd": ["sales_fytd", "sales_amount_fytd", "fytd_sales_amount", "sales_amount"],
        "gp_fytd": ["gp_fytd", "gross_profit_fytd", "fytd_gross_profit", "gross_profit"],
        "sales_py_fytd": ["sales_py_fytd", "sales_amount_py_fytd", "py_fytd_sales_amount"],
        "gp_py_fytd": ["gp_py_fytd", "gross_profit_py_fytd", "py_fytd_gross_profit"],
        "sales_diff": ["sales_diff", "sales_amount_diff"],
        "gp_diff": ["gp_diff", "gross_profit_diff"],
        "sales_yoy": ["sales_yoy", "sales_yoy_rate"],
        "gp_yoy": ["gp_yoy", "gp_yoy_rate", "gross_profit_yoy"],
    }

    def pick(cands: List[str]) -> Optional[Any]:
        for c in cands:
            if c in row:
                return row[c]
        return None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("売上（今年度累計）", yen(pick(keys["sales_fytd"])))
    c2.metric("粗利（今年度累計）", yen(pick(keys["gp_fytd"])))
    c3.metric("売上前年差", yen(pick(keys["sales_diff"])))
    c4.metric("粗利前年差", yen(pick(keys["gp_diff"])))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("売上（昨年度累計）", yen(pick(keys["sales_py_fytd"])))
    c6.metric("粗利（昨年度累計）", yen(pick(keys["gp_py_fytd"])))
    c7.metric("売上前年比", pct(pick(keys["sales_yoy"])))
    c8.metric("粗利前年比", pct(pick(keys["gp_yoy"])))


def render_fytd_top_bottom(role: RoleInfo, top_n: int) -> None:
    st.markdown("### 年度累計：上がった先 / 下がった先（前年差）")

    colA, colB = st.columns(2)

    # TOP
    with colA:
        st.markdown("#### 上がった先（前年差 上位）")
        sql_top = f"""
        SELECT *
        FROM {VIEW_ADMIN_CUST_FYTD_TOP_SCOPED}
        WHERE login_email = @login_email
        ORDER BY sales_diff DESC
        LIMIT @top_n
        """
        df_top = query_df(sql_top, params={"login_email": role.login_email, "top_n": int(top_n)}, label="年度累計TOP")

        if df_top.empty:
            st.info("データがありません。")
        else:
            df_show = df_top.copy()
            # 日本語列名に寄せる（存在する列だけ）
            rename_map = {
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "staff_name": "担当者",
                "branch_name": "支店名",
                "sales_fytd": "売上（今年度累計）",
                "gp_fytd": "粗利（今年度累計）",
                "sales_py_fytd": "売上（昨年度累計）",
                "gp_py_fytd": "粗利（昨年度累計）",
                "sales_diff": "売上前年差",
                "gp_diff": "粗利前年差",
                "sales_yoy": "売上前年比",
                "gp_yoy": "粗利前年比",
            }
            for k, v in list(rename_map.items()):
                if k in df_show.columns:
                    df_show.rename(columns={k: v}, inplace=True)

            for col in ["売上（今年度累計）", "粗利（今年度累計）", "売上（昨年度累計）", "粗利（昨年度累計）", "売上前年差", "粗利前年差"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda x: yen(x))
            for col in ["売上前年比", "粗利前年比"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda x: pct(x))

            st.dataframe(df_show, use_container_width=True, hide_index=True)

    # BOTTOM
    with colB:
        st.markdown("#### 下がった先（前年差 下位）")
        sql_bottom = f"""
        SELECT *
        FROM {VIEW_ADMIN_CUST_FYTD_BOTTOM_SCOPED}
        WHERE login_email = @login_email
        ORDER BY sales_diff ASC
        LIMIT @top_n
        """
        df_bottom = query_df(sql_bottom, params={"login_email": role.login_email, "top_n": int(top_n)}, label="年度累計BOTTOM")

        if df_bottom.empty:
            st.info("データがありません。")
        else:
            df_show = df_bottom.copy()
            rename_map = {
                "customer_code": "得意先コード",
                "customer_name": "得意先名",
                "staff_name": "担当者",
                "branch_name": "支店名",
                "sales_fytd": "売上（今年度累計）",
                "gp_fytd": "粗利（今年度累計）",
                "sales_py_fytd": "売上（昨年度累計）",
                "gp_py_fytd": "粗利（昨年度累計）",
                "sales_diff": "売上前年差",
                "gp_diff": "粗利前年差",
                "sales_yoy": "売上前年比",
                "gp_yoy": "粗利前年比",
            }
            for k, v in list(rename_map.items()):
                if k in df_show.columns:
                    df_show.rename(columns={k: v}, inplace=True)

            for col in ["売上（今年度累計）", "粗利（今年度累計）", "売上（昨年度累計）", "粗利（昨年度累計）", "売上前年差", "粗利前年差"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda x: yen(x))
            for col in ["売上前年比", "粗利前年比"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda x: pct(x))

            st.dataframe(df_show, use_container_width=True, hide_index=True)


def render_month_yoy_rankings(role: RoleInfo, top_n: int) -> None:
    st.markdown("### 当月：前年同月比ランキング（上がった先 / 下がった先）")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 上がった先（前年比 上位）")
        sql = f"""
        SELECT
          customer_code, customer_name, staff_name, branch_name,
          sales_amount, gross_profit,
          py_sales_amount, py_gross_profit,
          sales_diff, gp_diff,
          sales_yoy, gp_yoy
        FROM {VIEW_MONTH_TOP}
        WHERE login_email = @login_email
        ORDER BY sales_yoy DESC
        LIMIT @top_n
        """
        df = query_df(sql, params={"login_email": role.login_email, "top_n": int(top_n)}, label="当月YoY TOP")
        if df.empty:
            st.info("データがありません。")
        else:
            df2 = df.copy()
            df2.rename(
                columns={
                    "customer_code": "得意先コード",
                    "customer_name": "得意先名",
                    "staff_name": "担当者",
                    "branch_name": "支店名",
                    "sales_amount": "売上（当月）",
                    "gross_profit": "粗利（当月）",
                    "py_sales_amount": "売上（前年同月）",
                    "py_gross_profit": "粗利（前年同月）",
                    "sales_diff": "売上前年差",
                    "gp_diff": "粗利前年差",
                    "sales_yoy": "売上前年比",
                    "gp_yoy": "粗利前年比",
                },
                inplace=True,
            )
            for c in ["売上（当月）", "粗利（当月）", "売上（前年同月）", "粗利（前年同月）", "売上前年差", "粗利前年差"]:
                if c in df2.columns:
                    df2[c] = df2[c].apply(yen)
            for c in ["売上前年比", "粗利前年比"]:
                if c in df2.columns:
                    df2[c] = df2[c].apply(pct)

            st.dataframe(df2, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("#### 下がった先（前年比 下位）")
        sql = f"""
        SELECT
          customer_code, customer_name, staff_name, branch_name,
          sales_amount, gross_profit,
          py_sales_amount, py_gross_profit,
          sales_diff, gp_diff,
          sales_yoy, gp_yoy
        FROM {VIEW_MONTH_BOTTOM}
        WHERE login_email = @login_email
        ORDER BY sales_yoy ASC
        LIMIT @top_n
        """
        df = query_df(sql, params={"login_email": role.login_email, "top_n": int(top_n)}, label="当月YoY BOTTOM")
        if df.empty:
            st.info("データがありません。")
        else:
            df2 = df.copy()
            df2.rename(
                columns={
                    "customer_code": "得意先コード",
                    "customer_name": "得意先名",
                    "staff_name": "担当者",
                    "branch_name": "支店名",
                    "sales_amount": "売上（当月）",
                    "gross_profit": "粗利（当月）",
                    "py_sales_amount": "売上（前年同月）",
                    "py_gross_profit": "粗利（前年同月）",
                    "sales_diff": "売上前年差",
                    "gp_diff": "粗利前年差",
                    "sales_yoy": "売上前年比",
                    "gp_yoy": "粗利前年比",
                },
                inplace=True,
            )
            for c in ["売上（当月）", "粗利（当月）", "売上（前年同月）", "粗利（前年同月）", "売上前年差", "粗利前年差"]:
                if c in df2.columns:
                    df2[c] = df2[c].apply(yen)
            for c in ["売上前年比", "粗利前年比"]:
                if c in df2.columns:
                    df2[c] = df2[c].apply(pct)

            st.dataframe(df2, use_container_width=True, hide_index=True)

    # 比較不能（参考）
    with st.expander("比較不能（新規・前年なし）", expanded=False):
        sql = f"""
        SELECT customer_code, customer_name, staff_name, branch_name, sales_amount, gross_profit
        FROM {VIEW_MONTH_UNCOMPARABLE}
        WHERE login_email = @login_email
        ORDER BY sales_amount DESC
        LIMIT @top_n
        """
        df = query_df(sql, params={"login_email": role.login_email, "top_n": int(top_n)}, label="当月YoY 比較不能")
        if df.empty:
            st.info("データがありません。")
        else:
            df2 = df.copy()
            df2.rename(
                columns={
                    "customer_code": "得意先コード",
                    "customer_name": "得意先名",
                    "staff_name": "担当者",
                    "branch_name": "支店名",
                    "sales_amount": "売上（当月）",
                    "gross_profit": "粗利（当月）",
                },
                inplace=True,
            )
            for c in ["売上（当月）", "粗利（当月）"]:
                if c in df2.columns:
                    df2[c] = df2[c].apply(yen)
            st.dataframe(df2, use_container_width=True, hide_index=True)


def render_new_deliveries_summary(role: RoleInfo, current_month: date, latest_date: date) -> None:
    st.markdown("### 新規納品サマリー（昨日 / 直近7日 / 当月 / 年度累計）")

    # 新規納品 fact から “realized_flag=1” を拾う前提（viewの列名は環境差あり得る）
    # ここは「確実に動く」ために、存在しやすい列名で書く。
    # v_new_deliveries_realized_daily_fact_all_months に以下がある想定：
    # - sales_date（または date）
    # - customer_code
    # - yj_code
    # - realized_new_flag（または is_new_deliveries / new_flag）
    # - sales_amount / gross_profit（あれば）
    #
    # ない列があれば BigQuery 側で view の列名を揃えるのが正攻法。

    # 期間
    d_yesterday = latest_date - timedelta(days=1)
    d_7 = latest_date - timedelta(days=7)

    def agg_sql(label: str, d_from: date, d_to: date) -> str:
        return f"""
        WITH base AS (
          SELECT
            sales_date,
            customer_code,
            yj_code,
            sales_amount,
            gross_profit,
            login_email,
            -- 新規フラグ（列名差異を吸収）
            CASE
              WHEN SAFE_CAST(realized_flag AS INT64) = 1 THEN 1
              WHEN SAFE_CAST(is_new_deliveries AS INT64) = 1 THEN 1
              WHEN SAFE_CAST(new_flag AS INT64) = 1 THEN 1
              WHEN SAFE_CAST(realized_new_flag AS INT64) = 1 THEN 1
              ELSE 0
            END AS is_new
          FROM {VIEW_NEW_DELIVERIES_DAILY_ALL}
          WHERE sales_date BETWEEN @d_from AND @d_to
        )
        SELECT
          '{label}' AS 期間,
          COUNT(DISTINCT IF(is_new=1, customer_code, NULL)) AS 新規得意先数,
          COUNT(DISTINCT IF(is_new=1, yj_code, NULL)) AS 新規品目数,
          SUM(IF(is_new=1, sales_amount, 0)) AS 新規売上,
          SUM(IF(is_new=1, gross_profit, 0)) AS 新規粗利
        FROM base
        WHERE login_email = @login_email
        """
    # FYTD: 4月1日〜latest_date（current_monthの年がFY途中のため計算）
    # fiscal_year_apr は BigQuery 側で持っているが、ここは確実に計算する
    fy_start_year = current_month.year if current_month.month >= 4 else current_month.year - 1
    fy_start = date(fy_start_year, 4, 1)

    sql = f"""
    {agg_sql("昨日", d_yesterday, d_yesterday)}
    UNION ALL
    {agg_sql("直近7日", d_7, latest_date)}
    UNION ALL
    {agg_sql("当月", current_month, latest_date)}
    UNION ALL
    {agg_sql("年度累計", fy_start, latest_date)}
    """
    df = query_df(
        sql,
        params={
            "login_email": role.login_email,
            "d_from": current_month,  # ダミー（UNION側で上書きされないので使わない）
            "d_to": latest_date,
        },
        label="新規納品サマリー",
    )

    if df.empty:
        st.info("データがありません。")
        return

    df2 = df.copy()
    for c in ["新規売上", "新規粗利"]:
        if c in df2.columns:
            df2[c] = df2[c].apply(yen)
    st.dataframe(df2, use_container_width=True, hide_index=True)


# =============================================================================
# SEARCH UI (Customer)
# =============================================================================

@st.cache_data(ttl=600, show_spinner=False)
def search_customers(keyword: str, login_email: str, limit: int = 30) -> pd.DataFrame:
    # 得意先名で部分一致。候補は code+name を出す。
    # 現場はコードを覚えてない前提。
    sql = f"""
    SELECT
      customer_code,
      customer_name,
      staff_name,
      branch_name
    FROM {VIEW_FACT_LOGIN_JAN_DAILY}
    WHERE login_email = @login_email
      AND (
        customer_name LIKE CONCAT('%', @kw, '%')
        OR customer_code LIKE CONCAT('%', @kw, '%')
      )
    GROUP BY customer_code, customer_name, staff_name, branch_name
    ORDER BY customer_name
    LIMIT @lim
    """
    df = cached_query_df(sql, params={"kw": keyword, "login_email": login_email, "lim": int(limit)}, use_bqstorage=False, timeout_sec=90)
    return df


def render_customer_search_and_drill(role: RoleInfo, current_month: date, latest_date: date) -> None:
    st.markdown("### 得意先検索（候補 → 選択）")

    kw = st.text_input("得意先名で検索（例：熊谷 / 循環器）", value="", placeholder="2文字以上で候補を表示").strip()
    if len(kw) < 2:
        st.caption("※ 2文字以上入力すると候補が出ます。")
        return

    try:
        df_cands = search_customers(kw, role.login_email, limit=30)
    except Exception as e:
        st.error(f"検索でエラー：{e}")
        return

    if df_cands.empty:
        st.info("候補がありません。")
        return

    # selectbox に code|name を表示
    df_cands = df_cands.copy()
    df_cands["label"] = df_cands["customer_code"].astype(str) + " | " + df_cands["customer_name"].astype(str)
    options = df_cands["label"].tolist()

    selected = st.selectbox("候補から選択", options=options, index=0)

    sel_row = df_cands[df_cands["label"] == selected].iloc[0]
    customer_code = str(sel_row["customer_code"])
    customer_name = str(sel_row["customer_name"])

    st.caption(f"選択：{customer_code} / {customer_name}")

    # ミニドリル：当月売上/粗利、年度累計売上/粗利（重いJOINはしない）
    # 当月
    sql_month = f"""
    SELECT
      SUM(sales_amount) AS sales_amount,
      SUM(gross_profit) AS gross_profit
    FROM {VIEW_FACT_LOGIN_JAN_DAILY}
    WHERE login_email = @login_email
      AND customer_code = @customer_code
      AND sales_date BETWEEN @d_from AND @d_to
    """
    # FYTD（4/1〜latest_date）
    fy_start_year = current_month.year if current_month.month >= 4 else current_month.year - 1
    fy_start = date(fy_start_year, 4, 1)

    col1, col2 = st.columns(2)
    with col1:
        df_m = query_df(
            sql_month,
            params={"login_email": role.login_email, "customer_code": customer_code, "d_from": current_month, "d_to": latest_date},
            label="当月ミニドリル",
        )
        if not df_m.empty:
            st.metric("売上（当月）", yen(df_m.loc[0, "sales_amount"]))
            st.metric("粗利（当月）", yen(df_m.loc[0, "gross_profit"]))

    with col2:
        df_f = query_df(
            sql_month,
            params={"login_email": role.login_email, "customer_code": customer_code, "d_from": fy_start, "d_to": latest_date},
            label="年度累計ミニドリル",
        )
        if not df_f.empty:
            st.metric("売上（年度累計）", yen(df_f.loc[0, "sales_amount"]))
            st.metric("粗利（年度累計）", yen(df_f.loc[0, "gross_profit"]))


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # サイドバー：実行制御
    with st.sidebar:
        st.subheader("実行オプション（高速化）")
        st.session_state["use_bqstorage"] = st.checkbox("BigQuery Storage API を使う（速いが詰まる場合あり）", value=False)
        st.session_state["timeout_sec"] = st.number_input("クエリ待ちタイムアウト（秒）", min_value=10, max_value=600, value=90, step=10)
        st.caption("ずっと読み込みになる場合は Storage API をOFF。")
        st.markdown("---")
        st.subheader("表示設定")
        top_n = st.number_input("ランキング表示件数", min_value=10, max_value=200, value=50, step=10)
        st.session_state["top_n"] = int(top_n)

    # ログイン
    login_email = get_login_email()
    if not login_email:
        st.stop()

    # ヘルスチェック（軽量）
    with st.expander("接続チェック（問題切り分け用）", expanded=False):
        if st.button("SELECT 1（1秒で返るはず）"):
            df_ping = query_df("SELECT 1 AS ok", label="接続チェック")
            st.write(df_ping)

    # 役割
    role = load_role(login_email)

    # current_month / latest_date
    current_month = get_current_month()
    latest_date = get_latest_sales_date(current_month)

    # 権限分岐
    if not role.role_sales_view and not role.role_admin_view:
        st.error("閲覧権限がありません（role_sales_view / role_admin_view が false）。")
        st.stop()

    # 管理者（判断専用）
    if role.role_admin_view:
        render_admin_header(role, current_month, latest_date)

        st.markdown("---")
        st.subheader("管理者メニュー")

        colA, colB, colC, colD = st.columns(4)
        run_fytd = colA.button("年度累計サマリー")
        run_month = colB.button("当月YoYランキング")
        run_new = colC.button("新規納品サマリー")
        run_search = colD.button("得意先検索")

        st.caption("※ 必要なものだけ押してください（起動直後に全実行しない＝高速化）。")

        if run_fytd:
            st.markdown("---")
            render_fytd_org_summary(role)
            render_fytd_top_bottom(role, top_n=st.session_state["top_n"])

        if run_month:
            st.markdown("---")
            render_month_yoy_rankings(role, top_n=st.session_state["top_n"])

        if run_new:
            st.markdown("---")
            render_new_deliveries_summary(role, current_month=current_month, latest_date=latest_date)

        if run_search:
            st.markdown("---")
            render_customer_search_and_drill(role, current_month=current_month, latest_date=latest_date)

    else:
        # 担当者（入口は必要最低限）
        st.subheader("担当者入口（閲覧）")
        st.caption(f"ログイン：{role.login_email}")
        st.caption(f"current_month：{current_month.isoformat()} / 最新売上日：{latest_date.isoformat()}")

        st.markdown("---")
        st.info("担当者入口は次段（Phase1.5）で導線整備します。いまは管理者入口（判断専用）を優先。")
        render_customer_search_and_drill(role, current_month=current_month, latest_date=latest_date)


if __name__ == "__main__":
    main()
