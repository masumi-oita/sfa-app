import os
from datetime import date
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# =========================
# CONFIG
# =========================
PROJECT_ID = "salesdb-479915"
DATASET = "sales_data"

# Views you already created
V_LOGIN_CONTEXT = f"{PROJECT_ID}.{DATASET}.dim_staff_role"
V_BOTTOM_BY_STAFF = f"{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_bottom_current_month_by_staff"
V_BOTTOM = f"{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_bottom_current_month"
V_TOP = f"{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_top_current_month"
V_UNCOMPARABLE = f"{PROJECT_ID}.{DATASET}.v_sales_customer_yoy_uncomparable_current_month"
V_SALES_FACT_LOGIN_DAILY = f"{PROJECT_ID}.{DATASET}.v_sales_fact_login_jan_daily"

# =========================
# Helpers
# =========================
def get_bq_client() -> bigquery.Client:
    """
    Streamlit secrets に service_account を置く想定:
    st.secrets["gcp_service_account"] = {...json...}
    """
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    # fallback: local env (GOOGLE_APPLICATION_CREDENTIALS)
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=300)
def bq_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig()

    if params:
        job_params = []
        for k, v in params.items():
            # infer type (string/int/float/date)
            if isinstance(v, bool):
                typ = "BOOL"
            elif isinstance(v, int):
                typ = "INT64"
            elif isinstance(v, float):
                typ = "FLOAT64"
            elif isinstance(v, date):
                typ = "DATE"
            else:
                typ = "STRING"
            job_params.append(bigquery.ScalarQueryParameter(k, typ, v))
        job_config.query_parameters = job_params

    return client.query(sql, job_config=job_config).to_dataframe()


def get_user_email() -> str:
    # 1) URL query param: ?user_email=xxx
    qp = st.query_params
    if "user_email" in qp and qp["user_email"]:
        return str(qp["user_email"])

    # 2) session_state
    if "user_email" in st.session_state and st.session_state["user_email"]:
        return st.session_state["user_email"]

    # 3) fallback input
    return ""


@st.cache_data(ttl=300)
def fetch_roles(user_email: str) -> dict:
    sql = f"""
    SELECT
      login_email,
      role_admin_view,
      role_admin_edit,
      role_sales_view
    FROM `{V_LOGIN_CONTEXT}`
    WHERE login_email = @user_email
    """
    df = bq_query(sql, {"user_email": user_email})
    if df.empty:
        return {
            "login_email": user_email,
            "role_admin_view": False,
            "role_admin_edit": False,
            "role_sales_view": True,  # salesは一旦True扱い（社内なので）
            "registered": False,
        }
    r = df.iloc[0].to_dict()
    r["registered"] = True
    return r


@st.cache_data(ttl=300)
def fetch_current_month() -> str:
    sql = f"SELECT CAST(MAX(month) AS STRING) AS cur_month FROM `{V_BOTTOM}`"
    df = bq_query(sql)
    return df.iloc[0]["cur_month"] if not df.empty else ""


# =========================
# UI
# =========================
st.set_page_config(page_title="SFA Sales OS", layout="wide")

st.title("SFA Sales OS（入口）")

with st.sidebar:
    st.header("ログイン")
    user_email = get_user_email()
    user_email = st.text_input("user_email（メール）", value=user_email, placeholder="okazaki@shinrai8.by-works.com")
    st.session_state["user_email"] = user_email

    roles = fetch_roles(user_email) if user_email else None
    if roles:
        st.caption(f"登録: {'OK' if roles['registered'] else '未登録（暫定）'}")
        st.write(
            {
                "admin_view": bool(roles["role_admin_view"]),
                "admin_edit": bool(roles["role_admin_edit"]),
            }
        )

    st.divider()
    st.caption("URLで渡す場合：")
    st.code("?user_email=xxx@shinrai8.by-works.com", language="text")

if not user_email:
    st.info("左のサイドバーで user_email を入力してください（社内運用の暫定ログイン）。")
    st.stop()

current_month = fetch_current_month()
st.caption(f"Current month: {current_month}（YoY valid）")

is_admin_view = bool(roles and roles.get("role_admin_view"))
is_admin_edit = bool(roles and roles.get("role_admin_edit"))

# -------------------------
# Tabs
# -------------------------
tabs = ["担当者入口（下落/伸び/比較不能）", "ドリル（明細）"]
if is_admin_view:
    tabs.insert(0, "管理者入口（担当別 下落）")

tab_objs = st.tabs(tabs)

# =========================
# Admin tab
# =========================
idx = 0
if is_admin_view:
    with tab_objs[idx]:
        st.subheader("管理者入口：担当別 下落（当月）")
        st.caption("優先度：粗利差（ABS）→粗利率差→売上差（ABS）")

        limit_n = st.slider("担当あたり表示件数", 5, 30, 10)

        sql = f"""
        SELECT *
        FROM `{V_BOTTOM_BY_STAFF}`
        """
        df = bq_query(sql)

        if df.empty:
            st.warning("データがありません。")
        else:
            # group by login_email
            for login_email, g in df.groupby("login_email"):
                st.markdown(f"### {login_email}")
                st.dataframe(g.head(limit_n), use_container_width=True)
                st.divider()
    idx += 1

# =========================
# Rep tab
# =========================
with tab_objs[idx]:
    st.subheader("担当者入口（自分の担当だけ）")
    st.caption("下落 / 伸び / 比較不能（前年なし等）")

    colA, colB = st.columns([1, 2])
    with colA:
        view_choice = st.radio("表示", ["下落（bottom）", "伸び（top）", "比較不能（uncomparable）"], index=0)

    view_map = {
        "下落（bottom）": V_BOTTOM,
        "伸び（top）": V_TOP,
        "比較不能（uncomparable）": V_UNCOMPARABLE,
    }
    view = view_map[view_choice]

    sql = f"""
    SELECT
      customer_code,
      customer_name,
      month,
      sales_amount,
      gross_profit,
      gross_profit_rate,
      sales_amount_py,
      gross_profit_py,
      sales_diff_yoy,
      gp_diff_yoy,
      sales_yoy_rate,
      gp_yoy_rate
    FROM `{view}`
    WHERE login_email = @user_email
    ORDER BY
      -- view側で並んでいるが、念のため
      ABS(gp_diff_yoy) DESC,
      ABS(gp_yoy_rate) DESC,
      ABS(sales_diff_yoy) DESC
    LIMIT 2000
    """
    df = bq_query(sql, {"user_email": user_email})

    with colB:
        if df.empty:
            st.warning("データがありません（担当得意先が未紐付けの可能性）。")
        else:
            st.dataframe(df, use_container_width=True, height=520)

            st.divider()
            st.markdown("#### ドリルに送る（customer_code）")
            selected = st.selectbox("customer_code（上の表からコピーでもOK）", options=[""] + df["customer_code"].dropna().unique().tolist())
            if selected:
                st.session_state["drill_customer_code"] = selected
                st.success(f"drill_customer_code = {selected}")

idx += 1

# =========================
# Drill tab
# =========================
with tab_objs[idx]:
    st.subheader("ドリル（得意先 → 月 → 明細）")

    customer_code = st.session_state.get("drill_customer_code", "")
    customer_code = st.text_input("customer_code", value=customer_code)

    if not customer_code:
        st.info("上のタブで customer_code を選ぶか、ここに入力してください。")
        st.stop()

    # months available for that customer (login filtered)
    sql_m = f"""
    SELECT
      month,
      ANY_VALUE(customer_name) AS customer_name,
      SUM(sales_amount) AS sales_amount,
      SUM(gross_profit) AS gross_profit,
      SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gross_profit_rate
    FROM `{V_SALES_FACT_LOGIN_DAILY}`
    WHERE login_email = @user_email
      AND customer_code = @customer_code
    GROUP BY month
    ORDER BY month DESC
    """
    mdf = bq_query(sql_m, {"user_email": user_email, "customer_code": customer_code})

    if mdf.empty:
        st.warning("この得意先はあなたの担当ではないか、売上データがありません。")
        st.stop()

    st.dataframe(mdf, use_container_width=True, height=240)

    month_list = mdf["month"].astype(str).tolist()
    default_month = month_list[0] if month_list else ""
    month_pick = st.selectbox("month（YYYY-MM-01）", options=month_list, index=0)

    # item summary inside month
    sql_i = f"""
    SELECT
      yj_code,
      jan,
      ANY_VALUE(item_name) AS item_name,
      ANY_VALUE(pack_unit) AS pack_unit,
      SUM(quantity) AS qty,
      SUM(sales_amount) AS sales_amount,
      SUM(gross_profit) AS gross_profit,
      SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(sales_amount), 0)) AS gross_profit_rate
    FROM `{V_SALES_FACT_LOGIN_DAILY}`
    WHERE login_email = @user_email
      AND customer_code = @customer_code
      AND month = DATE(@month_pick)
    GROUP BY yj_code, jan
    ORDER BY gross_profit DESC, sales_amount DESC
    """
    idf = bq_query(sql_i, {"user_email": user_email, "customer_code": customer_code, "month_pick": month_pick})
    st.markdown("### 品目（JAN/YJ）")
    st.dataframe(idf, use_container_width=True, height=380)

    st.divider()
    st.markdown("### 明細（日次）")
    sql_d = f"""
    SELECT
      sales_date,
      yj_code,
      jan,
      item_name,
      pack_unit,
      quantity,
      sales_amount,
      gross_profit
    FROM `{V_SALES_FACT_LOGIN_DAILY}`
    WHERE login_email = @user_email
      AND customer_code = @customer_code
      AND month = DATE(@month_pick)
    ORDER BY sales_date DESC, gross_profit DESC
    LIMIT 2000
    """
    ddf = bq_query(sql_d, {"user_email": user_email, "customer_code": customer_code, "month_pick": month_pick})
    st.dataframe(ddf, use_container_width=True, height=420)
