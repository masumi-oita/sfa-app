import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- データロード ---
@st.cache_data(ttl=600)
def load_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
    return client.query(query).to_dataframe()

st.set_page_config(page_title="Strategic Sales Drill-down", layout="wide")
df = load_data()

if not df.empty:
    st.sidebar.title("🎮 表示設定")
    mode = st.sidebar.radio("モード切替", ["管理者モード", "営業員モード"])
    
    df_view = df.copy()
    if mode == "営業員モード":
        staff = st.sidebar.selectbox("担当者選択", sorted(df["担当社員名"].dropna().unique()))
        df_view = df[df["担当社員名"] == staff]
    
    # --- 1. 年度別サマリー ---
    st.header("📅 年度パフォーマンス比較 (YoY)")
    df_fy = df_view.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    if len(df_fy) >= 2:
        curr, prev = df_fy.iloc[-1], df_fy.iloc[-2]
        c1, c2, c3 = st.columns(3)
        c1.metric(f"{curr['年度']}年度 売上", f"¥{curr['売上額']:,.0f}", delta=f"¥{curr['売上額']-prev['売上額']:,.0f}")
        c2.metric("前年比(%)", f"{(curr['売上額']/prev['売上額']*100):.1f}%")
        c3.metric(f"{curr['年度']}年度 粗利", f"¥{curr['粗利額']:,.0f}")

    # --- 2. 下落・上昇ドリルダウン・タブ ---
    st.divider()
    tab_down, tab_up = st.tabs(["🔻 下落・失注分析 (Down)", "🔼 成長・拡大分析 (Up)"])

    # 共通データ作成 (年度×得意先×成分)
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
    matrix["diff"] = matrix[curr_f] - matrix[prev_f]

    with tab_down:
        # ランキング：失注が顕著な得意先 Top 20
        with st.expander("🏆 失注・下落額が大きい得意先 Top 20"):
            top_loss_cust = matrix.groupby("得意先名")["diff"].sum().sort_values().head(20)
            st.table(top_loss_cust.reset_index().rename(columns={"diff": "下落金額"}).style.format({"下落金額": "¥{:,.0f}"}))

        # ドリルダウン
        st.subheader("🕵️ 得意先から月次・品目へ深掘り")
        selected_cust_down = st.selectbox("分析する下落得意先を選択", matrix.groupby("得意先名")["diff"].sum().sort_values().index)
        
        if selected_cust_down:
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"📈 **{selected_cust_down}** の月次売上推移")
                cust_monthly = df_view[df_view["得意先名"] == selected_cust_down].groupby(["売上月", "年度"])["売上額"].sum().reset_index()
                st.plotly_chart(px.line(cust_monthly, x="売上月", y="売上額", color="年度", markers=True), use_container_width=True)
            with col2:
                st.write(f"📉 **{selected_cust_down}** の下落品目明細")
                item_diff = matrix.loc[selected_cust_down].sort_values("diff").head(15)
                st.dataframe(item_diff[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

    with tab_up:
        # ランキング：成長が顕著な得意先 Top 20
        with st.expander("🏆 成長・拡大額が大きい得意先 Top 20"):
            top_gain_cust = matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20)
            st.table(top_gain_cust.reset_index().rename(columns={"diff": "上昇金額"}).style.format({"上昇金額": "¥{:,.0f}"}))

        # ドリルダウン
        st.subheader("🚀 成長要因を分析")
        selected_cust_up = st.selectbox("分析する成長得意先を選択", matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).index)
        
        if selected_cust_up:
            col3, col4 = st.columns(2)
            with col3:
                st.write(f"📈 **{selected_cust_up}** の月次売上推移")
                cust_monthly_up = df_view[df_view["得意先名"] == selected_cust_up].groupby(["売上月", "年度"])["売上額"].sum().reset_index()
                st.plotly_chart(px.line(cust_monthly_up, x="売上月", y="売上額", color="年度", markers=True), use_container_width=True)
            with col4:
                st.write(f"🔼 **{selected_cust_up}** の成長品目明細")
                item_diff_up = matrix.loc[selected_cust_up].sort_values("diff", ascending=False).head(15)
                st.dataframe(item_diff_up[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

else:
    st.error("BigQueryのテーブルを確認してください。")
