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

st.set_page_config(page_title="Kyushu Towa SFA Platform", layout="wide")
df = load_data()

if not df.empty:
    st.sidebar.title("🎮 表示設定")
    mode = st.sidebar.radio("モード切替", ["管理者モード", "営業員モード"])
    
    df_view = df.copy()
    if mode == "営業員モード":
        staff_list = sorted(df["担当社員名"].dropna().unique())
        target_staff = st.sidebar.selectbox("担当者を選択", staff_list)
        df_view = df[df["担当社員名"] == target_staff]
    
    # --- 1. 【新機能】得意先別の年度売上集計表 ---
    st.header("🏢 得意先別 年度売上集計")
    cust_fy_pivot = df_view.pivot_table(
        index="得意先名", 
        columns="年度", 
        values="売上額", 
        aggfunc="sum", 
        fill_value=0
    )
    # 年度ごとに合計をソート可能にするため、最新年度の降順で表示
    latest_col = cust_fy_pivot.columns[-1]
    st.dataframe(
        cust_fy_pivot.sort_values(latest_col, ascending=False).style.format("¥{:,.0f}"), 
        use_container_width=True
    )

    # --- 2. 時系列トレンド ---
    st.divider()
    st.header("📈 月次売上推移")
    trend_df = df_view.groupby(["売上月", "年度"])["売上額"].sum().reset_index()
    fig_trend = px.line(trend_df, x="売上月", y="売上額", color="年度", markers=True)
    st.plotly_chart(fig_trend, use_container_width=True)

    # --- 3. 戦略ドリルダウン (Down vs Up) ---
    st.divider()
    tab_down, tab_up = st.tabs(["🔻 下落・失注分析", "🔼 成長・拡大分析"])

    # 比較マトリクス
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    if len(matrix.columns) >= 2:
        curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
        matrix["diff"] = matrix[curr_f] - matrix[prev_f]

        with tab_down:
            with st.expander("🏆 失注・下落額ランキング (TOP 20)"):
                st.table(matrix.groupby("得意先名")["diff"].sum().sort_values().head(20).reset_index().style.format({"diff": "¥{:,.0f}"}))
            
            sel_down = st.selectbox("詳細を分析する得意先（下落先）", matrix.groupby("得意先名")["diff"].sum().sort_values().index)
            if sel_down:
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"📉 **{sel_down}** の下落品目詳細")
                    st.dataframe(matrix.loc[sel_down].sort_values("diff").head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with col2:
                    st.write("📊 当該得意先の月次推移")
                    c_m = df_view[df_view["得意先名"] == sel_down].groupby("売上月")["売上額"].sum().reset_index()
                    st.plotly_chart(px.bar(c_m, x="売上月", y="売_金額" if "売_金額" in c_m else "売上額"), use_container_width=True)

        with tab_up:
            with st.expander("🏆 成長・拡大額ランキング (TOP 20)"):
                st.table(matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20).reset_index().style.format({"diff": "¥{:,.0f}"}))
            
            sel_up = st.selectbox("詳細を分析する得意先（上昇先）", matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).index)
            if sel_up:
                col3, col4 = st.columns(2)
                with col3:
                    st.write(f"🔼 **{sel_up}** の上昇品目詳細")
                    st.dataframe(matrix.loc[sel_up].sort_values("diff", ascending=False).head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with col4:
                    st.write("📊 当該得意先の月次推移")
                    c_m_up = df_view[df_view["得意先名"] == sel_up].groupby("売上月")["売上額"].sum().reset_index()
                    st.plotly_chart(px.bar(c_m_up, x="売上月", y="売上額"), use_container_width=True)

else:
    st.error("BigQueryのテーブルを更新してください。")
