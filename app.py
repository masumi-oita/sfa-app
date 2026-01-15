import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- データロード（キャッシュ5分） ---
@st.cache_data(ttl=300)
def load_all_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    
    df_all = client.query("SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`").to_dataframe()
    df_today = client.query("SELECT * FROM `salesdb-479915.sales_data.t_daily_new_adoption`").to_dataframe()
    return df_all, df_today

st.set_page_config(page_title="Kyushu Towa SFA Platform", layout="wide")
df_all, df_today = load_all_data()

if not df_all.empty:
    # 1. サイドバー設定
    st.sidebar.title("🎮 表示切替")
    mode = st.sidebar.radio("モード選択", ["管理者モード", "営業員モード"])
    
    df_view = df_all.copy()
    if mode == "営業員モード":
        staff = st.sidebar.selectbox("担当者を選択", sorted(df_all["担当社員名"].dropna().unique()))
        df_view = df_all[df_all["担当社員名"] == staff]
        st.title(f"🏃 {staff} 営業分析ダッシュボード")
    else:
        st.title("🏛️ 全社戦略・管理者ダッシュボード")

    # --- Section A: 昨日採用速報 ---
    st.header("⚡ 昨日採用速報")
    if df_today.empty:
        st.info("昨日の新規採用データはありません。")
    else:
        c1, c2 = st.columns(2)
        c1.metric("昨日採用額（計）", f"¥{df_today['採用売上額'].sum():,.0f}")
        c2.metric("昨日採用件数", f"{len(df_today)}件")
        with st.expander("昨日採用の明細を確認"):
            st.dataframe(df_today.style.format({"採用売上額": "¥{:,.0f}"}), use_container_width=True)

    # --- Section B: 得意先別・年度売上マトリクス ---
    st.divider()
    st.header("🏢 得意先別 年度売上集計")
    cust_pivot = df_view.pivot_table(index="得意先名", columns="年度", values="売上額", aggfunc="sum", fill_value=0)
    # 最新年度の降順で表示
    if not cust_pivot.empty:
        st.dataframe(cust_pivot.sort_values(cust_pivot.columns[-1], ascending=False).style.format("¥{:,.0f}"), use_container_width=True)

    # --- Section C: 上下ドリルダウン分析 ---
    st.divider()
    st.header("🔍 下落(Down) vs 上昇(Up) 戦略分析")
    tab_down, tab_up = st.tabs(["🔻 下落・失注分析", "🔼 成長・拡大分析"])

    # 比較用マトリクス
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    if len(matrix.columns) >= 2:
        curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
        matrix["diff"] = matrix[curr_f] - matrix[prev_f]

        with tab_down:
            st.subheader("売上減少が大きい得意先ランキング")
            loss_rank = matrix.groupby("得意先名")["diff"].sum().sort_values().head(20)
            st.table(loss_rank.reset_index().rename(columns={"diff": "減少額"}).style.format({"減少額": "¥{:,.0f}"}))
            
            sel_d = st.selectbox("詳細を分析する得意先（下落）", loss_rank.index)
            if sel_d:
                col1, col2 = st.columns(2)
                with col1:
                    st.write("📉 品目別下落詳細")
                    st.dataframe(matrix.loc[sel_d].sort_values("diff").head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with col2:
                    st.write("📊 月次トレンド")
                    m_trend = df_view[df_view["得意先名"] == sel_d].groupby("売上月")["売上額"].sum().reset_index()
                    st.plotly_chart(px.bar(m_trend, x="売上月", y="売上額"), use_container_width=True)

        with tab_up:
            st.subheader("売上増加が大きい得意先ランキング")
            gain_rank = matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20)
            st.table(gain_rank.reset_index().rename(columns={"diff": "増加額"}).style.format({"増加額": "¥{:,.0f}"}))
            
            sel_u = st.selectbox("詳細を分析する得意先（上昇）", gain_rank.index)
            if sel_u:
                col3, col4 = st.columns(2)
                with col3:
                    st.write("🔼 品目別上昇詳細")
                    st.dataframe(matrix.loc[sel_u].sort_values("diff", ascending=False).head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with col4:
                    st.write("📊 月次トレンド")
                    m_trend_u = df_view[df_view["得意先名"] == sel_u].groupby("売上月")["売上額"].sum().reset_index()
                    st.plotly_chart(px.bar(m_trend_u, x="売上月", y="売上額"), use_container_width=True)

    # --- Section D: 月次トレンド推移 ---
    st.divider()
    st.header("📈 24ヶ月売上トレンド")
    total_trend = df_view.groupby(["売上月", "年度"])["売上額"].sum().reset_index()
    st.plotly_chart(px.line(total_trend, x="売上月", y="売上額", color="年度", markers=True), use_container_width=True)

else:
    st.error("BigQueryのテーブルを確認してください。")
