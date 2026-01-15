import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- データロード（爆速キャッシュ） ---
@st.cache_data(ttl=600)
def load_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
    return client.query(query).to_dataframe()

st.set_page_config(page_title="Kyushu Towa Business Intelligence", layout="wide")
df = load_data()

if not df.empty:
    # --- サイドバーフィルタ ---
    st.sidebar.title("🎮 表示切替")
    view_mode = st.sidebar.radio("モード", ["管理者モード", "営業員モード"])
    
    if view_mode == "管理者モード":
        st.title("🏛️ 全社経営ダッシュボード")
        df_filtered = df.copy()
    else:
        target_staff = st.sidebar.selectbox("担当者選択", sorted(df["担当社員名"].unique()))
        st.title(f"🏃 {target_staff} 営業分析")
        df_filtered = df[df["担当社員名"] == target_staff]

    # --- 1. 【復活】年度別サマリー（YoY） ---
    st.header("📅 年度パフォーマンス比較")
    df_fy = df_filtered.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    
    if not df_fy.empty:
        latest_fy = df_fy.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric(f"{latest_fy['年度']}年度 売上計", f"¥{latest_fy['売上額']:,.0f}")
        c3.metric(f"{latest_fy['年度']}年度 粗利計", f"¥{latest_fy['粗利額']:,.0f}")
        
        if len(df_fy) > 1:
            prev_fy = df_fy.iloc[-2]
            yoy_growth = (latest_fy['売上額'] / prev_fy['売上額'] - 1) * 100
            c2.metric("前年売上比（YoY）", f"{yoy_growth:+.1f}%", delta=f"¥{latest_fy['売上額'] - prev_fy['売上額']:,.0f}")

        # 年度推移グラフ
        fig_fy = px.bar(df_fy, x="年度", y="売上額", text_auto='.3s', color="年度", color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_fy, use_container_width=True)

    # --- 2. 戦略分析エリア（アラート & トレンド） ---
    st.divider()
    st.header("🔍 戦略分析ドリルダウン")
    tab_alert, tab_trend, tab_rank = st.tabs(["⚠️ 要注意アラート", "📈 月次推移", "🏆 ランキング"])

    with tab_alert:
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("🛑 売上急落先（前年度比 差分ワースト）")
            yoy_cust = df_filtered.groupby(["年度", "得意先名"])["売上額"].sum().unstack(level=0)
            if len(yoy_cust.columns) >= 2:
                curr, prev = yoy_cust.columns[-1], yoy_cust.columns[-2]
                yoy_cust["下落額"] = yoy_cust[curr].fillna(0) - yoy_cust[prev].fillna(0)
                st.dataframe(yoy_cust[yoy_cust["下落額"] < 0].sort_values("下落額").head(15)[["下落額"]].style.format("¥{:,.0f}"), use_container_width=True)
            else:
                st.info("前年度比較データが不足しています")

        with col_right:
            st.subheader("📉 失注品目（昨年売上あり・今年ゼロ）")
            yoy_item = df_filtered.groupby(["年度", "商品名"])["売上額"].sum().unstack(level=0)
            if len(yoy_item.columns) >= 2:
                curr, prev = yoy_item.columns[-1], yoy_item.columns[-2]
                lost = yoy_item[(yoy_item[prev] > 0) & (yoy_item[curr].fillna(0) == 0)]
                st.dataframe(lost[[prev]].sort_values(prev, ascending=False).head(15).style.format("¥{:,.0f}"), use_container_width=True)

    with tab_trend:
        st.subheader("24ヶ月月次推移（採用実績 vs 過去実績）")
        trend_df = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
        fig_trend = px.line(trend_df, x="売上月", y="売上額", color="データ区分", markers=True)
        st.plotly_chart(fig_trend, use_container_width=True)

    with tab_rank:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("売上TOP10（商品/得意先）")
            label = "商品名" if view_mode == "管理者モード" else "得意先名"
            st.bar_chart(df_filtered.groupby(label)["売上額"].sum().sort_values(ascending=False).head(10))
        with c2:
            st.subheader("粗利TOP10（商品/得意先）")
            st.bar_chart(df_filtered.groupby(label)["粗利額"].sum().sort_values(ascending=False).head(10))

else:
    st.error("データの読み込みに失敗しました。")
