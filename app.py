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

st.set_page_config(page_title="Kyushu Towa Strategic SFA", layout="wide")
df = load_data()

if not df.empty:
    # フィルタ設定
    st.sidebar.title("🎮 モード選択")
    mode = st.sidebar.radio("表示切替", ["管理者モード", "営業員モード"])
    
    # 共通フィルタ
    if mode == "管理者モード":
        st.title("🏛️ 管理者・全体戦略ダッシュボード")
        df_view = df.copy()
    else:
        target_staff = st.sidebar.selectbox("担当者選択", sorted(df["担当社員名"].unique()))
        st.title(f"🏃 {target_staff} 担当分析")
        df_view = df[df["担当社員名"] == target_staff]

    # --- 1. メインKPI ---
    col1, col2, col3 = st.columns(3)
    sales = df_view["売上額"].sum()
    profit = df_view["粗利額"].sum()
    col1.metric("選択範囲 売上", f"¥{sales:,.0f}")
    col2.metric("総粗利", f"¥{profit:,.0f}")
    col3.metric("粗利率", f"{(profit/sales*100):.1f}%" if sales != 0 else "0%")

    # --- 2. 【新機能】アラート分析（売上減少・失注） ---
    st.divider()
    tab_alert, tab_rank, tab_trend = st.tabs(["⚠️ 要注意アラート", "🏆 ランキング", "📈 時系列"])

    with tab_alert:
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("🛑 売上減少が激しい得意先 (YoY)")
            # 今年度と前年度の比較
            yoy_cust = df_view.groupby(["年度", "得意先名"])["売上額"].sum().unstack(level=0)
            if len(yoy_cust.columns) >= 2:
                current_fy = yoy_cust.columns[-1]
                prev_fy = yoy_cust.columns[-2]
                yoy_cust["差分"] = yoy_cust[current_fy].fillna(0) - yoy_cust[prev_fy].fillna(0)
                declining = yoy_cust[yoy_cust["差分"] < 0].sort_values("差分").head(10)
                st.dataframe(declining[["差分"]].style.format("¥{:,.0f}"), use_container_width=True)
            else:
                st.info("比較可能な2年分以上のデータがありません")

        with c2:
            st.subheader("📉 失注・不採用品目")
            # 昨年度は売上があったが、今年度ゼロの商品を特定
            lost_items = df_view.groupby(["年度", "商品名"])["売上額"].sum().unstack(level=0)
            if len(lost_items.columns) >= 2:
                lost_items["今年度売上"] = lost_items[lost_items.columns[-1]].fillna(0)
                lost_items["昨年度売上"] = lost_items[lost_items.columns[-2]].fillna(0)
                # 昨年度 > 0 且つ 今年度 == 0
                churn = lost_items[(lost_items["昨年度売上"] > 0) & (lost_items["今年度売上"] == 0)]
                st.dataframe(churn[["昨年度売上"]].sort_values("昨年度売上", ascending=False).head(10), use_container_width=True)
            else:
                st.info("分析に必要な年度データが不足しています")

    with tab_rank:
        c1, c2 = st.columns(2)
        if mode == "管理者モード":
            with c1:
                st.subheader("売上額 Top 10 (全社)")
                st.bar_chart(df_view.groupby("商品名")["売上額"].sum().sort_values(ascending=False).head(10))
            with c2:
                st.subheader("粗利額 Top 10 (全社)")
                st.bar_chart(df_view.groupby("商品名")["粗利額"].sum().sort_values(ascending=False).head(10))
        else:
            with c1:
                st.subheader("担当先 売上ランキング")
                st.dataframe(df_view.groupby("得意先名")["売上額"].sum().sort_values(ascending=False).head(10))
            with c2:
                st.subheader("担当先 利益ランキング")
                st.dataframe(df_view.groupby("得意先名")["粗利額"].sum().sort_values(ascending=False).head(10))

    with tab_trend:
        st.subheader("月次売上推移")
        trend = df_view.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
        st.plotly_chart(px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True), use_container_width=True)

else:
    st.warning("BigQueryのテーブルを確認してください")
