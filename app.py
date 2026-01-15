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
    # --- サイドバー・モード設定 ---
    st.sidebar.title("🎮 表示設定")
    view_mode = st.sidebar.radio("モード切替", ["管理者モード", "営業員モード"])
    
    df_filtered = df.copy()
    if view_mode == "営業員モード":
        staff_list = sorted(df["担当社員名"].unique())
        target_staff = st.sidebar.selectbox("担当者を選択", staff_list)
        df_filtered = df[df["担当社員名"] == target_staff]
        st.title(f"🏃 {target_staff} 分析ダッシュボード")
    else:
        st.title("🏛️ 全社戦略ダッシュボード")

    # --- 1. 年度別サマリー (基本機能) ---
    st.header("📅 年度別パフォーマンス")
    df_fy = df_filtered.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    if not df_fy.empty:
        c1, c2, c3 = st.columns(3)
        latest = df_fy.iloc[-1]
        c1.metric(f"{latest['年度']}年度 売上", f"¥{latest['売上額']:,.0f}")
        c3.metric(f"{latest['年度']}年度 粗利", f"¥{latest['粗利額']:,.0f}")
        if len(df_fy) > 1:
            prev = df_fy.iloc[-2]
            yoy = (latest['売上額'] / prev['売上額'] - 1) * 100
            c2.metric("前年比(YoY)", f"{yoy:+.1f}%", delta=f"¥{latest['売上額']-prev['売上額']:,.0f}")

    # --- 2. 失注ドリルダウン・セクション (新機能) ---
    st.divider()
    st.header("🔍 失注・下落ドリルダウン分析")
    
    # 年度別の「得意先×商品」売上マトリクスを作成
    yoy_matrix = df_filtered.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    
    if len(yoy_matrix.columns) >= 2:
        curr_fy, prev_fy = yoy_matrix.columns[-1], yoy_matrix.columns[-2]
        
        # 得意先ごとの下落合計を計算
        cust_loss = yoy_matrix.groupby("得意先名").apply(lambda x: (x[curr_fy] - x[prev_fy]).sum()).sort_values()
        
        # 下落が大きい得意先を選択
        st.subheader("🛑 売上下落・失注が発生している得意先")
        loss_list = cust_loss[cust_loss < 0].index.tolist()
        selected_cust = st.selectbox("詳細を分析する得意先を選択してください", loss_list)
        
        if selected_cust:
            cust_detail = yoy_matrix.loc[selected_cust]
            cust_detail["差分額"] = cust_detail[curr_fy] - cust_detail[prev_fy]
            
            # ドリルダウン表示
            col_l, col_r = st.columns(2)
            with col_l:
                st.write(f"📊 **{selected_cust}** の商品別構成（今年度）")
                fig_pie = px.pie(cust_detail[cust_detail[curr_fy] > 0].reset_index(), values=curr_fy, names="成分規格名", hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col_r:
                st.write(f"📉 **{selected_cust}** の失注・減少明細")
                loss_detail = cust_detail[cust_detail["差分額"] < 0].sort_values("差分額")
                st.dataframe(loss_detail[[prev_fy, curr_fy, "差分額"]].style.format("¥{:,.0f}"), use_container_width=True)

    # --- 3. ランキング (隠しボタン/Expander) ---
    st.divider()
    with st.expander("🏆 各種ランキングを表示（売上・粗利）"):
        r_col1, r_col2 = st.columns(2)
        with r_col1:
            st.subheader("売上TOP10 (成分規格別)")
            st.bar_chart(df_filtered.groupby("成分規格名")["売上額"].sum().sort_values(ascending=False).head(10))
        with r_col2:
            st.subheader("粗利TOP10 (成分規格別)")
            st.bar_chart(df_filtered.groupby("成分規格名")["粗利額"].sum().sort_values(ascending=False).head(10))

    # --- 4. トレンド分析 ---
    st.header("📈 24ヶ月トレンド")
    trend = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
    st.plotly_chart(px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True), use_container_width=True)

else:
    st.warning("BigQueryで手順1のSQLを実行してください。")
