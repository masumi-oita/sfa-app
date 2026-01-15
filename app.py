import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. データロード ---
@st.cache_data(ttl=600)
def load_data():
    try:
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        credentials = service_account.Credentials.from_service_account_info(key_dict)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
        query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return pd.DataFrame()

st.set_page_config(page_title="Kyushu Towa Business Intelligence", layout="wide")
df = load_data()

if not df.empty:
    # --- 2. モード・フィルタ設定 ---
    st.sidebar.title("🎮 表示設定")
    mode = st.sidebar.radio("モード選択", ["管理者モード", "営業員モード"])
    
    df_view = df.copy()
    if mode == "営業員モード":
        staff_list = sorted(df["担当社員名"].dropna().unique())
        target_staff = st.sidebar.selectbox("担当者を選択", staff_list)
        df_view = df[df["担当社員名"] == target_staff]
        st.title(f"🏃 {target_staff} 営業個人分析")
    else:
        st.title("🏛️ 全社経営戦略ダッシュボード")

    # --- 3. 年度別パフォーマンス (GM視点) ---
    st.header("📅 年度別実績・前年比")
    df_fy = df_view.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    
    if not df_fy.empty:
        c1, c2, c3 = st.columns(3)
        latest = df_fy.iloc[-1]
        c1.metric(f"{latest['年度']}年度 売上", f"¥{latest['売上額']:,.0f}")
        c3.metric(f"{latest['年度']}年度 粗利", f"¥{latest['粗利額']:,.0f}")
        if len(df_fy) > 1:
            prev = df_fy.iloc[-2]
            yoy = (latest['売上額'] / prev['売上額'] - 1) * 100
            c2.metric("前年比(YoY)", f"{yoy:+.1f}%", delta=f"¥{latest['売上額']-prev['ver_prev_sales']:,.0f}" if 'ver_prev_sales' in locals() else None)

        st.plotly_chart(px.bar(df_fy, x="年度", y="売上額", text_auto='.3s', color="年度"), use_container_width=True)

    # --- 4. 戦略ドリルダウン (失注・下落) ---
    st.divider()
    st.header("🔍 失注・下落要因ドリルダウン")
    
    # 年度別の「得意先×成分」マトリクス
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    
    if len(matrix.columns) >= 2:
        curr_fy, prev_fy = matrix.columns[-1], matrix.columns[-2]
        
        # 売上減少幅が大きい得意先を特定
        cust_diff = matrix.groupby("得意先名").apply(lambda x: (x[curr_fy] - x[prev_fy]).sum()).sort_values()
        declining_customers = cust_diff[cust_diff < 0].index.tolist()
        
        selected_cust = st.selectbox("分析対象の得意先（売上減少先）を選択してください", declining_customers)
        
        if selected_cust:
            cust_data = matrix.loc[selected_cust].copy()
            cust_data["差分額"] = cust_data[curr_fy] - cust_data[prev_fy]
            
            col_l, col_r = st.columns([1, 2])
            with col_l:
                st.write(f"📊 **{selected_cust}** の今年度商品構成")
                fig_pie = px.pie(cust_data[cust_data[curr_fy] > 0].reset_index(), values=curr_fy, names="成分規格名", hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
            with col_r:
                st.write(f"📉 **{selected_cust}** の失注・減少明細（金額順）")
                # 減少したものだけ表示
                loss_df = cust_data[cust_data["差分額"] < 0].sort_values("差分額")
                st.dataframe(loss_df[[prev_fy, curr_fy, "差分額"]].style.format("¥{:,.0f}"), use_container_width=True)

    # --- 5. ランキング (Expanderで整理) ---
    st.divider()
    with st.expander("🏆 売上・粗利ランキングを表示"):
        r_col1, r_col2 = st.columns(2)
        target_col = "成分規格名" if mode == "管理者モード" else "得意先名"
        with r_col1:
            st.subheader(f"売上高 TOP10 ({target_col})")
            st.bar_chart(df_view.groupby(target_col)["売上額"].sum().sort_values(ascending=False).head(10))
        with r_col2:
            st.subheader(f"粗利額 TOP10 ({target_col})")
            st.bar_chart(df_view.groupby(target_col)["粗利額"].sum().sort_values(ascending=False).head(10))

    # --- 6. トレンド分析 ---
    st.header("📈 24ヶ月トレンド")
    trend = df_view.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
    st.plotly_chart(px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True), use_container_width=True)

else:
    st.error("BigQueryのテーブルを確認してください。")
