import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. データロード（速報と本番データの両方を読み込み） ---
@st.cache_data(ttl=300)
def load_all_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    
    # 全実績データ（成分規格レベル集計）
    df_all = client.query("SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`").to_dataframe()
    # 昨日採用データ
    df_today = client.query("SELECT * FROM `salesdb-479915.sales_data.t_daily_new_adoption`").to_dataframe()
    return df_all, df_today

st.set_page_config(page_title="Kyushu Towa SFA Strategic Platform", layout="wide")
df_all, df_today = load_all_data()

if not df_all.empty:
    # --- サイドバー設定 ---
    st.sidebar.title("🎮 営業戦略設定")
    mode = st.sidebar.radio("モード切替", ["管理者モード", "営業員モード"])
    
    df_view = df_all.copy()
    if mode == "営業員モード":
        staff = st.sidebar.selectbox("担当者を選択", sorted(df_all["担当社員名"].dropna().unique()))
        df_view = df_all[df_all["担当社員名"] == staff]
        df_today_view = df_today[df_today["担当社員名"] == staff] if "担当社員名" in df_today.columns else df_today
    else:
        df_today_view = df_today

    # --- Section 0: 経営KPIサマリー ---
    st.title(f"🏛️ {'全社' if mode == '管理者モード' else staff} 経営戦略コンソール")
    df_fy = df_view.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    if not df_fy.empty:
        c1, c2, c3, c4 = st.columns(4)
        curr = df_fy.iloc[-1]
        c1.metric(f"{curr['年度']}年度 売上", f"¥{curr['売上額']:,.0f}")
        c2.metric("粗利益", f"¥{curr['粗利額']:,.0f}")
        c3.metric("粗利率", f"{(curr['粗利額']/curr['売上額']*100):.1f}%")
        if len(df_fy) > 1:
            prev = df_fy.iloc[-2]
            yoy = (curr['売上額'] / prev['売上額'] - 1) * 100
            c4.metric("前年比(YoY)", f"{yoy:+.1f}%")

    # --- Section 1: ⚡ 昨日採用速報（速報ドリルダウン） ---
    st.divider()
    st.header("⚡ 昨日採用速報")
    if df_today_view.empty:
        st.info("昨日の新規採用実績はありません。")
    else:
        with st.expander("昨日採用の明細・ドリルダウンを表示"):
            st.dataframe(df_today_view.style.format({"採用売上額": "¥{:,.0f}"}), use_container_width=True)

    # --- Section 2: 📅 新規採用・月次・年度サマリー ---
    st.divider()
    st.header("📅 新規採用・月別進捗ドリルダウン")
    df_new = df_view[df_view["データ区分"] == "採用実績"]
    if not df_new.empty:
        col_m1, col_m2 = st.columns([2, 1])
        with col_m1:
            st.plotly_chart(px.bar(df_new.groupby(["売上月", "年度"])["売上額"].sum().reset_index(), 
                                   x="売上月", y="売上額", color="年度", barmode="group", title="採用実績の月別推移"), use_container_width=True)
        with col_m2:
            st.write("📋 採用累計（年度）")
            st.dataframe(df_new.groupby("年度")["売上額"].sum().reset_index().style.format({"売上額": "¥{:,.0f}"}), use_container_width=True)
        
        target_month = st.selectbox("詳細を見たい「月」を選択", ["-- 選択 --"] + sorted(df_new["売上月"].unique().tolist(), reverse=True))
        if target_month != "-- 選択 --":
            st.write(f"🔎 {target_month} の採用明細")
            st.dataframe(df_new[df_new["売上月"] == target_month][["得意先名", "成分規格名", "売上額"]].sort_values("売上額", ascending=False).style.format({"売上額": "¥{:,.0f}"}), use_container_width=True)

    # --- Section 3: 🏢 得意先別・年度売上集計マトリクス ---
    st.divider()
    st.header("🏢 得意先別・年度売上サマリー")
    cust_pivot = df_view.pivot_table(index="得意先名", columns="年度", values="売上額", aggfunc="sum", fill_value=0)
    if not cust_pivot.empty:
        st.dataframe(cust_pivot.sort_values(cust_pivot.columns[-1], ascending=False).style.format("¥{:,.0f}"), use_container_width=True)

    # --- Section 4: 🔍 戦略ドリルダウン（下落 vs 上昇） ---
    st.divider()
    st.header("🔍 戦略分析：下落(Lost) vs 上昇(Growth)")
    tab_down, tab_up = st.tabs(["🔻 下落・失注分析", "🔼 成長・拡大分析"])

    # 分析用行列
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    if len(matrix.columns) >= 2:
        curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
        matrix["diff"] = matrix[curr_f] - matrix[prev_f]

        # 下落分析
        with tab_down:
            loss_rank = matrix.groupby("得意先名")["diff"].sum().sort_values().head(20)
            st.subheader("売上減少ワースト20")
            st.table(loss_rank.reset_index().rename(columns={"diff": "減少額"}).style.format({"減少額": "¥{:,.0f}"}))
            
            sel_d = st.selectbox("下落要因を深掘りする得意先を選択", ["-- 選択してください --"] + loss_rank.index.tolist())
            if sel_d != "-- 選択してください --":
                cd1, cd2, cd3 = st.columns(3)
                with cd1:
                    st.write("📊 商品構成（今年度）")
                    st.plotly_chart(px.pie(matrix.loc[sel_d].reset_index(), values=curr_f, names="成分規格名", hole=0.4), use_container_width=True)
                with cd2:
                    st.write("📈 月次推移（24ヶ月）")
                    c_trend = df_view[df_view["得意先名"] == sel_d].groupby(["売上月", "年度"])["売上額"].sum().reset_index()
                    st.plotly_chart(px.line(c_trend, x="売上月", y="売上額", color="年度", markers=True), use_container_width=True)
                with cd3:
                    st.write("📉 品目別の減少明細")
                    st.dataframe(matrix.loc[sel_d].sort_values("diff").head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

        # 上昇分析
        with tab_up:
            gain_rank = matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20)
            st.subheader("売上成長ベスト20")
            st.table(gain_rank.reset_index().rename(columns={"diff": "増加額"}).style.format({"増加額": "¥{:,.0f}"}))
            
            sel_u = st.selectbox("成長要因を深掘りする得意先を選択", ["-- 選択してください --"] + gain_rank.index.tolist())
            if sel_u != "-- 選択してください --":
                cu1, cu2, cu3 = st.columns(3)
                with cu1:
                    st.write("📊 商品構成（拡大中）")
                    st.plotly_chart(px.pie(matrix.loc[sel_u].reset_index(), values=curr_f, names="成分規格名", hole=0.4), use_container_width=True)
                with cu2:
                    st.write("📈 月次推移")
                    u_trend = df_view[df_view["得意先名"] == sel_u].groupby(["売上月", "年度"])["売上額"].sum().reset_index()
                    st.plotly_chart(px.line(u_trend, x="売上月", y="売上額", color="年度", markers=True), use_container_width=True)
                with cu3:
                    st.write("🔼 品目別の成長明細")
                    st.dataframe(matrix.loc[sel_u].sort_values("diff", ascending=False).head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

else:
    st.error("BigQueryのテーブルを確認してください。")
