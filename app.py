import streamlit as st
import pandas as pd
import plotly.express as px
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# --- データロード ---
@st.cache_data(ttl=300)
def load_all_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    
    df_all = client.query("SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`").to_dataframe()
    df_today = client.query("SELECT * FROM `salesdb-479915.sales_data.t_daily_new_adoption`").to_dataframe()
    return df_all, df_today

st.set_page_config(page_title="Kyushu Towa SFA Strategic Console", layout="wide")
df_all, df_today = load_all_data()

if not df_all.empty:
    # --- サイドバー設定 ---
    st.sidebar.title("🎮 表示切替")
    mode = st.sidebar.radio("モード選択", ["管理者モード", "営業員モード"])
    
    df_view = df_all.copy()
    if mode == "営業員モード":
        staff = st.sidebar.selectbox("担当者を選択", sorted(df_all["担当社員名"].dropna().unique()))
        df_view = df_all[df_all["担当社員名"] == staff]
        df_today_view = df_today[df_today["担当社員名"] == staff] if "担当社員名" in df_today.columns else df_today
        st.title(f"🏃 {staff} 営業分析ダッシュボード")
    else:
        df_today_view = df_today
        st.title("🏛️ 全社経営戦略・管理者ダッシュボード")

    # --- Section 0: 全体パフォーマンス (Top KPIs) ---
    st.header("📊 全体パフォーマンス状況")
    df_fy_all = df_view.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    if not df_fy_all.empty:
        curr_perf = df_fy_all.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"{curr_perf['年度']}年度 売上", f"¥{curr_perf['売上額']:,.0f}")
        c2.metric(f"{curr_perf['年度']}年度 粗利", f"¥{curr_perf['粗利額']:,.0f}")
        c3.metric("平均粗利率", f"{(curr_perf['粗利額']/curr_perf['売上額']*100):.1f}%" if curr_perf['売上額'] != 0 else "0%")
        if len(df_fy_all) > 1:
            prev_perf = df_fy_all.iloc[-2]
            yoy = (curr_perf['売上額'] / prev_perf['売上額'] - 1) * 100
            c4.metric("前年売上比", f"{yoy:+.1f}%")

    # --- Section 1: ⚡ 昨日採用速報 (Manager Drill-down) ---
    st.divider()
    st.header("⚡ 昨日採用速報")
    if df_today_view.empty:
        st.info("昨日の新規採用データはありません。")
    else:
        # 管理者向け：金額ベースのサマリー
        t_adopt = df_today_view["採用売上額"].sum()
        st.subheader(f"合計採用額: ¥{t_adopt:,.0f} ({len(df_today_view)}品目)")
        
        # ドリルダウン構造
        with st.expander("🔍 採用実績の詳細ドリルダウン（得意先・品目別）"):
            # 得意先ごとに金額サマリー
            cust_adopt_summary = df_today_view.groupby("得意先名")["採用売上額"].sum().sort_values(ascending=False).reset_index()
            for index, row in cust_adopt_summary.iterrows():
                cust_name = row["得意先名"]
                cust_total = row["採用売上額"]
                # 得意先ごとの詳細を表示
                with st.container():
                    st.write(f"🏥 **{cust_name}** --- 採用合計: ¥{cust_total:,.0f}")
                    detail_items = df_today_view[df_today_view["得意先名"] == cust_name][["商品名", "数量", "採用売上額"]]
                    st.table(detail_items.rename(columns={"採用売上額": "想定売上"}).style.format({"想定売上": "¥{:,.0f}"}))

    # --- Section 2: 📅 新規採用・月次進捗サマリー ---
    st.divider()
    st.header("📅 新規採用・月次トレンド")
    df_new_adopt = df_view[df_view["データ区分"] == "採用実績"]
    if not df_new_adopt.empty:
        col_adopt_l, col_adopt_r = st.columns([2, 1])
        with col_adopt_l:
            # 月次バーチャート
            adopt_monthly = df_new_adopt.groupby(["売上月", "年度"])["売上額"].sum().reset_index()
            st.plotly_chart(px.bar(adopt_monthly, x="売上月", y="売上額", color="年度", barmode="group", title="採用額の月次推移（年度比較）"), use_container_width=True)
        with col_adopt_r:
            st.write("📋 年度別 採用累計")
            st.dataframe(df_new_adopt.groupby("年度")["売上額"].sum().reset_index().style.format({"売上額": "¥{:,.0f}"}), use_container_width=True)

    # --- Section 3: 🏢 得意先別・年度売上マトリクス ---
    st.divider()
    st.header("🏢 得意先別・年度売上マトリクス")
    cust_pivot = df_view.pivot_table(index="得意先名", columns="年度", values="売上額", aggfunc="sum", fill_value=0)
    if not cust_pivot.empty:
        # 最新年度の降順
        st.dataframe(cust_pivot.sort_values(cust_pivot.columns[-1], ascending=False).style.format("¥{:,.0f}"), use_container_width=True)

    # --- Section 4: 🔍 下落(Down) vs 上昇(Up) 戦略分析 ---
    st.divider()
    st.header("🔍 戦略ドリルダウン分析")
    tab_down, tab_up, tab_rank = st.tabs(["🔻 下落・失注分析", "🔼 成長・拡大分析", "🏆 各種ランキング"])

    # 共通マトリクス作成
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    if len(matrix.columns) >= 2:
        curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
        matrix["diff"] = matrix[curr_f] - matrix[prev_f]

        with tab_down:
            loss_rank = matrix.groupby("得意先名")["diff"].sum().sort_values().head(20)
            st.subheader("売上減少が大きい得意先 TOP 20")
            st.table(loss_rank.reset_index().rename(columns={"diff": "減少額"}).style.format({"減少額": "¥{:,.0f}"}))
            sel_d = st.selectbox("詳細を分析する得意先（下落）", loss_rank.index)
            if sel_d:
                c_d1, c_d2 = st.columns(2)
                with c_d1: st.write("📉 品目別下落詳細"); st.dataframe(matrix.loc[sel_d].sort_values("diff").head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with c_d2: st.write("📊 月次トレンド"); st.plotly_chart(px.bar(df_view[df_view["得意先名"] == sel_d].groupby("売上月")["売上額"].sum().reset_index(), x="売上月", y="売上額"), use_container_width=True)

        with tab_up:
            gain_rank = matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20)
            st.subheader("売上成長が大きい得意先 TOP 20")
            st.table(gain_rank.reset_index().rename(columns={"diff": "増加額"}).style.format({"増加額": "¥{:,.0f}"}))
            sel_u = st.selectbox("詳細を分析する得意先（上昇）", gain_rank.index)
            if sel_u:
                c_u1, c_u2 = st.columns(2)
                with c_u1: st.write("🔼 品目別成長詳細"); st.dataframe(matrix.loc[sel_u].sort_values("diff", ascending=False).head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)
                with c_u2: st.write("📊 月次トレンド"); st.plotly_chart(px.bar(df_view[df_view["得意先名"] == sel_u].groupby("売上月")["売上額"].sum().reset_index(), x="売上月", y="売上額"), use_container_width=True)
        
        with tab_rank:
            st.subheader("成分規格別 売上ランキング (TOP 20)")
            st.bar_chart(df_view.groupby("成分規格名")["売上額"].sum().sort_values(ascending=False).head(20))

else:
    st.error("BigQueryのテーブルを確認してください。")
