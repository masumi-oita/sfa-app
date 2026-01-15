import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# --- データロード ---
@st.cache_data(ttl=300)
def load_all_data():
    key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    
    df_all = client.query("SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`").to_dataframe()
    df_adopt = client.query("SELECT * FROM `salesdb-479915.sales_data.t_new_adoption_master`").to_dataframe()
    return df_all, df_adopt

st.set_page_config(page_title="Strategic Sales Console", layout="wide")
df_all, df_adopt = load_all_data()

if not df_all.empty:
    # タイムスタンプ設定
    today = datetime.now()
    yesterday = (today - timedelta(days=1)).date()
    start_of_week = (today - timedelta(days=today.weekday())).date() # 月曜開始
    start_of_month = today.replace(day=1).date()
    start_of_fy = today.replace(month=4, day=1).date() if today.month >= 4 else today.replace(year=today.year-1, month=4, day=1).date()

    st.sidebar.title("🎮 表示設定")
    mode = st.sidebar.radio("モード", ["管理者モード", "営業員モード"])
    
    df_view = df_all.copy()
    df_adopt_view = df_adopt.copy()
    if mode == "営業員モード":
        staff = st.sidebar.selectbox("担当者選択", sorted(df_all["担当社員名"].dropna().unique()))
        df_view = df_all[df_all["担当社員名"] == staff]
        df_adopt_view = df_adopt[df_adopt["担当社員名"] == staff]

    # --- Section 1: 新規採用・4階層ドリルダウン ---
    st.header("⚡ 新規採用戦略サマリー")
    period = st.radio("表示期間の切り替え", ["昨日", "今週", "今月", "本年度"], horizontal=True)
    
    # 期間フィルタリング
    df_adopt_view['売上日'] = pd.to_datetime(df_adopt_view['売上日']).dt.date
    if period == "昨日":
        target_df = df_adopt_view[df_adopt_view['売上日'] == yesterday]
    elif period == "今週":
        target_df = df_adopt_view[df_adopt_view['売上日'] >= start_of_week]
    elif period == "今月":
        target_df = df_adopt_view[df_adopt_view['売上日'] >= start_of_month]
    else:
        target_df = df_adopt_view[df_adopt_view['売上日'] >= start_of_fy]

    # 採用サマリー表示
    c1, c2, c3 = st.columns(3)
    c1.metric(f"{period}の採用額", f"¥{target_df['採用売上額'].sum():,.0f}")
    c2.metric("採用件数", f"{len(target_df)}件")
    c3.metric("採用品目数", f"{target_df['商品名'].nunique()}品目")

    if not target_df.empty:
        with st.expander(f"{period}の得意先・品目ドリルダウンを表示"):
            # 得意先ごとに金額ベースでサマリー
            cust_sum = target_df.groupby("得意先名")["採用売上額"].sum().sort_values(ascending=False).reset_index()
            for _, row in cust_sum.iterrows():
                with st.container():
                    st.write(f"🏥 **{row['得意先名']}** --- 合計 ¥{row['採用売上額']:,.0f}")
                    st.table(target_df[target_df["得意先名"] == row["得意先名"]][["商品名", "採用売上額"]].rename(columns={"採用売上額": "想定額"}).style.format({"想定額": "¥{:,.0f}"}))

    # --- Section 2: 得意先別・年度売上マトリクス (既存機能・復活) ---
    st.divider()
    st.header("🏢 得意先別・年度売上集計")
    pivot = df_view.pivot_table(index="得意先名", columns="年度", values="売上額", aggfunc="sum", fill_value=0)
    if not pivot.empty:
        st.dataframe(pivot.sort_values(pivot.columns[-1], ascending=False).style.format("¥{:,.0f}"), use_container_width=True)

    # --- Section 3: 戦略ドリルダウン（下落 vs 上昇・既存機能） ---
    st.divider()
    st.header("🔍 下落(Lost) vs 上昇(Growth) 分析")
    tab_down, tab_up = st.tabs(["🔻 下落・失注分析", "🔼 成長・拡大分析"])

    # 行列データ作成
    matrix = df_view.groupby(["年度", "得意先名", "成分規格名"])["売上額"].sum().unstack(level=0).fillna(0)
    if len(matrix.columns) >= 2:
        curr_f, prev_f = matrix.columns[-1], matrix.columns[-2]
        matrix["diff"] = matrix[curr_f] - matrix[prev_f]

        with tab_down:
            loss_rank = matrix.groupby("得意先名")["diff"].sum().sort_values().head(20)
            st.table(loss_rank.reset_index().rename(columns={"diff": "減少額"}).style.format({"減少額": "¥{:,.0f}"}))
            sel_d = st.selectbox("下落得意先を選択してドリルダウン", ["-- 選択 --"] + loss_rank.index.tolist())
            if sel_d != "-- 選択 --":
                cd1, cd2, cd3 = st.columns(3)
                with cd1: st.plotly_chart(px.pie(matrix.loc[sel_d].reset_index(), values=curr_f, names="成分規格名", hole=0.4, title="商品構成"), use_container_width=True)
                with cd2: st.plotly_chart(px.line(df_view[df_view["得意先名"] == sel_d].groupby(["売上月", "年度"])["売上額"].sum().reset_index(), x="売上月", y="売上額", color="年度", title="月次推移"), use_container_width=True)
                with cd3: st.write("📉 減少品目明細"); st.dataframe(matrix.loc[sel_d].sort_values("diff").head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

        with tab_up:
            gain_rank = matrix.groupby("得意先名")["diff"].sum().sort_values(ascending=False).head(20)
            st.table(gain_rank.reset_index().rename(columns={"diff": "増加額"}).style.format({"増加額": "¥{:,.0f}"}))
            sel_u = st.selectbox("上昇得意先を選択してドリルダウン", ["-- 選択 --"] + gain_rank.index.tolist())
            if sel_u != "-- 選択 --":
                cu1, cu2, cu3 = st.columns(3)
                with cu1: st.plotly_chart(px.pie(matrix.loc[sel_u].reset_index(), values=curr_f, names="成分規格名", hole=0.4, title="商品構成"), use_container_width=True)
                with cu2: st.plotly_chart(px.line(df_view[df_view["得意先名"] == sel_u].groupby(["売上月", "年度"])["売上額"].sum().reset_index(), x="売上月", y="売上額", color="年度", title="月次推移"), use_container_width=True)
                with cu3: st.write("🔼 増加品目明細"); st.dataframe(matrix.loc[sel_u].sort_values("diff", ascending=False).head(15)[[prev_f, curr_f, "diff"]].style.format("¥{:,.0f}"), use_container_width=True)

else:
    st.error("BigQueryのテーブルを確認してください。")
