import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & データ読み込み（Driveスコープを追加） ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # Secretsからjson_keyを読み込み
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        
        # ★【重要】BigQueryに加えてGoogle Driveの読み取り権限をセットする
        scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/bigquery"
        ]
        
        credentials = service_account.Credentials.from_service_account_info(key_dict, scopes=scopes)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    except Exception as e:
        st.error(f"認証情報の読み込みに失敗しました: {e}")
        return pd.DataFrame()

    # SQL内の日本語カラム名を ` ` で囲む（前回の修正を維持）
    query = """
    SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    ORDER BY `売上日` DESC
    """
    return client.query(query).to_dataframe()

# --- アプリのメイン表示 ---
st.set_page_config(page_title="Kyushu Towa Sales Dashboard", layout="wide")
df = load_data()

# (以下、前回までの表示・フィルタ・KPI・グラフのコードをすべて継続)
if not df.empty:
    last_update = df[df["データ区分"] == "採用実績"]["売上日"].max()
    st.title("📊 営業実績・利益分析ダッシュボード")
    st.info(f"💡 【データ更新情報】 RPA/GASによる最終取り込み日: **{last_update}**")

    # サイドバーフィルタ
    st.sidebar.header("分析フィルタ")
    branch_options = df["支店名"].fillna("本部（過去実績）").unique()
    staff_options = df["担当社員名"].fillna("未割当（過去実績）").unique()
    selected_branches = st.sidebar.multiselect("支店名を選択", options=branch_options)
    selected_staffs = st.sidebar.multiselect("担当者を選択", options=staff_options)

    # フィルタリング
    df_filtered = df.copy()
    if selected_branches:
        df_filtered = df_filtered[df_filtered["支店名"].fillna("本部（過去実績）").isin(selected_branches)]
    if selected_staffs:
        df_filtered = df_filtered[df_filtered["担当社員名"].fillna("未割当（過去実績）").isin(selected_staffs)]

    # KPI表示
    total_sales = (df_filtered["数量"] * df_filtered["分析用単価"]).sum()
    total_profit = (df_filtered["数量"] * (df_filtered["分析用単価"] - df_filtered["分析用原価"])).sum()
    margin_rate = (total_profit / total_sales * 100) if total_sales != 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("総販売金額", f"¥{total_sales:,.0f}")
    col2.metric("総粗利額", f"¥{total_profit:,.0f}")
    col3.metric("平均粗利率", f"{margin_rate:.1f}%")

    # グラフ表示
    st.subheader("📈 売上高の月別推移")
    monthly_summary = df_filtered.copy()
    monthly_summary["販売額"] = monthly_summary["数量"] * monthly_summary["分析_単価"] if "分析_単価" in monthly_summary else monthly_summary["数量"] * monthly_summary["分析用単価"]
    plot_df = monthly_summary.groupby(["売上月", "データ区分"])["販売額"].sum().reset_index()
    fig = px.bar(plot_df, x="売上月", y="販売額", color="データ区分", barmode="stack", color_discrete_map={"過去実績": "#636EFA", "採用実績": "#EF553B"})
    st.plotly_chart(fig, use_container_width=True)

    # 詳細テーブル
    st.subheader("📑 取引詳細データ一覧")
    st.dataframe(df_filtered, use_container_width=True)
else:
    st.warning("BigQueryからデータを取得できませんでした。")
