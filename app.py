import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証エラー回避：BigQuery接続設定 ---
@st.cache_data(ttl=600)
def load_data():
    # SecretsからJSONキーを読み込み、辞書形式に変換
    try:
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        credentials = service_account.Credentials.from_service_account_info(key_dict)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    except Exception as e:
        st.error(f"認証情報の読み込みに失敗しました。Secretsの設定を確認してください。: {e}")
        return pd.DataFrame()

    # 岡崎様が「整地」したビューを読み込む
    query = """
    SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    ORDER BY 売上日 DESC
    """
    return client.query(query).to_dataframe()

# --- アプリのメイン表示 ---
st.set_page_config(page_title="Kyushu Towa Sales Dashboard", layout="wide")
df = load_data()

if not df.empty:
    # --- 2. 最終更新日の表示（GASの稼働状況確認） ---
    # 採用実績（GAS経由）の最新日付を取得
    last_update = df[df["データ区分"] == "採用実績"]["売上日"].max()
    
    st.title("📊 営業実績・利益分析ダッシュボード")
    st.info(f"💡 【データ更新情報】 RPA/GASによる最新取り込み日: **{last_update}**")

    # --- 3. サイドバー：支店・担当者フィルタ（踏襲カラム） ---
    st.sidebar.header("分析フィルタ")
    
    # 過去データも含めた全リストを作成（NULLは「過去分/未設定」に置換）
    branch_list = df["支店名"].fillna("本部（過去実績）").unique()
    staff_list = df["担当社員名"].fillna("未割当（過去実績）").unique()
    
    selected_branches = st.sidebar.multiselect("支店名で絞り込み", options=branch_list)
    selected_staffs = st.sidebar.multiselect("担当者で絞り込み", options=staff_list)

    # フィルタリング実行
    df_filtered = df.copy()
    if selected_branches:
        df_filtered = df_filtered[df_filtered["支店名"].fillna("本部（過去実績）").isin(selected_branches)]
    if selected_staffs:
        df_filtered = df_filtered[df_filtered["担当社員名"].fillna("未割当（過去実績）").isin(selected_staffs)]

    # --- 4. メイン指標（KPI） ---
    # SQLで「逆算」した分析用単価・原価を使用して計算
    total_sales = (df_filtered["数量"] * df_filtered["分析用単価"]).sum()
    total_profit = (df_filtered["数量"] * (df_filtered["分析用単価"] - df_filtered["分析用原価"])).sum()
    margin_rate = (total_profit / total_sales * 100) if total_sales != 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("総販売金額（24ヶ月）", f"¥{total_sales:,.0f}")
    col2.metric("総粗利額（24ヶ月）", f"¥{total_profit:,.0f}")
    col3.metric("平均粗利率", f"{margin_rate:.1f}%")

    # --- 5. 2年間の推移グラフ（過去実績 vs 採用実績） ---
    st.subheader("📈 売上・粗利の24ヶ月推移")
    
    # 月別・区分別に集計
    monthly_df = df_filtered.groupby(["売上月", "データ区分"]).agg({
        "数量": "sum",
        "分析用単価": "mean" # 加重平均ではないが目安として
    }).reset_index()
    monthly_df["販売金額"] = monthly_df["数量"] * monthly_df["分析用単価"]

    fig = px.bar(
        monthly_df, 
        x="売上月", 
        y="販売金額", 
        color="データ区分",
        color_discrete_map={"過去実績": "#636EFA", "採用実績": "#EF553B"},
        barmode="stack",
        title="月別販売金額推移（積上げ）"
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- 6. 詳細データ表示 ---
    st.subheader("📑 取引詳細（ユニークコード軸）")
    st.dataframe(
        df_filtered[["売上日", "得意先名", "商品名", "数量", "分析用単価", "分析用原価", "データ区分", "戦略品フラグ", "ユニークコード_JAN"]],
        use_container_width=True
    )
else:
    st.warning("表示できるデータがありません。BigQueryのビューを確認してください。")
