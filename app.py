import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & データ読み込み（日本語カラムをバッククォートで保護） ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # Secretsからjson_keyを読み込み
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        credentials = service_account.Credentials.from_service_account_info(key_dict)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
    except Exception as e:
        st.error(f"認証情報の読み込みに失敗しました: {e}")
        return pd.DataFrame()

    # SQL内の日本語カラム名を ` ` で囲み、エラー \345 を回避
    query = """
    SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python`
    ORDER BY `売上日` DESC
    """
    return client.query(query).to_dataframe()

# --- アプリのメイン設定 ---
st.set_page_config(page_title="Kyushu Towa Sales Dashboard", layout="wide")
df = load_data()

if not df.empty:
    # --- 2. 最終更新日の表示（RPA/GAS稼働確認用） ---
    # 採用実績（GAS経由）の最新日付を取得
    last_update = df[df["データ区分"] == "採用実績"]["売上日"].max()
    
    st.title("📊 営業実績・利益分析ダッシュボード")
    st.info(f"💡 【データ更新情報】 RPA/GASによる最終取り込み日: **{last_update}**")

    # --- 3. サイドバー：支店・担当者フィルタ（踏襲カラム） ---
    st.sidebar.header("分析フィルタ")
    
    # 過去データ側のNULL値を「過去実績分」として置換して選択肢を作成
    branch_options = df["支店名"].fillna("本部（過去実績）").unique()
    staff_options = df["担当社員名"].fillna("未割当（過去実績）").unique()
    
    selected_branches = st.sidebar.multiselect("支店名を選択", options=branch_options)
    selected_staffs = st.sidebar.multiselect("担当者を選択", options=staff_options)

    # フィルタリング実行
    df_filtered = df.copy()
    if selected_branches:
        df_filtered = df_filtered[df_filtered["支店名"].fillna("本部（過去実績）").isin(selected_branches)]
    if selected_staffs:
        df_filtered = df_filtered[df_filtered["担当社員名"].fillna("未割当（過去実績）").isin(selected_staffs)]

    # --- 4. メイン指標（KPI）：逆算した分析用カラムを使用 ---
    total_sales = (df_filtered["数量"] * df_filtered["分析用単価"]).sum()
    total_profit = (df_filtered["数量"] * (df_filtered["分析用単価"] - df_filtered["分析用原価"])).sum()
    margin_rate = (total_profit / total_sales * 100) if total_sales != 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("総販売金額（24ヶ月）", f"¥{total_sales:,.0f}")
    col2.metric("総粗利額（24ヶ月）", f"¥{total_profit:,.0f}")
    col3.metric("平均粗利率", f"{margin_rate:.1f}%")

    # --- 5. 売上推移グラフ：過去実績(青) vs 採用実績(赤) ---
    st.subheader("📈 売上高の月別推移（積上げ）")
    
    # 月別・区分別に販売金額を集計
    monthly_summary = df_filtered.copy()
    monthly_summary["販売額"] = monthly_summary["数量"] * monthly_summary["分析用単価"]
    plot_df = monthly_summary.groupby(["売上月", "データ区分"])["販売額"].sum().reset_index()

    fig = px.bar(
        plot_df, 
        x="売上月", 
        y="販売額", 
        color="データ区分",
        color_discrete_map={"過去実績": "#636EFA", "採用実績": "#EF553B"},
        barmode="stack",
        labels={"販売額": "金額(¥)", "売上月": "年月"}
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- 6. 詳細データ：ユニークコード軸のテーブル ---
    st.subheader("📑 取引詳細データ一覧")
    st.dataframe(
        df_filtered[[
            "売上日", "得意先名", "商品名", "数量", 
            "分析用単価", "分析用原価", "データ区分", 
            "戦略品フラグ", "ユニークコード_JAN", "ユニークコード_YJ"
        ]],
        use_container_width=True
    )
else:
    st.warning("BigQueryからデータを取得できませんでした。")
