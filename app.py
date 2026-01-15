import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & データ読み込み（カラム・権限踏襲） ---
@st.cache_data(ttl=600)
def load_data():
    try:
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/bigquery"
        ]
        credentials = service_account.Credentials.from_service_account_info(key_dict, scopes=scopes)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
        
        query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上日` DESC"
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"接続エラー: {e}")
        return pd.DataFrame()

# --- アプリ設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
df_raw = load_data()

if not df_raw.empty:
    # データ前処理：欠損値補完（フィルタ・集計用）
    df_raw["支店名"] = df_raw["支店名"].fillna("本部（過去実績）")
    df_raw["担当社員名"] = df_raw["担当社員名"].fillna("未割当（過去実績）")
    # 利益額の算出（逆算カラムを使用）
    df_raw["粗利額"] = df_raw["数量"] * (df_raw["分析用単価"] - df_raw["分析用原価"])
    df_raw["売上額"] = df_raw["数量"] * df_raw["分析用単価"]

    # --- 2. モード選択とフィルタリング ---
    st.sidebar.title("🎛️ 表示設定")
    mode = st.sidebar.radio("表示モード切替", ["管理者モード (全社・支店)", "営業員モード (個人)"])

    df_filtered = df_raw.copy()

    if mode == "管理者モード (全社・支店)":
        st.title("🏛️ 管理者・マネージャー用ダッシュボード")
        target_branch = st.sidebar.multiselect("分析対象の支店", options=df_raw["支店名"].unique())
        if target_branch:
            df_filtered = df_filtered[df_filtered["支店名"].isin(target_branch)]
    else:
        st.title("🏃 営業員別パフォーマンス・ドリルダウン")
        target_staff = st.sidebar.selectbox("担当者を選択してください", options=df_raw["担当社員名"].unique())
        df_filtered = df_filtered[df_filtered["担当社員名"] == target_staff]

    # --- 3. 共通KPI表示 ---
    last_update = df_raw[df_raw["データ区分"] == "採用実績"]["売上日"].max()
    st.caption(f"最終取り込み日: {last_update}")

    kpi1, kpi2, kpi3 = st.columns(3)
    total_sales = df_filtered["売上額"].sum()
    total_profit = df_filtered["粗利額"].sum()
    margin = (total_profit / total_sales * 100) if total_sales != 0 else 0
    kpi1.metric("選択範囲の総売上", f"¥{total_sales:,.0f}")
    kpi2.metric("総粗利額", f"¥{total_profit:,.0f}")
    kpi3.metric("粗利率", f"{margin:.1f}%")

    st.divider()

    # --- 4. 管理者用ドリルダウン: 支店・担当者別の俯瞰 ---
    if mode == "管理者モード (全社・支店)":
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("支店別売上・粗利比較")
            branch_summary = df_filtered.groupby("支店名")[["売上額", "粗利額"]].sum().reset_index()
            fig_branch = px.bar(branch_summary, x="支店名", y="売上額", color="粗利額", 
                                title="支店別の売上規模と収益性")
            st.plotly_chart(fig_branch, use_container_width=True)

        with col_right:
            st.subheader("担当者別ランキング（粗利順）")
            staff_rank = df_filtered.groupby("担当社員名")["粗利額"].sum().sort_values(ascending=False).reset_index()
            fig_staff = px.bar(staff_rank, x="粗利額", y="担当社員名", orientation='h', color="粗利額",
                               title="担当者別の貢献度")
            st.plotly_chart(fig_staff, use_container_width=True)

    # --- 5. 営業員用ドリルダウン: 得意先・商品別の深掘り ---
    else:
        tab1, tab2 = st.tabs(["得意先別分析", "商品別分析"])
        
        with tab1:
            st.subheader(f"🤝 {target_staff}担当の得意先別利益構造")
            cust_summary = df_filtered.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
            cust_summary["利益率"] = (cust_summary["粗利額"] / cust_summary["売上額"] * 100)
            fig_cust = px.scatter(cust_summary, x="売上額", y="利益率", size="粗利額", hover_name="得意先名",
                                  title="得意先別の売上 vs 利益率（円の大きさは粗利額）")
            st.plotly_chart(fig_cust, use_container_width=True)

        with tab2:
            st.subheader("📦 商品・戦略品別パフォーマンス")
            item_summary = df_filtered.groupby(["商品名", "戦略品フラグ"])[["数量", "粗利額"]].sum().reset_index()
            fig_item = px.treemap(item_summary, path=["戦略品フラグ", "商品名"], values="粗利額",
                                  color="粗利額", title="戦略品カテゴリ別の利益構成（ツリーマップ）")
            st.plotly_chart(fig_item, use_container_width=True)

    # --- 6. 共通: 月別推移 & 詳細データ ---
    st.divider()
    st.subheader("📈 時系列推移")
    trend_df = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
    fig_trend = px.line(trend_df, x="売上月", y="売上額", color="データ区分", markers=True, 
                        title="過去実績と採用実績の月別推移")
    st.plotly_chart(fig_trend, use_container_width=True)

    with st.expander("詳細な取引データを確認"):
        st.dataframe(df_filtered.drop(columns=["売上額", "粗利額"]), use_container_width=True)

else:
    st.warning("データが見つかりません。")
