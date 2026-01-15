import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & 爆速サマリーデータの読み込み ---
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
        
        with st.status("🚀 統合分析データをロード中...", expanded=False) as status:
            # 年度列を含む集計済みテーブルを読み込む
            query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
            df = client.query(query).to_dataframe()
            status.update(label="✅ ロード完了", state="complete")
        return df
    except Exception as e:
        st.error(f"データ取得失敗: {e}")
        return pd.DataFrame()

# --- アプリ基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
df_raw = load_data()

if not df_raw.empty:
    # --- 2. データ補完 & エリア定義 ---
    df_raw["支店名"] = df_raw["支店名"].fillna("本部")
    df_raw["担当社員名"] = df_raw["担当社員名"].fillna("未割当")
    
    oita_branches = ["大分", "別府", "中津", "佐伯"]
    df_raw["エリア"] = df_raw["支店名"].apply(
        lambda x: "大分エリア" if any(b in x for b in oita_branches) else "熊本エリア"
    )

    # --- 3. サイドバー：モード切替 & フィルタ ---
    st.sidebar.title("🎮 表示設定")
    view_mode = st.sidebar.radio("モード切替", ["管理者モード（全社・エリア）", "営業員モード（個人分析）"])

    df_filtered = df_raw.copy()

    if view_mode == "管理者モード（全社・エリア）":
        st.title("🏛️ 管理者ダッシュボード")
        selected_areas = st.sidebar.multiselect("エリア選択", options=["大分エリア", "熊本エリア"], default=["大分エリア", "熊本エリア"])
        df_filtered = df_filtered[df_filtered["エリア"].isin(selected_areas)]
        
        selected_branches = st.sidebar.multiselect("支店絞り込み", options=sorted(df_filtered["支店名"].unique()))
        if selected_branches:
            df_filtered = df_filtered[df_filtered["支店名"].isin(selected_branches)]
    else:
        st.title("🏃 営業員ドリルダウン")
        target_staff = st.sidebar.selectbox("担当者を選択", options=sorted(df_raw["担当社員名"].unique()))
        df_filtered = df_filtered[df_filtered["担当社員名"] == target_staff]

    # --- 4. 年度別パフォーマンス分析（新機能） ---
    st.header("📅 年度別サマリー")
    df_fy = df_filtered.groupby("年度")[["売上額", "粗利額"]].sum().reset_index().sort_values("年度")
    
    if len(df_fy) > 0:
        latest_fy = df_fy.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric(f"{latest_fy['年度']}年度 売上計", f"¥{latest_fy['売上額']:,.0f}")
        c3.metric(f"{latest_fy['年度']}年度 粗利計", f"¥{latest_fy['粗利額']:,.0f}")
        
        if len(df_fy) > 1:
            prev_fy = df_fy.iloc[-2]
            yoy_growth = (latest_fy['売上額'] / prev_fy['売上額'] - 1) * 100
            c2.metric("前年度比（売上）", f"{yoy_growth:+.1f}%", delta=f"¥{latest_fy['売上額'] - prev_fy['売上額']:,.0f}")
    
    # --- 5. メインビジュアル（タブ形式で機能整理） ---
    tab_fy, tab_trend, tab_portfolio = st.tabs(["📊 年度推移", "📈 月次トレンド", "🎯 得意先分析"])

    with tab_fy:
        st.subheader("年度別売上推移")
        fig_fy = px.bar(df_fy, x="年度", y="売上額", text_auto='.3s', color="年度", color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig_fy, use_container_width=True)

    with tab_trend:
        st.subheader("24ヶ月間の売上推移")
        trend = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
        fig_trend = px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True)
        st.plotly_chart(fig_trend, use_container_width=True)

    with tab_portfolio:
        if view_mode == "管理者モード（全社・エリア）":
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("エリア別 粗利構成")
                fig_area = px.pie(df_filtered.groupby("エリア")["粗利額"].sum().reset_index(), values="粗利額", names="エリア", hole=0.4)
                st.plotly_chart(fig_area, use_container_width=True)
            with col_b:
                st.subheader("支店別ランキング")
                fig_branch = px.bar(df_filtered.groupby("支店名")["粗利額"].sum().sort_values(ascending=False).reset_index(), x="支店名", y="粗利額", color="粗利額")
                st.plotly_chart(fig_branch, use_container_width=True)
        else:
            st.subheader("担当得意先ポートフォリオ")
            cust_df = df_filtered.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
            cust_df["粗利率"] = (cust_df["粗利額"] / cust_df["売上額"] * 100)
            fig_cust = px.scatter(cust_df, x="売上額", y="粗利率", size="粗利額", hover_name="得意先名", color="粗利率", color_continuous_scale="RdYlGn")
            st.plotly_chart(fig_cust, use_container_width=True)

    # --- 6. 戦略品分析 ---
    st.divider()
    st.subheader("💊 戦略品フラグ別 粗利構成")
    strat_df = df_filtered.groupby("戦略品フラグ")["粗利額"].sum().reset_index()
    fig_strat = px.bar(strat_df, x="戦略品フラグ", y="粗利額", color="戦略品フラグ", text_auto='.2s')
    st.plotly_chart(fig_strat, use_container_width=True)

    # --- 7. データ詳細 ---
    with st.expander("詳細データの確認（サマリー）"):
        st.dataframe(df_filtered, use_container_width=True)

else:
    st.warning("BigQueryからデータが取得できません。")
