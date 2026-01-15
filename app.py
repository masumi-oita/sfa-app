import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & 高速サマリーデータの読み込み ---
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
        
        with st.status("🚀 爆速データをロード中...", expanded=False) as status:
            # 80万行ではなく、集計済みの「サマリーテーブル」を読み込む
            query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
            df = client.query(query).to_dataframe()
            status.update(label="✅ ロード完了", state="complete")
        return df
    except Exception as e:
        st.error(f"データ取得に失敗しました。BigQueryでのテーブル作成を確認してください: {e}")
        return pd.DataFrame()

# --- アプリ基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
df_raw = load_data()

if not df_raw.empty:
    # --- 2. データ補完 & エリア定義（踏襲） ---
    df_raw["支店名"] = df_raw["支店名"].fillna("本部")
    df_raw["担当社員名"] = df_raw["担当社員名"].fillna("未割当")
    
    # 大分エリアの支店定義
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

    # --- 4. KPI表示（ここを「売上額」「粗利額」に完全対応） ---
    st.divider()
    k1, k2, k3 = st.columns(3)
    
    sales = df_filtered["売上額"].sum()
    profit = df_filtered["粗利額"].sum()
    margin = (profit / sales * 100) if sales != 0 else 0
    
    k1.metric("選択範囲の売上高", f"¥{sales:,.0f}")
    k2.metric("総粗利額", f"¥{profit:,.0f}")
    k3.metric("平均粗利率", f"{margin:.1f}%")

    # --- 5. メインビジュアル ---
    tab1, tab2 = st.tabs(["📊 収益構造分析", "📈 時系列トレンド"])

    with tab1:
        if view_mode == "管理者モード（全社・エリア）":
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("エリア別 粗利構成")
                fig_area = px.pie(df_filtered.groupby("エリア")["粗利額"].sum().reset_index(), 
                                  values="粗利額", names="エリア", hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_area, use_container_width=True)
            with c2:
                st.subheader("支店別パフォーマンス")
                fig_branch = px.bar(df_filtered.groupby("支店名")["粗利額"].sum().sort_values(ascending=False).reset_index(),
                                    x="支店名", y="粗利額", color="粗利額", color_continuous_scale="Viridis")
                st.plotly_chart(fig_branch, use_container_width=True)
        else:
            # 営業員モード：得意先別ポートフォリオ（散布図）
            st.subheader("得意先別ポートフォリオ（売上×粗利率）")
            cust_df = df_filtered.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
            cust_df["粗利率"] = (cust_df["粗利額"] / cust_df["売上額"] * 100)
            fig_cust = px.scatter(cust_df, x="売上額", y="粗利率", size="粗利額", hover_name="得意先名", 
                                  color="粗利率", color_continuous_scale="RdYlGn", title="円の大きさは粗利額")
            st.plotly_chart(fig_cust, use_container_width=True)

    with tab2:
        st.subheader("📈 時系列推移（過去2年）")
        trend = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
        fig_trend = px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True, 
                            title="採用実績（赤） vs 過去実績（青）の推移を確認")
        st.plotly_chart(fig_trend, use_container_width=True)

    # --- 6. 戦略品分析 ---
    st.divider()
    st.subheader("💊 戦略品・区分別 粗利構成")
    strat_df = df_filtered.groupby("戦略品フラグ")["粗利額"].sum().reset_index()
    fig_strat = px.bar(strat_df, x="戦略品フラグ", y="粗利額", color="戦略品フラグ", text_auto='.2s')
    st.plotly_chart(fig_strat, use_container_width=True)

    # --- 7. データ詳細 ---
    with st.expander("詳細データの確認"):
        st.dataframe(df_filtered, use_container_width=True)

else:
    st.warning("BigQueryからデータが取得できません。手順1のSQLを実行して、テーブルを作成してください。")
