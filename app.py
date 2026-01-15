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
        
        with st.status("🚀 高速集計データをロード中...", expanded=False) as status:
            # 80万行の生データではなく、集計済みの「完成品テーブル」を読み込む
            query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_summary_materialized`"
            df = client.query(query).to_dataframe()
            status.update(label="✅ ロード完了", state="complete")
        return df
    except Exception as e:
        st.error(f"データ取得に失敗しました。手順1のテーブル作成を確認してください: {e}")
        return pd.DataFrame()

# --- アプリ基本設定 ---
st.set_page_config(page_title="Kyushu Towa SFA", layout="wide")
df = load_data()

if not df.empty:
    # --- 2. データ補完 & エリア定義（踏襲） ---
    df["支店名"] = df["支店名"].fillna("本部")
    df["担当社員名"] = df["担当社員名"].fillna("未割当")
    
    oita_branches = ["大分", "別府", "中津", "佐伯"]
    df["エリア"] = df["支店名"].apply(lambda x: "大分エリア" if any(b in x for b in oita_branches) else "熊本エリア")

    # --- 3. サイドバー設定 ---
    st.sidebar.title("🎛️ 表示モード切替")
    mode = st.sidebar.radio("選択してください", ["管理者モード（全社・エリア）", "営業員モード（個人分析）"])

    # --- 4. フィルタリングロジック ---
    if mode == "管理者モード（全社・エリア）":
        st.title("🏛️ 管理者・エリアサマリー")
        sel_area = st.sidebar.multiselect("エリア選択", options=["大分エリア", "熊本エリア"], default=["大分エリア", "熊本エリア"])
        df_filtered = df[df["エリア"].isin(sel_area)]
        
        sel_branch = st.sidebar.multiselect("支店選択", options=sorted(df_filtered["支店名"].unique()))
        if sel_branch:
            df_filtered = df_filtered[df_filtered["支店名"].isin(sel_branch)]
    else:
        st.title("🏃 営業員別パフォーマンス")
        target_staff = st.sidebar.selectbox("担当者名を選択", options=sorted(df["担当社員名"].unique()))
        df_filtered = df[df["担当社員名"] == target_staff]

    # --- 5. メインKPI表示 ---
    st.divider()
    col1, col2, col3 = st.columns(3)
    total_sales = df_filtered["売上額"].sum()
    total_profit = df_filtered["粗利額"].sum()
    margin_rate = (total_profit / total_sales * 100) if total_sales != 0 else 0

    col1.metric("選択範囲の売上高", f"¥{total_sales:,.0f}")
    col2.metric("総粗利額", f"¥{total_profit:,.0f}")
    col3.metric("平均粗利率", f"{margin_rate:.1f}%")

    # --- 6. ドリルダウン・ビジュアル ---
    tab1, tab2 = st.tabs(["📊 収益分析", "📈 時系列推移"])

    with tab1:
        if mode == "管理者モード（全社・エリア）":
            c1, c2 = st.columns(2)
            # エリア別粗利
            fig_area = px.pie(df_filtered.groupby("エリア")["粗利額"].sum().reset_index(), 
                              values="粗利額", names="エリア", hole=0.4, title="エリア別粗利構成")
            c1.plotly_chart(fig_area, use_container_width=True)
            # 支店別ランキング
            fig_branch = px.bar(df_filtered.groupby("支店名")["粗利額"].sum().sort_values(ascending=False).reset_index(),
                                x="支店名", y="粗利額", title="支店別粗利ランキング", color="粗利額")
            c2.plotly_chart(fig_branch, use_container_width=True)
        else:
            # 営業員モード：得意先ポートフォリオ（Scatter）
            cust_df = df_filtered.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
            cust_df["粗利率"] = (cust_df["粗利額"] / cust_df["売上額"] * 100)
            fig_cust = px.scatter(cust_df, x="売上額", y="粗利率", size="粗利額", hover_name="得意先名",
                                  title="担当先ポートフォリオ（円の大きさは粗利額）", color="粗利率", color_continuous_scale="RdYlGn")
            st.plotly_chart(fig_cust, use_container_width=True)

    with tab2:
        # 時系列トレンド（24ヶ月）
        trend_df = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
        fig_trend = px.line(trend_df, x="売上月", y="売上額", color="データ区分", markers=True, 
                            title="24ヶ月間の売上推移（過去実績 vs 採用実績）")
        st.plotly_chart(fig_trend, use_container_width=True)

    # --- 7. 戦略品分析 ---
    st.subheader("💊 戦略品フラグ別 粗利構成")
    strat_df = df_filtered.groupby("戦略品フラグ")["粗利額"].sum().reset_index()
    st.plotly_chart(px.bar(strat_df, x="戦略品フラグ", y="粗利額", color="戦略品フラグ"), use_container_width=True)

    # --- 8. データ詳細 ---
    with st.expander("詳細データの確認"):
        st.dataframe(df_filtered, use_container_width=True)

else:
    st.warning("データがロードできませんでした。BigQueryでテーブルが作成されているか確認してください。")
