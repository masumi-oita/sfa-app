import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

# --- 1. 認証 & データ読み込み（列を絞って軽量化） ---
@st.cache_data(ttl=3600) # 1時間キャッシュ（朝一度ロードすればサクサク）
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
        
        with st.status("⚡ データを軽量化してロード中...", expanded=True) as status:
            # 必要な列だけに絞ってダウンロード容量を削減（10分→数十秒への鍵）
            query = """
            SELECT 
                `売上日`, `売上月`, `支店名`, `担当社員名`, `得意先名`, 
                `商品名`, `数量`, `分析用単価`, `分析用原価`, 
                `データ区分`, `戦略品フラグ`
            FROM `salesdb-479915.sales_data.t_sales_performance_materialized`
            """
            df = client.query(query).to_dataframe()
            
            # 日付型の変換を確実に行う
            df["売上日"] = pd.to_datetime(df["売上日"])
            status.update(label=f"✅ ロード完了: {len(df):,}件", state="complete", expanded=False)
        return df
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return pd.DataFrame()

# --- アプリ設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
df_raw = load_data()

if not df_raw.empty:
    # --- データ前処理 ---
    df_raw["支店名"] = df_raw["支店名"].fillna("本部")
    df_raw["担当社員名"] = df_raw["担当社員名"].fillna("未割当")
    
    # エリア定義（大分・熊本）
    oita_branches = ["大分", "別府", "中津", "佐伯"]
    df_raw["エリア"] = df_raw["支店名"].apply(lambda x: "大分エリア" if any(b in x for b in oita_branches) else "熊本エリア")

    # 利益計算
    df_raw["売上額"] = df_raw["数量"] * df_raw["分析用単価"]
    df_raw["粗利額"] = df_raw["数量"] * (df_raw["分析用単価"] - df_raw["分析用原価"])

    # --- 2. サイドバー：モード切替 & フィルタ ---
    st.sidebar.title("🎮 表示設定")
    view_mode = st.sidebar.radio("モード切替", ["管理者モード（全社・エリア）", "営業員モード（個人分析）"])

    # フィルタリングの土台
    df_filtered = df_raw.copy()

    if view_mode == "管理者モード（全社・エリア）":
        st.title("🏛️ 管理者ダッシュボード")
        selected_areas = st.sidebar.multiselect("エリア選択", options=["大分エリア", "熊本エリア"], default=["大分エリア", "熊本エリア"])
        df_filtered = df_filtered[df_filtered["エリア"].isin(selected_areas)]
        
        selected_branches = st.sidebar.multiselect("支店絞り込み", options=df_filtered["支店名"].unique())
        if selected_branches:
            df_filtered = df_filtered[df_filtered["支店名"].isin(selected_branches)]
    else:
        st.title("🏃 営業員ドリルダウン")
        target_staff = st.sidebar.selectbox("担当者を選択", options=sorted(df_raw["担当社員名"].unique()))
        df_filtered = df_filtered[df_filtered["担当社員名"] == target_staff]

    # --- 3. KPI表示 ---
    last_update = df_raw[df_raw["データ区分"] == "採用実績"]["売上日"].max()
    st.caption(f"最終更新（採用実績）: {last_update.date() if hasattr(last_update, 'date') else last_update}")

    k1, k2, k3 = st.columns(3)
    sales = df_filtered["売上額"].sum()
    profit = df_filtered["粗利額"].sum()
    margin = (profit / sales * 100) if sales != 0 else 0
    k1.metric("選択範囲の売上", f"¥{sales:,.0f}")
    k2.metric("総粗利額", f"¥{profit:,.0f}")
    k3.metric("平均粗利率", f"{margin:.1f}%")

    st.divider()

    # --- 4. モード別メインビジュアル ---
    if view_mode == "管理者モード（全社・エリア）":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("エリア別 粗利構成")
            area_chart = px.pie(df_filtered.groupby("エリア")["粗利額"].sum().reset_index(), 
                                values="粗利額", names="エリア", hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(area_chart, use_container_width=True)
        with c2:
            st.subheader("支店別パフォーマンス")
            branch_chart = px.bar(df_filtered.groupby("支店名")["粗利額"].sum().sort_values(ascending=False).reset_index(),
                                  x="支店名", y="粗利額", color="粗利額")
            st.plotly_chart(branch_chart, use_container_width=True)
    else:
        # 営業員モード：得意先分析
        st.subheader("得意先別ポートフォリオ（売上×粗利率）")
        cust_df = df_filtered.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
        cust_df["粗利率"] = (cust_df["粗利額"] / cust_df["売上額"] * 100)
        fig_cust = px.scatter(cust_df, x="売上額", y="粗利率", size="粗利額", hover_name="得意先名", 
                              color="粗利率", color_continuous_scale="RdYlGn")
        st.plotly_chart(fig_cust, use_container_width=True)

    # --- 5. 共通：時系列トレンド（月別） ---
    st.subheader("📈 時系列推移（24ヶ月）")
    trend = df_filtered.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
    fig_trend = px.line(trend, x="売上月", y="売上額", color="データ区分", markers=True)
    st.plotly_chart(fig_trend, use_container_width=True)

    # --- 6. 詳細データ（負荷軽減のため直近1000件に制限） ---
    with st.expander("取引明細の確認（直近1,000件）"):
        st.dataframe(df_filtered.sort_values("売上日", ascending=False).head(1000), use_container_width=True)

else:
    st.warning("BigQueryにデータが存在しないか、接続に問題があります。")
