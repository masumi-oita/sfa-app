import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import json

@st.cache_data(ttl=86400) # 1日キャッシュ
def load_data():
    try:
        key_dict = json.loads(st.secrets["gcp_service_account"]["json_key"])
        scopes = ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/bigquery"]
        credentials = service_account.Credentials.from_service_account_info(key_dict, scopes=scopes)
        client = bigquery.Client(credentials=credentials, project=key_dict["project_id"])
        
        with st.status("⚡ 高速テーブルからデータを読み込み中...", expanded=True) as status:
            # 参照先をビュー(v_...)から、作成したテーブル(t_...)に変更
            query = "SELECT * FROM `salesdb-479915.sales_data.t_sales_performance_materialized` ORDER BY `売上日` DESC"
            df = client.query(query).to_dataframe()
            status.update(label="✅ ロード完了", state="complete")
        return df
    except Exception as e:
        st.error(f"ロード失敗: {e}")
        return pd.DataFrame()
        
        # 読み込みの進捗を可視化（フリーズ対策）
        with st.status("📦 データ集計中...", expanded=True) as status:
            st.write("BigQueryへの接続を確立...")
            query = "SELECT * FROM `salesdb-479915.sales_data.v_sales_performance_for_python` ORDER BY `売上日` DESC"
            
            st.write("2年分の実績データをロード中（逆算ロジック適用済み）...")
            df = client.query(query).to_dataframe()
            
            status.update(label=f"✅ ロード完了: {len(df):,}件", state="complete", expanded=False)
        return df
    except Exception as e:
        st.error(f"認証またはデータ取得に失敗しました: {e}")
        return pd.DataFrame()

# --- アプリ設定 ---
st.set_page_config(page_title="Kyushu Towa SFA Dashboard", layout="wide")
df_raw = load_data()

if not df_raw.empty:
    # --- データ加工（踏襲カラムのクレンジング） ---
    df_raw["支店名"] = df_raw["支店名"].fillna("本部（過去実績）")
    df_raw["担当社員名"] = df_raw["担当社員名"].fillna("未割当（過去実績）")
    
    # エリア定義（後付け実装の「箱」）
    oita_branches = ["大分", "別府", "中津", "佐伯"] # 実際の支店名に合わせて修正してください
    df_raw["エリア"] = df_raw["支店名"].apply(lambda x: "大分エリア" if any(b in x for b in oita_branches) else "熊本エリア")

    # 逆算値に基づく指標
    df_raw["売上額"] = df_raw["数量"] * df_raw["分析用単価"]
    df_raw["粗利額"] = df_raw["数量"] * (df_raw["分析用単価"] - df_filtered["分析用原価"]) if "分析用原価" in df_raw else df_raw["数量"] * (df_raw["分析用単価"] - df_raw["最新マスタ原価"])

    # --- 2. モード選択（管理者 vs 営業員） ---
    st.sidebar.title("🎮 表示モード")
    view_mode = st.sidebar.radio("切り替え", ["管理者（全社・エリア別）", "営業員（個人別分析）"])

    # --- 3. 共通KPI（画面トップ） ---
    last_update = df_raw[df_raw["データ区分"] == "採用実績"]["売上日"].max()
    st.title(f"📊 {view_mode}")
    st.info(f"💡 最新取り込み日: {last_update}")

    # --- 4. モード別ドリルダウン実装 ---
    if view_mode == "管理者（全社・エリア別）":
        tab1, tab2 = st.tabs(["🌎 エリア・支店比較", "🏢 支店詳細分析"])
        
        with tab1:
            st.subheader("エリア別売上・利益構成")
            area_sum = df_raw.groupby("エリア")[["売上額", "粗利額"]].sum().reset_index()
            col_a1, col_a2 = st.columns(2)
            fig_area = px.pie(area_sum, values="売上額", names="エリア", hole=.4, title="売上シェア")
            col_a1.plotly_chart(fig_area, use_container_width=True)
            
            fig_profit = px.bar(area_sum, x="エリア", y="粗利額", color="エリア", title="エリア別粗利額")
            col_a2.plotly_chart(fig_profit, use_container_width=True)

        with tab2:
            target_branch = st.multiselect("支店を選択", options=df_raw["支店名"].unique())
            df_branch = df_raw[df_raw["支店名"].isin(target_branch)] if target_branch else df_raw
            
            st.subheader("支店内の担当者別ランキング")
            staff_rank = df_branch.groupby("担当社員名")["粗利額"].sum().sort_values(ascending=False).reset_index()
            fig_staff = px.bar(staff_rank, x="粗利額", y="担当社員名", orientation='h', title="担当者別粗利貢献度")
            st.plotly_chart(fig_staff, use_container_width=True)

    else:
        # 営業員モード
        target_staff = st.selectbox("自分の名前を選択", options=df_raw["担当社員名"].unique())
        df_staff = df_raw[df_raw["担当社員名"] == target_staff]
        
        tab_p1, tab_p2 = st.tabs(["🤝 得意先分析", "💊 商品・戦略品分析"])
        
        with tab_p1:
            st.subheader(f"{target_staff}様の得意先ポートフォリオ")
            cust_sum = df_staff.groupby("得意先名")[["売上額", "粗利額"]].sum().reset_index()
            cust_sum["利益率"] = (cust_sum["粗利額"] / cust_sum["売上額"] * 100)
            # 散布図で「稼ぎ頭」と「課題先」を可視化
            fig_scat = px.scatter(cust_sum, x="売上額", y="利益率", size="粗利額", hover_name="得意先名",
                                 title="得意先別：売上 × 利益率（円の大きさは粗利額）")
            st.plotly_chart(fig_scat, use_container_width=True)

        with tab_p2:
            st.subheader("戦略品フラグ別の進捗")
            strat_sum = df_staff.groupby("戦略品フラグ")["粗利額"].sum().reset_index()
            fig_strat = px.bar(strat_sum, x="戦略品フラグ", y="粗利額", title="戦略品カテゴリ別の収益")
            st.plotly_chart(fig_strat, use_container_width=True)

    # --- 5. 共通：時系列推移 & 詳細一覧 ---
    st.divider()
    st.subheader("📈 24ヶ月間の時系列推移（データ区分別）")
    trend_df = df_raw.groupby(["売上月", "データ区分"])["売上額"].sum().reset_index()
    fig_line = px.line(trend_df, x="売上月", y="売上額", color="データ区分", markers=True)
    st.plotly_chart(fig_line, use_container_width=True)

    with st.expander("📝 すべての詳細データを確認（逆算原価・ユニークコード含む）"):
        st.dataframe(df_raw, use_container_width=True)
else:
    st.warning("データが取得できませんでした。BigQueryの設定を確認してください。")
