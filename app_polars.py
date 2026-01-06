import streamlit as st
import pandas as pd
import polars as pl  # Polarsを明示的にインポート
import os
import sqlite3
import logic_v2
import db_utils
import scraper # Import Scraper Module
from datetime import datetime, timedelta
import importlib

# Force Reload Modules ensuring fixes are applied without restart
importlib.reload(scraper)
importlib.reload(db_utils)
import logic_v2
importlib.reload(logic_v2)

# ロジックエンジン（Polarsベース）をインポート
from advanced_logic import KeirinLogicEngine, apply_advanced_logic

# ==========================================
# ページ設定
# ==========================================
st.set_page_config(
    page_title="競輪DeepDive | データで熱狂をつかめ!",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSSスタイル (Antigravityテーマ)
st.markdown("""
<style>
    .big-font { font-size: 20px !important; font-weight: bold; }
    .win-rank-1 { background-color: #FFD700; color: black; padding: 2px 5px; border-radius: 3px; }
    .jimoto-tag { background-color: #ffcccc; color: red; font-weight: bold; padding: 2px; border: 1px solid red; border-radius: 4px; }
    .special-badge { background-color: #663399; color: white; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }
    .stDataFrame { border: 1px solid #e0e0e0; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# ロジックエンジンの初期化 (キャッシュ)
# ==========================================
@st.cache_resource
def get_logic_engine():
    """
    Polarsベースの分析エンジンを初期化
    データの保存場所を指定 (デフォルト: data/files)
    """
    # インスタンス化して返す。この関数はキャッシュされるため、
    # エンジン内の重いデータロードは一度だけで済む。
    print("Logic Engine Loading... (Cache Reset v2)")
    return KeirinLogicEngine(data_root_dir='logic_data')

# Cache Clear Trigger (Update this when Logic Class changes)
# version: 2025-12-28-v2

engine = get_logic_engine()

# ==========================================
# サイドバー & DB接続
# ==========================================
# ==========================================
# サイドバー機能実装 (検索 & DB)
# ==========================================

# ==========================================
# サイドバー機能実装 (検索 & DB)
# ==========================================

# ==========================================
# サイドバー機能実装 (スクレイピング & 検索 & DB)
# ==========================================

st.sidebar.image("assets/deepdive_logo.png", use_container_width=True)

# ----------------------------
# 0. スクレイピング (出走表取得)
# ----------------------------
st.sidebar.markdown("---")
st.sidebar.header("📡 出走表取得")
with st.sidebar.expander("Webから取得 (推奨)", expanded=True):
    # Venue List
    active_venues = [
        "函館","青森","いわき平","弥彦","前橋","取手","宇都宮","大宮","西武園","京王閣","立川","松戸","川崎","平塚","小田原","伊東","静岡","名古屋","岐阜","大垣","豊橋","富山","松阪","四日市","福井","奈良","向日町","和歌山","岸和田","玉野","広島","防府","高松","小松島","高知","松山","小倉","久留米","武雄","佐世保","別府","熊本"
    ]
    
    # Date Input (Range)
    # Default: Today
    today = datetime.today()
    target_dates = st.sidebar.date_input("開催期間", [today, today]) # Default tuple
    
    # Multi-Select Venue
    target_venues = st.sidebar.multiselect("競輪場を選択", active_venues, default=["平塚", "松戸"])
    
    if st.sidebar.button("データ取得開始", type="primary"):
        if not target_venues:
            st.sidebar.error("競輪場を選択してください")
        else:
            # Handle Date Range
            if isinstance(target_dates, (list, tuple)):
                if len(target_dates) == 2:
                    s_date, e_date = target_dates
                elif len(target_dates) == 1:
                    s_date = target_dates[0]
                    e_date = target_dates[0]
                else: 
                     s_date = today
                     e_date = today
            else:
                 s_date = target_dates
                 e_date = target_dates
            
            s_str = s_date.strftime("%Y-%m-%d")
            e_str = e_date.strftime("%Y-%m-%d")
            
            st.sidebar.info(f"{s_str} ～ {e_str} のデータを取得中...")
            
            # Progress Bar logic is tricky within sidebar, just spinner
            with st.spinner("K-Dreamsからデータを取得しています..."):
                try:
                    all_scraped_data = []
                    
                    # Store in session state structure compatible with uploaded_files
                    # List of dicts: {'label':..., 'df':..., 'meta':...}
                    
                    # Iterate selected venues
                    # Using scraper.fetch_race_data which supports multi-threading for races, 
                    # but we are calling it per venue.
                    for v_name in target_venues:
                        # fetch_race_data returns list of dicts [{'df':..., 'meta':...}]
                        # Note: exclude_ids is optional
                        # Force max_workers=1 to prevent data corruption bug
                        venue_results = scraper.fetch_race_data(v_name, s_str, e_str, max_workers=1)
                        if venue_results:
                            for res in venue_results:
                                df_s = res['df']
                                meta_s = res['meta']
                                refund_s = res.get('refund', {}) # Capture refund data

                                # Format Label: "Place Date 11R (Web)"
                                date_label = meta_s.get('date', '')
                                r_num = meta_s.get('race_num', '?')
                                label = f"{v_name} {date_label} {r_num}R (Web)"
                                
                                # Add to collection
                                all_scraped_data.append({
                                    'label': label,
                                    'df': df_s,
                                    'meta': meta_s,
                                    'refund': refund_s, # Store for DB Save
                                    'filename': f"scraped_{v_name}_{date_label}_{r_num}R.html",
                                    'sort_key': (v_name, int(r_num) if r_num.isdigit() else 0)
                                })
                    
                    if all_scraped_data:
                        st.session_state['scraped_races'] = all_scraped_data
                        st.sidebar.success(f"{len(all_scraped_data)}レース取得成功！")
                        st.rerun() 
                    else:
                        st.sidebar.warning("開催データが見つかりませんでした (中止・順延の可能性があります)")
                        
                except Exception as e:
                    st.sidebar.error(f"取得エラー: {e}")

    # Load from DB Feature (User Request)
    st.sidebar.markdown("---")
    with st.sidebar.expander("📂 保存済みデータを読込", expanded=False):
        load_date = st.date_input("開催日選択", datetime.today())
        

             
        d_str = load_date.strftime("%Y年%m月%d日")
        
        # New: Venue Check (User Request 2025-12-29)
        d_str = load_date.strftime("%Y年%m月%d日")
        
        if st.button("開催場を確認"):
             with st.spinner("DB確認中..."):
                 venues = db_utils.get_available_venues(d_str)
                 if venues:
                     st.session_state['db_found_venues'] = venues
                     st.session_state['db_check_date'] = d_str
                 else:
                     st.warning(f"{d_str} のデータはありません")
                     if 'db_found_venues' in st.session_state: del st.session_state['db_found_venues']

        # Show MultiSelect if venues found
        target_venues = None
        if 'db_found_venues' in st.session_state and st.session_state.get('db_check_date') == d_str:
             all_v = st.session_state['db_found_venues']
             target_venues = st.multiselect("読み込む場を選択", all_v, default=all_v)

        if st.button("DBから読み込む"):
            with st.spinner("データを検索中..."):
                try:
                    # Pass filtered venues if selected
                    loaded_data = db_utils.load_races_as_batch(d_str, target_venues=target_venues)
                    
                    if loaded_data:
                        # Sanitize Loaded Data
                        for r_dat in loaded_data:
                            if 'df' in r_dat and not r_dat['df'].empty:
                                if '競走得点' in r_dat['df'].columns:
                                    def clean_sc(x):
                                        s = str(x).strip()
                                        import re
                                        # Robust extraction: 2-3 digits, optional dot, optional 1-2 decimals
                                        m = re.search(r'(\d{2,3}(\.\d{1,2})?)', s)
                                        try:
                                            return float(m.group(1)) if m else 0.0
                                        except: return 0.0
                                    r_dat['df']['競走得点'] = r_dat['df']['競走得点'].apply(clean_sc)
                                if '車番' in r_dat['df'].columns:
                                     r_dat['df']['車番'] = pd.to_numeric(r_dat['df']['車番'], errors='coerce').fillna(0).astype(int)

                        st.session_state['scraped_races'] = loaded_data
                        st.sidebar.success(f"{len(loaded_data)}レース 読み込み成功！")
                        st.rerun()
                    else:
                        st.sidebar.warning(f"{d_str} のデータが見つかりませんでした。")
                except Exception as ex:
                    st.sidebar.error(f"読込エラー: {ex}")

    # Save to DB Feature
    if 'scraped_races' in st.session_state and st.session_state['scraped_races']:
        st.sidebar.markdown("---")
        if st.session_state['scraped_races']:
            st.info(f"取得済みレース数: {len(st.session_state['scraped_races'])}件")
            
            # Force Overwrite Option (User Request)
            overwrite_db = st.checkbox("既にデータがあっても上書きする (Force Overwrite)", value=False)
            
            # Sequential Save Button
            if st.button(f"取得した{len(st.session_state['scraped_races'])}レースをDB保存"):
                # Sequential Save Logic
                total_races = len(st.session_state["scraped_races"])
                progress_bar = st.progress(0)
                status_text = st.empty()
                success_count = 0
                error_count = 0
                
                msg_container = st.sidebar.container()
                
                with st.spinner("DBに1件ずつ保存中..."):
                    for i, r_data in enumerate(st.session_state["scraped_races"]):
                        label = r_data.get("label", f"Race {i+1}")
                        status_text.text(f"保存中 ({i+1}/{total_races}): {label}")
                        try:
                            if "df" in r_data and not r_data["df"].empty:
                                df_chk = r_data["df"]
                                if "車番" in df_chk.columns:
                                    df_chk["車番"] = pd.to_numeric(df_chk["車番"], errors="coerce").fillna(0).astype(int)
                                for col in ["期別", "年齢", "枠番"]:
                                    if col in df_chk.columns:
                                        df_chk[col] = pd.to_numeric(df_chk[col], errors="coerce").fillna(0).astype(int)
                                if "脚質" in df_chk.columns:
                                    df_chk["脚質"] = df_chk["脚質"].fillna("").astype(str)
                                if "競走得点" in df_chk.columns:
                                    df_chk["競走得点"] = pd.to_numeric(df_chk["競走得点"], errors="coerce").fillna(0.0)
                            c, msg = db_utils.save_race_data([r_data], overwrite=overwrite_db)
                            if c > 0: success_count += c
                        except Exception as e:
                            error_count += 1
                            msg_container.error(f"Error {label}: {e}")
                        progress_bar.progress((i + 1) / total_races)
                status_text.text("完了！")
                progress_bar.progress(1.0)
                if success_count > 0: st.sidebar.success(f"{success_count}レース 保存完了！")
                elif error_count > 0: st.sidebar.warning(f"{error_count}件のエラーあり")
                else: st.sidebar.info("新規保存なし")
           
        # Fallback: Load from Memory (User Request)
        if st.session_state['scraped_races']:
             st.sidebar.markdown("---")
             st.sidebar.markdown("**お困りの場合:**")
             if st.sidebar.button("直前に取得したデータを表示 (DB介さず)"):
                 st.session_state['scraped_races'] = st.session_state['scraped_races'] # Trigger rerun logic?
                 # Actually, logic checks 'scraped_races'. Simple rerun might do it.
                 # But we need to ensure the main area renders it.
                 # Main area renders `st.session_state['scraped_races']` if present.
                 st.sidebar.success("直前のデータを表示します！")
                 st.rerun()

    # --- Batch Prediction Button (New Feature) ---
    if 'scraped_races' in st.session_state and st.session_state['scraped_races']:
        st.sidebar.markdown("---")
        if st.sidebar.button("🚀 全レース予想＆保存 (標準詳細)"):
            r_list = st.session_state['scraped_races']
            total_r = len(r_list)
            
            # Progress UI
            prog_bar = st.sidebar.progress(0)
            status_txt = st.sidebar.empty()
            saved_count = 0
            
            for i, r_dat in enumerate(r_list):
                if 'df' not in r_dat or r_dat['df'].empty: continue
                
                df_race = r_dat['df']
                meta = r_dat['meta']
                place_name = meta.get('place')
                race_num = meta.get('race_num')
                
                status_txt.text(f"予想中 ({i+1}/{total_r}): {place_name} {race_num}R")
                
                try:
                    # 1. Scoring (Full Pipeline)
                    # Support Date/Place if missing
                    if '競輪場' not in df_race.columns and place_name: df_race['競輪場'] = place_name
                    if '日付' not in df_race.columns and meta.get('date'): df_race['日付'] = meta.get('date')
                    if 'レース番号' not in df_race.columns and race_num: df_race['レース番号'] = race_num

                    # Clean Score (Robust)
                    if '競走得点' in df_race.columns:
                        def clean_input_score_batch(x):
                            s = str(x).strip()
                            import re
                            m = re.search(r'(\d{2,3}(\.\d{1,2})?)', s)
                            try: return float(m.group(1)) if m else 0.0
                            except: return 0.0
                        df_race['競走得点'] = df_race['競走得点'].apply(clean_input_score_batch)

                    if '車番' in df_race.columns:
                         df_race['車番'] = pd.to_numeric(df_race['車番'], errors='coerce').fillna(0).astype(int)

                    # Full Features
                    df_target = db_utils.run_global_features(df_race)
                    df_target = db_utils.run_race_features(df_target)
                    
                    # Use AI Logic (V3) for Unified Prediction
                    df_scored = logic_v2.calculate_ai_score(df_target)
                    
                    # Final Score uses ai_score from classic (no separate bonus)
                    df_scored['final_score'] = pd.to_numeric(df_scored.get('ai_score', 0), errors='coerce').fillna(0.0)
                    df_scored['ai_bonus'] = 0.0

                    # 2. Strategy (AI Logic V3)
                    strategy_data = logic_v2.generate_betting_strategy(df_scored, score_col='final_score')
                    
                    # 3. Check for Suji-Fix (激熱) using same check (simplified)
                    race_type_for_exclusion = strategy_data.get('type', 'standard')
                    
                    # Skip saving suji_fix (激熱) races
                    if race_type_for_exclusion == 'suji_fix':
                        print(f"Skipping suji_fix race: {place_name} {race_num}R")
                        continue
                    
                    # 4. Save
                    d_raw = meta.get('date', datetime.now().strftime('%Y年%m月%d日'))
                    # Clean Date
                    d_clean = d_raw.replace('-', '年').replace('/', '年')
                    if '年' not in d_clean:
                         try: d_clean = datetime.strptime(d_raw, "%Y-%m-%d").strftime("%Y年%m月%d日")
                         except: pass
                    
                    # Fix 1RR issue
                    r_num_str = str(race_num)
                    if not r_num_str.endswith('R'):
                        r_num_str += 'R'
                    
                    # Generate Hash ID
                    import hashlib
                    raw_str = f"{d_clean}{place_name}{r_num_str}"
                    race_id = hashlib.md5(raw_str.encode()).hexdigest()

                    st_title = strategy_data.get('title', '標準')
                    st_reason = strategy_data.get('reason', '')
                    
                    pred_data = {
                        "race_id": race_id,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "place": place_name,
                        "race_num": r_num_str,
                        "date": d_clean,
                        "prediction_text": f"【{st_title}】{st_reason} (一括)",
                        "tickets": strategy_data.get('tickets', []),
                        "structured_bets": strategy_data.get('structured_bets', []),  # Added for stats calc
                        "strategy_title": st_title,
                        "strategy_type": "classic", # Changed to Classic
                        "race_type": strategy_data.get('type', 'standard'),
                        "ai_indices": df_scored[['車番', 'final_score', '選手名', 'ai_tag']].to_dict('records') if 'final_score' in df_scored.columns else []
                    }
                    
                    if db_utils.save_prediction(pred_data):
                        saved_count += 1
                        
                except Exception as e:
                    print(f"Batch Error {place_name} {race_num}R: {e}")
                    
                prog_bar.progress((i + 1) / total_r)
            
            status_txt.text("完了!")
            st.sidebar.success(f"✅ {saved_count} レースの予想を保存しました！")

# 1. 過去レース検索 (DB)

if os.path.exists(db_utils.DB_PATH):
    # A. Search Scope
    with st.sidebar.expander("1. 検索対象・期間", expanded=True):
        # Define Special Categories
        ALL_PLACES = "全競輪場"
        BANK_33 = "33バンク"
        BANK_500 = "500バンク"
        
        # Lists
        list_33 = ["前橋", "松戸", "小田原", "伊東", "奈良", "防府", "富山"]
        list_500 = ["宇都宮", "大宮", "高知"]
        
        # Mix options
        base_opts = [ALL_PLACES, BANK_33, BANK_500]
        venue_opts = base_opts + list(db_utils.TRACK_PREFECTURE_MAP.keys())
        
        venue_sb = st.selectbox("競輪場", venue_opts, index=0, key="sb_venue")
        
        # Year Multiselect (2016-2025)
        years = list(range(2016, 2026))
        selected_years = st.multiselect("対象年度", years, default=[2023, 2024, 2025], key="sb_years")
    
    # B. Player & Line Filters
    with st.sidebar.expander("2. 選手・ライン条件", expanded=False):
        # Player Name
        search_name = st.text_input("選手名 (部分一致)", key="sb_name")
        
        # Line Info
        f_longest = st.checkbox("最長ラインの選手のみ", key="sb_longest")
        
        # Line Length Slider
        # 1 (Tanki) to 5+
        f_line_len = st.slider("ライン長 (0=指定なし)", 0, 5, 0, key="sb_len")
        
        # Line Position
        f_line_pos = st.slider("ライン内位置 (0=指定なし)", 0, 5, 0, key="sb_pos")
        
        # Line Strength (Head/Second)
        # Assuming db_utils has 'line_strength_head' (Str) -> "強", "中", "弱"
        st.caption("ライン強度判定")
        f_str_head = st.multiselect("先行強度", ["強", "中", "弱", "無"], default=[], key="sb_str_h")
        f_str_sec  = st.multiselect("番手強度", ["強", "中", "弱", "無"], default=[], key="sb_str_s")
        
        f_jimoto = st.checkbox("地元選手のみ (Home)", key="sb_jimoto")

    # C. Tactic & Ability Filters
    with st.sidebar.expander("3. 戦法・能力値", expanded=False):
        # "Most in Race" Flags (is_top_nige, etc.)
        st.caption("レース内No.1 (最大値を持つ選手)")
        c1, c2 = st.columns(2)
        f_top_nige = c1.checkbox("逃げ最多", key="sb_t_nige")
        f_top_maku = c2.checkbox("捲り最多", key="sb_t_maku")
        f_top_sashi = c1.checkbox("差し最多", key="sb_t_sashi")
        
        # Fav Tactic
        st.caption("得意戦法 (基本戦法)")
        f_tactics = st.multiselect("戦法タイプ", ["逃", "捲", "差", "マ"], default=[], key="sb_tac")

    # D. Action
    if st.sidebar.button("詳細検索実行", type="primary"):
        if not selected_years:
            st.sidebar.error("年度を選択してください")
        else:
            try:
                # 1. Calc Date Range from Years
                min_y = min(selected_years)
                max_y = max(selected_years)
                s_date = f"{min_y}-01-01"
                e_date = f"{max_y}-12-31"
                
                with st.spinner("DB検索中..."):
                    # Resolve Venue Param
                    if venue_sb == "全競輪場":
                        target_venue = None
                    elif venue_sb == "33バンク":
                        target_venue = ["前橋", "松戸", "小田原", "伊東", "奈良", "防府", "富山"]
                    elif venue_sb == "500バンク":
                        target_venue = ["宇都宮", "大宮", "高知"]
                    else:
                        target_venue = venue_sb

                    # Load Raw Data
                    df_res = db_utils.load_races_from_db(target_venue, s_date, e_date)
                
                if df_res.empty:
                    st.sidebar.warning("データが見つかりませんでした")
                else:
                    # 2. Filter Process
                    # Filter by Year Exact (since range might include unselected middle years)
                    if 'date_dt' not in df_res.columns:
                        # Re-parse if needed, or assume '日付' string
                        # db_utils load returns '日付' as string "YYYY年MM月DD日" (converted in logic?) 
                        # load_races_from_db returns '日付' as "YYYY年MM月DD日".
                        # Let's convert to year.
                        def get_year(s):
                            try: return int(s[:4]) # "2023年..."
                            except: return 0
                        df_res['year_temp'] = df_res['日付'].apply(get_year)
                        df_res = df_res[df_res['year_temp'].isin(selected_years)]
                    
                    # Name Filter
                    if search_name:
                        df_res = df_res[df_res['選手名'].astype(str).str.contains(search_name)]
                    
                    # Line Filters
                    if f_longest and 'is_longest_line' in df_res.columns:
                        df_res = df_res[df_res['is_longest_line'] == 1]
                        
                    if f_line_len > 0 and 'line_length' in df_res.columns:
                        df_res = df_res[df_res['line_length'] == f_line_len]
                        
                    if f_line_pos > 0 and 'line_pos' in df_res.columns:
                        df_res = df_res[df_res['line_pos'] == f_line_pos]

                    # Strength
                    if f_str_head and 'line_strength_head' in df_res.columns:
                        df_res = df_res[df_res['line_strength_head'].isin(f_str_head)]
                    if f_str_sec and 'line_strength_second' in df_res.columns:
                        df_res = df_res[df_res['line_strength_second'].isin(f_str_sec)]

                    # Jimoto
                    if f_jimoto and 'is_jimoto' in df_res.columns:
                        df_res = df_res[df_res['is_jimoto'] == 1]

                    # Tactics High
                    if f_top_nige and 'is_top_nige' in df_res.columns:
                        df_res = df_res[df_res['is_top_nige'] == 1]
                    if f_top_maku and 'is_top_makuri' in df_res.columns:
                        df_res = df_res[df_res['is_top_makuri'] == 1] 
                    if f_top_sashi and 'is_top_sashi' in df_res.columns:
                         df_res = df_res[df_res['is_top_sashi'] == 1]
                    
                    # Fav Tactic (String Match)
                    if f_tactics and 'fav_tactic' in df_res.columns:
                         df_res = df_res[df_res['fav_tactic'].isin(f_tactics)]

                    # Result
                    st.session_state['search_result_db'] = df_res
                    st.sidebar.success(f"検索完了: {len(df_res)}件")
                    
                    # Player Stats Summary (If filtered by name and results exist)
                    if search_name and not df_res.empty:
                        st.session_state['player_stats_summary'] = True
                    else:
                        st.session_state.pop('player_stats_summary', None)

            except Exception as e:
                st.sidebar.error(f"検索エラー: {e}")

    # E. Settings & Help
    st.sidebar.markdown("---")
    st.sidebar.header("⚙️ 設定・ヘルプ")
    
    # API Key Persistence
    API_KEY_FILE = "api_key_secret.txt"
    loaded_key = ""
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r") as f:
                loaded_key = f.read().strip()
        except: pass

    api_key_input = st.sidebar.text_input("Gemini API Key", value=loaded_key, type="password", help="AIレポート生成に必要です")
    
    # Save if changed
    if api_key_input != loaded_key:
        with open(API_KEY_FILE, "w") as f:
            f.write(api_key_input)
        st.sidebar.success("APIキーを保存しました")
    
    with st.sidebar.expander("📚 用語・ロジック解説"):
        st.markdown("""
        ### 👑 ロジック V3 (New)
        *   **🌟 圧倒的捲り (Dom Makuri)**: 捲り回数が圧倒的(5回以上かつ他を圧倒)。SS級の信頼度。
        *   **🏃 圧倒的逃げ (Dom Nige)**: 短走路で圧倒的な逃げ回数を持つ選手。押し切り濃厚。
        *   **🚀 B-Top (Back Leader)**: バック回数トップの選手。短走路ではライン決着、長走路では連対率が高い。
        *   **🛡️ 激戦区 (Conflict)**: 逃げ選手が3名以上いるレース。潰し合いによる差し有利や、最強逃げの独走を判定。

        ### 🎯 予想タイプ (Race Type)
        *   **🏰 鉄板銀行 (Teppan)**: 1強、またはラインが強力でスジ決着が濃厚なレース。点数を絞って厚張り推奨。
        *   **⚔️ 有力スジ (Suji-Lead)**: ラインでの決着が有力だが、ヒモ荒れの可能性もあるレース。
        *   **⚡ ラインブレイカー (Breaker)**: スジ決着が崩れやすく、別線や単騎が絡む混戦レース。
        *   **💰 一撃回収 (Snipe)**: 期待値の高い穴選手(AI選出)がいるレース。

        ### 📊 選手特性 (Player Stats)
        *   **⚠️ ライン乖離**: 自分が好走しても番手が千切れやすい選手。
        *   **🔄 混戦浮上**: 展開が縺れた時に浮上する穴候補。
        *   **🗡️ 差し逆転**: 番手からキッチリ差し切るタイプ。
        *   **💒 相性良**: このバンクでの連対率が非常に高い選手。

        ### 📏 バンク特徴 (Specs)
        *   **短直線 (<50m)**: 逃げ有利 (前橋,小田原など)
        *   **長直線 (>58m)**: 捲り・差し有利 (大宮,武雄など)
        """)

# ==========================================
# タブ構成
# ==========================================
# ==========================================
# タブ構成
# ==========================================
st.title("競輪DeepDive｜データで熱狂をつかめ!")
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 出走表・AI予想", "🕵️ 選手・ロジック検索", "📈 エンジン状態", "⚙️ 設定", "📜 AI的中履歴"])

# Display Search Results from Sidebar (Global Area)
if 'search_result_db' in st.session_state:
    res_df = st.session_state['search_result_db']
    
    # 選手成績サマリー表示 (名前検索時)
    if st.session_state.get('player_stats_summary'):
        st.info(f"📊 選手成績サマリー (対象期間: {len(res_df)}走)")
        
        # 集計
        # Win stats
        if '着順_val' in res_df.columns:
            total = len(res_df)
            w1 = len(res_df[res_df['着順_val'] == 1])
            w2 = len(res_df[res_df['着順_val'] <= 2])
            w3 = len(res_df[res_df['着順_val'] <= 3])
            
            w1_rate = w1 / total if total > 0 else 0
            w2_rate = w2 / total if total > 0 else 0
            w3_rate = w3 / total if total > 0 else 0
            
            # S/B Stats (Mean of Pre-Race counts)
            # Note: Scraped 'S'/'B' are usually period totals held by player, NOT "Took S in this race".
            # So we show Mean (Average holding)
            # Ensure numeric calc
            num_cols = ['S', 'B', '逃', '捲', '差', 'マ', '競走得点']
            for c in num_cols:
                if c in res_df.columns:
                    res_df[c] = pd.to_numeric(res_df[c], errors='coerce').fillna(0)

            s_mean = 0
            b_mean = 0
            
            try:
                if 'S' in res_df.columns:
                   s_mean = pd.to_numeric(res_df['S'], errors='coerce').fillna(0).mean()
                if 'B' in res_df.columns:
                   b_mean = pd.to_numeric(res_df['B'], errors='coerce').fillna(0).mean()
            except: pass
            

            # Fav Tactic (Mode)
            fav_tac = "不明"
            if '脚質' in res_df.columns:
                fav_tac = res_df['脚質'].mode()[0] if not res_df['脚質'].mode().empty else "不明"
            
            # Ability Stats (Mean)
            try:
                a_nige = pd.to_numeric(res_df['逃'], errors='coerce').fillna(0).mean() if '逃' in res_df.columns else 0
                a_maku = pd.to_numeric(res_df['捲'], errors='coerce').fillna(0).mean() if '捲' in res_df.columns else 0
                a_sashi = pd.to_numeric(res_df['差'], errors='coerce').fillna(0).mean() if '差' in res_df.columns else 0
                a_mark = pd.to_numeric(res_df['マ'], errors='coerce').fillna(0).mean() if 'マ' in res_df.columns else 0
            except Exception as e:
                st.warning(f"Stat Calc Error: {e}")
                a_nige, a_maku, a_sashi, a_mark = 0, 0, 0, 0

            # Display Metrics
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("勝率", f"{w1_rate:.1%}", f"{w1}勝")
            m2.metric("2連対率", f"{w2_rate:.1%}", f"{w2}回")
            m3.metric("3連対率", f"{w3_rate:.1%}", f"{w3}回")
            m4.metric("平均S保持", f"{s_mean:.1f}回", "直近平均")
            m5.metric("平均B保持", f"{b_mean:.1f}回", "直近平均")
            
            st.caption(f"**脚質傾向 (平均回数)**: 逃:{a_nige:.1f}  捲:{a_maku:.1f}  差:{a_sashi:.1f}  マ:{a_mark:.1f}  (最多脚質: {fav_tac})")
            st.caption("※ S/B保持数は、各レース出場時点での出走表データ（期別合計）の平均値です。")

    st.dataframe(res_df)
    if st.button("検索結果を閉じる"):
        del st.session_state['search_result_db']
        if 'player_stats_summary' in st.session_state:
             del st.session_state['player_stats_summary']
        st.rerun()

# ------------------------------------------
# Tab 1: 出走表・AI予想 (メイン機能)
# ------------------------------------------
with tab1:
    st.header("出走表解析")
    
    uploaded_files = st.file_uploader("楽天Kドリームスの出走表(HTML)をアップロード（複数選択可）", type=['html', 'htm'], accept_multiple_files=True)
    
    # Merge Uploaded Files + Scraped Data
    all_race_data = []

    # A) Uploaded Files
    if uploaded_files:
        for f in uploaded_files:
            content = f.read().decode("utf-8", errors="ignore")
            # Use direct HTML cell parser for accurate column extraction
            df_curr, meta_curr = logic_v2.parse_kdreams_direct(content)
            
            if not df_curr.empty:
                # Meta info for label
                r_num = "??R"
                if 'レース番号' in df_curr.columns:
                    r_num = f"{df_curr['レース番号'].iloc[0]}R"
                elif meta_curr.get('race_num'):
                     r_num = f"{meta_curr['race_num']}R"
                     
                p_name = meta_curr.get('place', '不明')
                r_class = meta_curr.get('race_class', '')
                
                label = f"{p_name} {r_num} {r_class}"
                
                # Get integer race num for sorting
                r_num_int = 0
                if 'レース番号' in df_curr.columns:
                     try: r_num_int = int(df_curr['レース番号'].iloc[0])
                     except: pass
                elif meta_curr.get('race_num'):
                     try: r_num_int = int(meta_curr.get('race_num'))
                     except: pass

                all_race_data.append({
                    'label': label,
                    'df': df_curr,
                    'meta': meta_curr,
                    'filename': f.name,
                    'sort_key': (p_name, r_num_int)
                })

    # B) Scraped Races (Stored in Session State)
    if 'scraped_races' in st.session_state:
        for scraped in st.session_state['scraped_races']:
            all_race_data.append(scraped)
            
    race_data_list = all_race_data

    if not race_data_list:
        st.error("有効なレースデータが見つかりませんでした。")
    else:
        # --- Batch Analysis Section (User Request) ---
        with st.expander("📋 全レース AI分析サマリー (一括予想)", expanded=False):
            st.info(f"読み込み済みレース数: {len(race_data_list)}件")
            
            # Check for cached summary
            if 'batch_analysis_summary' in st.session_state and st.session_state['batch_analysis_summary'] is not None:
                 st.info("前回の分析結果を表示します")
                 st.dataframe(st.session_state['batch_analysis_summary'], use_container_width=True)
                 if st.button("分析結果をクリア"):
                     st.session_state['batch_analysis_summary'] = None
                     st.rerun()

            if st.button("全レースを一括分析する"):
                summary_rows = []
                progress_bar = st.progress(0)
                
                for i, r_data in enumerate(race_data_list):
                    progress_bar.progress((i + 1) / len(race_data_list))
                    
                    df_target = r_data['df']
                    meta_target = r_data['meta']
                    try:
                        # 1. Advanced Logic for Prediction
                        p_name = meta_target.get('place', '')
                        r_cls = meta_target.get('race_class', 'A級')
                        # 0. Pre-process Features (Must be same as single view)
                        if '競輪場' not in df_target.columns and p_name: df_target['競輪場'] = p_name
                        if '日付' not in df_target.columns and meta_target.get('date'): df_target['日付'] = meta_target.get('date')
                        if 'レース番号' not in df_target.columns and meta_target.get('race_num'): df_target['レース番号'] = meta_target.get('race_num')
                        
                        # Sanitize Input DataFrame Types
                        if '競走得点' in df_target.columns:
                            # Protect against double concatenation (85.1285.12) or list-string
                            def clean_input_score(x):
                                s = str(x).strip()
                                import re
                                # Match 2-3 digits, optional dot, optional 1-2 decimals
                                m = re.search(r'(\d{2,3}(\.\d{1,2})?)', s)
                                if m: 
                                    try: return float(m.group(1))
                                    except: return 0.0
                                return 0.0
                            df_target['競走得点'] = df_target['競走得点'].apply(clean_input_score)
                        
                        if '車番' in df_target.columns:
                             df_target['車番'] = pd.to_numeric(df_target['車番'], errors='coerce').fillna(0).astype(int)
                        
                        df_target = db_utils.run_global_features(df_target)
                        df_target = db_utils.run_race_features(df_target)
                        
                        # Use CLASSIC Logic for Unified Prediction
                        df_scored = logic_v2.calculate_classic_score(df_target)

                        # Legacy Metrics for "Trend" (User Request: 鉄板/混戦 etc.)
                        legacy_metrics = logic_v2.calculate_advanced_metrics(df_target)
                        trend_signals = legacy_metrics.get('signals', [])
                        trend_str = " ".join(trend_signals) if trend_signals else "-"
                        
                        # Final Score uses ai_score from classic (no separate bonus)
                        df_scored['final_score'] = pd.to_numeric(df_scored.get('ai_score', 0), errors='coerce').fillna(0.0)
                        df_scored['ai_bonus'] = 0.0

                        if 'final_score' in df_scored.columns:
                            top_row = df_scored.sort_values('final_score', ascending=False).iloc[0]
                            top_name = top_row['選手名']
                            top_score = top_row['final_score']
                            
                            confidence = "◎" if top_score >= 80 else "○"
                            if top_score >= 85: confidence = "★"
                            
                            top3_df = df_scored.sort_values('final_score', ascending=False).head(3)
                            top3_nums = top3_df['車番'].tolist()
                            pred_str = "-".join(map(str, top3_nums))
                            
                            
                            # --- Automatic History Save (Classic Logic) ---
                            strategy_data = logic_v2.generate_classic_strategy(df_scored, score_col='final_score')
                            
                            # Check for Suji-Fix (激熱) using hybrid check for exclusion
                            hybrid_check = logic_v2.generate_betting_strategy(df_scored, score_col='final_score')
                            race_type_for_exclusion = hybrid_check.get('type', 'standard')

                            # --- AUTO SAVE TO HISTORY ---
                            # RELAXED: Save if tickets exist OR type is valid, BUT skip suji_fix
                            is_valid = strategy_data.get('type') not in ['error', 'skip']
                            if strategy_data.get('tickets'): is_valid = True
                            
                            # Exclude L-Class / Girls Keirin (User Request)
                            if 'L級' in r_cls or 'ガールズ' in r_cls:
                                is_valid = False
                                
                            # Skip suji_fix (激熱) races - REMOVED per user request
                            # if race_type_for_exclusion == 'suji_fix':
                            #     is_valid = False

                            if is_valid:
                                try:
                                    p_name = meta.get('place', '')
                                    r_num = meta.get('race_num', '??R')
                                    d_raw = meta.get('date', '')
                                    d_clean = d_raw.replace('-', '年').replace('/', '年')
                                    if '年' not in d_clean: 
                                         try: d_clean = datetime.strptime(d_raw, "%Y-%m-%d").strftime("%Y年%m月%d日")
                                         except: pass
                                    
                                    # Fix 1RR issue
                                    r_num_str = str(r_num)
                                    if not r_num_str.endswith('R'):
                                        r_num_str += 'R'
                                    
                                    # Generate Hash ID
                                    import hashlib
                                    raw_str = f"{d_clean}{p_name}{r_num_str}"
                                    race_id = hashlib.md5(raw_str.encode()).hexdigest()

                                    st_title = strategy_data.get('title', '標準')
                                    st_reason = strategy_data.get('reason', '')
                                    
                                    pred_dict = {
                                        "race_id": race_id,
                                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "place": p_name,
                                        "race_num": r_num_str,
                                        "date": d_clean,
                                        "prediction_text": f"【{st_title}】{st_reason} (一括)",
                                        "tickets": strategy_data.get('tickets', []),
                                        "strategy_title": st_title,
                                        "strategy_type": "classic", # Changed to Classic
                                        "race_type": strategy_data.get('type', 'standard'),
                                        "ai_indices": df_scored[['車番', 'final_score', '選手名', 'ai_tag']].to_dict('records') if 'final_score' in df_scored.columns else []
                                    }
                                    res = db_utils.save_prediction(pred_dict)
                                except Exception as e_s: print(f"Save Error: {e_s}")

                                
                            # Calculate Gap and Bonus
                            score_gap = 0.0
                            max_bonus_val = 0.0
                            
                            try:
                                # Bonus of the Top Pick (Honmei)
                                max_bonus_val = float(top_row.get('ai_bonus', 0.0))
                                
                                # Score Gap (1st - 2nd)
                                if len(df_scored) >= 2:
                                    # df_scored is not sorted in place above, top_row is from sorted copy? No, line 676: df_scored.sort_values...iloc[0]
                                    # But df_scored itself is not sorted.
                                    # top3_df is consistent.
                                    s1 = float(top3_df.iloc[0]['final_score'])
                                    s2 = float(top3_df.iloc[1]['final_score'])
                                    score_gap = s1 - s2
                            except: pass
                                
                            summary_rows.append({
                                "レース": r_data['label'],
                                "レース傾向": trend_str, 
                                "本命選手": top_name,
                                "最大加点": f"{max_bonus_val:+.1f}",
                                "指数差(1-2位)": f"{score_gap:.1f}",
                                "確度": confidence
                            })
                    except Exception as e:
                        print(f"Batch Error: {e}")
                        st.error(f"Error processing {r_data['label']}: {e}")
                        pass
                
                progress_bar.empty()
                if summary_rows:
                    st.success(f"{len(summary_rows)}レースの分析が完了しました！")
                    
                    # Order columns nicely
                    df_summary = pd.DataFrame(summary_rows)
                    cols = ["レース", "レース傾向", "本命選手", "最大加点", "指数差(1-2位)", "確度"]
                    # Ensure cols exist (in case empty)
                    df_summary = df_summary[cols]
                    
                    st.session_state['batch_analysis_summary'] = df_summary
                    st.dataframe(df_summary, use_container_width=True)
                else:
                    st.warning("分析データが生成できませんでした。")

        # 2. Race Selection UI
        # Sort by Venue, then Race Number
        race_data_list.sort(key=lambda x: x['sort_key'])
        
        st.markdown("---")
        
        # --- Button Grid Logic ---
        # 1. Initialize State
        if 'selected_race_label' not in st.session_state:
            st.session_state['selected_race_label'] = race_data_list[0]['label']
        
        # 2. Group by Venue
        from itertools import groupby
        
        # Ensure sorted by Venue for grouping
        race_data_list.sort(key=lambda x: (x['sort_key'][0], x['sort_key'][1]))
        
        # 3. Render Buttons
        st.write("▼ 分析するレースを選択してください")
        
        for venue, items in groupby(race_data_list, key=lambda x: x['sort_key'][0]):
            st.subheader(f"🏟️ {venue}")
            # Create columns for buttons (e.g. 6 per row)
            items_list = list(items)
            cols = st.columns(6)
            for idx, item in enumerate(items_list):
                c = cols[idx % 6]
                r_num_str = f"{item['sort_key'][1]}R"
                label = item['label']
                filename = item.get('filename', str(idx))  # Unique per file
                
                # Style active button
                is_active = (st.session_state['selected_race_label'] == label)
                if c.button(f"{r_num_str}", key=f"btn_{label}_{filename}", type="primary" if is_active else "secondary"):
                    st.session_state['selected_race_label'] = label
                    st.rerun()

        # 4. Get Selected Data
        selected_label = st.session_state['selected_race_label']
        # Fallback if selection missing from current list (re-upload etc)
        target_data = next((d for d in race_data_list if d['label'] == selected_label), None)
        if not target_data and race_data_list:
            target_data = race_data_list[0]
            st.session_state['selected_race_label'] = target_data['label']
        
        if target_data:
            # IMPORTANT: creating a copy is essential to prevent mutating cached objects in session_state
            df_race = target_data['df'].copy()
            meta = target_data['meta']
            place_name = meta.get('place', '')
            race_class = meta.get('race_class', 'A級')
            
            st.success(f"📍 {selected_label} - 解析中...")
            st.caption(f"File: {target_data['filename']}")
            
            # DEBUG: Dump DF for inspection
            try:
                df_race.to_csv("debug_race_df.csv", index=False)
            except: pass
            
            # --- Continue Analysis below ---
            
            # 1. 基本特徴量の生成 (地元判定など)
            if '競輪場' not in df_race.columns and place_name:
                df_race['競輪場'] = place_name
            if '日付' not in df_race.columns and meta.get('date'):
                df_race['日付'] = meta.get('date')
            if 'レース番号' not in df_race.columns and meta.get('race_num'):
                df_race['レース番号'] = meta.get('race_num')
                
            # --- CRITICAL FIX: Ensure '競走得点' Column Name Consistency ---
            # Search for any column containing '競走得点' (handling whitespace/unicode)
            score_col_candidates = [c for c in df_race.columns if '競走得点' in str(c)]
            if score_col_candidates and '競走得点' not in df_race.columns:
                 # Rename best candidate
                 df_race.rename(columns={score_col_candidates[0]: '競走得点'}, inplace=True)
            elif not score_col_candidates and '得点' in str(df_race.columns):
                 # Last resort: look for '得点' if unique
                 score_c = [c for c in df_race.columns if '得点' in str(c)]
                 if len(score_c) == 1:
                     df_race.rename(columns={score_c[0]: '競走得点'}, inplace=True)
                     
            if '競走得点' in df_race.columns:
                 # Ensure float
                 df_race['競走得点'] = pd.to_numeric(df_race['競走得点'], errors='coerce').fillna(0.0)
            # -----------------------------------------------------------------
                
            try:
                df_race = db_utils.run_global_features(df_race)
                df_race = db_utils.run_race_features(df_race) # Add Specialist Flags
            except Exception as e:
                st.warning(f"特徴量生成スキップ: {e}")
            
            # 2. AIスコアリング (AI Logic V3)
            try:
                df_scored = logic_v2.calculate_ai_score(df_race)
                
                with st.expander("🔍 デバッグ: ロジック投入前のデータ確認"):
                    st.write(f"データ行数: {len(df_scored)}")
                    if 'ライン' in df_scored.columns:
                        st.write("▼ ライン列の生データ (Top 9)")
                        st.table(df_scored[['車番', '選手名', 'ライン']].head(9))
                        st.write("Unique Lines:", df_scored['ライン'].unique())
                    else:
                        st.error("⚠️ 'ライン'カラムが存在しません！")
                    
                    st.write("Engine Stats Cache Keys:", list(engine.stats_cache.keys()))
                
                # 3. ★ 拡張AIロジックの適用 (Polarsエンジン活用) ★
                if place_name:
                    # Debug: Show logic is attempting to run
                    # --- Debug RAW Column Names ---
                    with st.expander("🔍 デバッグ: 生カラム名一覧"):
                        st.write("カラム数:", len(df_scored.columns))
                        # Show columns with INDEX
                        col_list = list(df_scored.columns)
                        for i, c in enumerate(col_list):
                            st.write(f"  [{i}]: {c}")
                        
                        # Show first row data with index
                        if len(df_scored) > 0:
                            st.write("--- 最初の行のデータ ---")
                            sample = df_scored.iloc[0]
                            for i, c in enumerate(col_list):
                                st.write(f"  [{i}] {c} = {sample[c]}")
                        
                    # --- Debug Tactic Flags ---
                    with st.expander("🔍 デバッグ: 逃/捲/差 MAX判定"):
                        if '逃' in df_scored.columns:
                            st.write("逃 列の値:")
                            st.table(df_scored[['車番', '選手名', '逃']].astype(str))
                        if '捲' in df_scored.columns:
                            st.write("捲 列の値:")
                            st.table(df_scored[['車番', '選手名', '捲']].astype(str))
                        if '差' in df_scored.columns:
                            st.write("差 列の値:")
                            st.table(df_scored[['車番', '選手名', '差']].astype(str))
                        
                        if 'is_top_nige' in df_scored.columns:
                            nige_top = df_scored[df_scored['is_top_nige'] == 1]['選手名'].tolist()
                            st.info(f"逃NO1: {nige_top}")
                        if 'is_top_makuri' in df_scored.columns:
                            mak_top = df_scored[df_scored['is_top_makuri'] == 1]['選手名'].tolist()
                            st.info(f"捲NO1: {mak_top}")
                        if 'is_top_sashi' in df_scored.columns:
                            sashi_top = df_scored[df_scored['is_top_sashi'] == 1]['選手名'].tolist()
                            st.info(f"差NO1: {sashi_top}")

                    # st.toast(f"Applying Logic for {place_name} ({race_class})") # Optional toast
                    df_scored = apply_advanced_logic(df_scored, engine, place_name, race_class)
                else:
                    st.error("⚠️ 競輪場名（place_name）が取得できませんでした。ロジック適用をスキップします。")
                    st.write(f"Meta Info: {meta}")
                    
                # POST-LOGIC DEBUG
                with st.expander("🔍 デバッグ: ロジック適用後の詳細 (Stats Status)"):
                        st.write("Loaded Stats Cache Keys:", list(engine.stats_cache.keys()))
                        if 'bonus_reasons' in df_scored.columns:
                            st.write("ボーナス理由サンプル:", df_scored[['選手名', 'bonus_reasons']].head(5))
                        else:
                            st.write("⚠️ bonus_reasons カラムなし")
                        
                        if 'line_len_temp' in df_scored.columns:
                            st.write("ライン長(Temp):", df_scored['line_len_temp'].head())

                # Final Score uses ai_score + advanced bonus
                # Ensure bonus_score is numeric
                df_scored['ai_bonus'] = pd.to_numeric(df_scored.get('bonus_score', 0), errors='coerce').fillna(0.0)
                
                # Base Score (Classic)
                classic_score = pd.to_numeric(df_scored.get('ai_score', 0), errors='coerce').fillna(0.0)
                
                # Final = Classic + Advanced Bonus
                df_scored['final_score'] = classic_score + df_scored['ai_bonus']

    
                # --- 表示 ---
                # Bank Info (New)
                # Returns: (spec_str, desc, fav)
                spec_str, b_desc, b_fav = db_utils.get_bank_characteristics(place_name)
                st.info(f"**🏟️ バンク特徴: {spec_str}**\n\n{b_desc}\n\n👉 **有利な戦法: {b_fav}**")

                # Generate Strategy for Display (AI Logic V3)
                strategy_data = {}
                try:
                    strategy_data = logic_v2.generate_betting_strategy(df_scored, score_col='final_score')
                    structured_bets = strategy_data.get('tickets', [])
                except Exception as e:
                    st.warning(f"戦略生成エラー: {e}")
                    structured_bets = []

                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.subheader("🔥 データで熱狂をつかめ!")
                    
                    # Time Info
                    time_info = []
                    if meta.get('deadline'): time_info.append(f"⏰ 締切: **{meta['deadline']}**")
                    if meta.get('start_time'): time_info.append(f"🔫 発走: **{meta['start_time']}**")
                    
                    if time_info:
                        st.markdown(" ".join(time_info))
                    
                    # Line Info (New)
                    if meta.get('lines_parsed'):
                         st.info(f"🚀 **並び予想**: {meta['lines_parsed']}")
                    
                    # 表示用データの整形
                    display_df = df_scored.copy()
                    
                    # Velodrome to Prefecture mapping
                    velodrome_pref = {
                        "函館": "北海道", "青森": "青森", "いわき平": "福島", 
                        "弥彦": "新潟", "前橋": "群馬", "取手": "茨城", "宇都宮": "栃木",
                        "大宮": "埼玉", "西武園": "埼玉", "京王閣": "東京", "立川": "東京",
                        "松戸": "千葉", "千葉": "千葉", "川崎": "神奈川", "平塚": "神奈川",
                        "小田原": "神奈川", "伊東": "静岡", "静岡": "静岡",
                        "名古屋": "愛知", "豊橋": "愛知", "岐阜": "岐阜", "大垣": "岐阜",
                        "松阪": "三重", "四日市": "三重", "富山": "富山", "福井": "福井",
                        "奈良": "奈良", "向日町": "京都", "和歌山": "和歌山",
                        "岸和田": "大阪", "玉野": "岡山", "広島": "広島", "防府": "山口",
                        "高松": "香川", "小松島": "徳島", "高知": "高知", "松山": "愛媛",
                        "小倉": "福岡", "久留米": "福岡", "武雄": "佐賀", "佐世保": "長崎",
                        "別府": "大分", "熊本": "熊本"
                    }
                    venue_pref = velodrome_pref.get(place_name, "")
                    
                    # AIタグの装飾 (Antigravity理由があれば追加)
                    def format_tags(row):
                        tags = str(row.get('ai_tag', ''))
                        reason = str(row.get('bonus_reasons', '')) # logic_polars uses 'bonus_reasons'
                        if reason and reason != 'nan':
                            tags += reason # brackets already included in bonus_reasons
                        
                        # Check if local player (same prefecture as velodrome)
                        player_pref = str(row.get('府県', '')).strip()
                        is_local = False
                        if venue_pref and player_pref:
                            # Handle variations: "神奈川" vs "神奈川県", etc.
                            if venue_pref in player_pref or player_pref in venue_pref:
                                is_local = True
                        
                        if row.get('is_jimoto') == 1 or is_local:
                            tags = "🏠地元 " + tags
                        return tags.strip()
    
                    display_df['分析コメント'] = display_df.apply(format_tags, axis=1)
                    
                    # 表示カラム
                    cols = ['車番', '選手名', '府県', 'final_score', 'ai_bonus', '分析コメント', '競走得点', '脚質']
                    # 存在確認
                    cols = [c for c in cols if c in display_df.columns]
                    
                    # Ensure numeric for formatting
                    numeric_cols = ['final_score', 'ai_bonus', '競走得点', 'S', 'B', '逃', '捲', '差', 'マ']
                    for nc in numeric_cols:
                        if nc in display_df.columns:
                            display_df[nc] = pd.to_numeric(display_df[nc], errors='coerce').fillna(0.0)

                    st.dataframe(
                        display_df[cols].style
                        .background_gradient(subset=['final_score'], cmap="Purples") # 色設定
                        .format({
                             'final_score': '{:.1f}', 
                             'ai_bonus': '{:+.1f}',
                             '競走得点': '{:.2f}'}, na_rep="0.0"), 
                        use_container_width=True,
                        height=400
                    )
                    
                    # --- 予測率テーブル (Second Table) ---
                    st.markdown("---")
                    st.subheader("📊 予測勝率・連対率テーブル")
                    
                    # Calculate prediction rates based on data logic
                    pred_df = df_scored.copy()
                    
                    # Calculate comprehensive strength score for each player
                    # Base: normalized final_score
                    if 'final_score' in pred_df.columns:
                        base_score = pd.to_numeric(pred_df['final_score'], errors='coerce').fillna(100)
                    elif '競走得点' in pred_df.columns:
                        base_score = pd.to_numeric(pred_df['競走得点'], errors='coerce').fillna(100)
                    else:
                        base_score = pd.Series([100] * len(pred_df))
                    
                    # Apply bonus multipliers based on data-driven factors
                    # CAUTION: final_score already includes AI bonuses. 
                    # Adding multipliers here creates double-counting and flips ranking.
                    # To ensure consistency between AI Analysis (final_score) and Prediction Table,
                    # we do NOT add further multipliers if using final_score.
                    
                    multiplier = pd.Series([1.0] * len(pred_df), index=pred_df.index)
                    
                    # (Legacy multipliers commented out to fix inconsistency)
                    # if 'is_top_nige' in pred_df.columns: multiplier += pred_df['is_top_nige'].fillna(0) * 0.30
                    # if 'is_top_makuri' in pred_df.columns: multiplier += pred_df['is_top_makuri'].fillna(0) * 0.32
                    
                    # Factor 2: Line position advantage (Small adjustment ok?)
                    # If we remove all, we trust final_score 100%.
                    # Let's keep small adjustments if needed, but for now, prioritize consistency.
                    
                    # Calculate weighted strength score
                    # Use final_score (base_score) directly to maintain ranking order
                    strength = base_score * 1.0 # multiplier (disabled)
                    
                    # === 予測勝率 (Sum = 100%) ===
                    # Advanced Logic: Force Top 1 Win Rate based on Class/Venue historical data
                    # (Derived from analyze_top_score_rates.py)
                    
                    import numpy as np
                    
                    # 1. Determine Target Rate for Top 1 Score Player
                    target_top1_rate = 39.3 # Default global average
                    
                    # Check Class (if available)
                    race_class = ""
                    # Try to infer class from race_name in meta or other columns
                    # Simplification: Use heuristics or default
                    # If we had '級班' column, we would use it. 
                    # Here we might need to rely on typical performance if class is unknown.
                    # But actually we can differentiate by Score itself?
                    # High Score (Girls > 50 but scale is different). 
                    # Let's use venue-specific adjustments if available.
                    
                    venue_adjustments = {
                        "平塚": 2.0, "武雄": 1.7, "前橋": 0.7, "防府": -0.3, "玉野": -0.7, "松阪": -1.7
                    }
                    adj = venue_adjustments.get(place_name, 0.0)
                    target_top1_rate += adj

                    # Check for Girls Keirin (L-Class) - usually 7 cars, L codes
                    is_girls = False
                    if len(pred_df) <= 7:
                         # Heuristic: Check if 'L' is in any class code if available, or just check race metadata
                         # If race_name contains 'ガールズ'
                         r_name = meta.get('race_name', '')
                         if 'ガールズ' in r_name or 'L級' in r_name:
                             is_girls = True
                             target_top1_rate = 63.7 # From analysis
                    
                    # Check for Challenge (A3) - usually lower scores?
                    # A3 average is 43.6%
                    # S-Class average is ~35%
                    # If not girls, try to guess class by score average?
                    if not is_girls:
                        avg_score = pred_df['競走得点'].mean() if '競走得点' in pred_df.columns else 80
                        if avg_score < 80: # Challenge likely
                            target_top1_rate = 43.6
                        elif avg_score > 100: # S-Class likely
                            target_top1_rate = 35.0
                    
                    # 2. Calculate initial power-law distribution
                    # Use Power 3.9 as established baseline
                    strength_powered = np.power(strength, 3.9)
                    
                    # 3. Identify Top 1 Player (Based on calculated strength/final AI score)
                    # Use 'strength' which includes final_score + bonuses
                    top_idx = strength.idxmax()
                    
                    # Calculate raw distribution first
                    total_p = strength_powered.sum()
                    if total_p > 0:
                        raw_probs = strength_powered / total_p
                    else:
                        raw_probs = pd.Series([1/len(pred_df)]*len(pred_df), index=pred_df.index)
                    
                    # 4. Apply Target Rate using "Force & Distribute"
                    # We want AI's Top Pick to have `target_top1_rate`.
                    # Valid only if target is reasonable (e.g. < 90%)
                    if 0 < target_top1_rate < 90:
                        top_prob_target = target_top1_rate / 100.0
                        
                        probs = raw_probs.copy()
                        
                        # Set Top 1
                        probs[top_idx] = top_prob_target
                        
                        # Normalize others
                        others_mask = probs.index != top_idx
                        sum_others = probs[others_mask].sum()
                        
                        if sum_others > 0:
                            target_others = 1.0 - top_prob_target
                            probs[others_mask] = probs[others_mask] / sum_others * target_others
                        
                        pred_df['予測勝率'] = (probs * 100).round(1)
                    else:
                         pred_df['予測勝率'] = (raw_probs * 100).round(1)
                    
                    # === 連対期待 (Individual %) ===
                    # Optimized multiplier: 1.97x based on historical analysis (was 1.8x)
                    pred_df['連対期待'] = (pred_df['予測勝率'] * 1.97).clip(upper=95).round(1)
                    
                    # === 3着内期待 (Individual %) ===
                    # Optimized multiplier: 2.76x based on historical analysis (was 2.5x)
                    pred_df['3着内期待'] = (pred_df['予測勝率'] * 2.76).clip(upper=99).round(1)
                    
                    # Prepare display columns
                    pred_cols = ['車番', '選手名', '競走得点']
                    
                    # Ensure numeric for display columns
                    if '競走得点' in pred_df.columns:
                        pred_df['競走得点'] = pd.to_numeric(pred_df['競走得点'], errors='coerce').fillna(0.0)

                    # Add tactic columns if available
                    for tc in ['S', 'B', '逃', '捲', '差', 'マ']:
                        if tc in pred_df.columns:
                            pred_df[tc] = pd.to_numeric(pred_df[tc], errors='coerce').fillna(0)
                        if tc in pred_df.columns:
                            pred_cols.append(tc)
                    
                    pred_cols.extend(['予測勝率', '連対期待', '3着内期待'])
                    
                    # Filter to existing columns
                    pred_cols = [c for c in pred_cols if c in pred_df.columns]
                    
                    # Sort by 予測勝率 descending
                    pred_display = pred_df[pred_cols].sort_values('予測勝率', ascending=False)
                    
                    # Format and display
                    st.dataframe(
                        pred_display.style
                        .background_gradient(subset=['予測勝率'], cmap="Greens")
                        .format({
                            '競走得点': '{:.2f}',
                            '予測勝率': '{:.1f}%',
                            '連対期待': '{:.1f}%',
                            '3着内期待': '{:.1f}%'
                        }),
                        use_container_width=True,
                        height=350
                    )
                
                with col2:
                    st.subheader("🎯 ハイブリッド予想")
                    
                    # --- 6. Structured Tickets ---
                    if structured_bets:
                       # Display Strategy Title & Alert within Col2
                       st.markdown(f"**戦略: {strategy_data.get('title', '標準')}**")
                       
                       # Display Confidence & EV
                       conf = strategy_data.get('confidence_level', '中')
                       ev_comment = strategy_data.get('ev_comment', '')
                       pseudo_ev = strategy_data.get('pseudo_ev', 0)
                       rec_pts = strategy_data.get('recommended_points', {})
                       
                       # Confidence badge color
                       conf_color = "🟢" if conf == "高" else "🟡" if conf == "中" else "🔴"
                       st.markdown(f"**信頼度**: {conf_color} {conf}　|　**期待値 (推定)**: {pseudo_ev:+.2f}")
                       st.caption(ev_comment)
                       
                       # Recommended points
                       pts_str = " / ".join([f"{k}: {v}点" for k, v in rec_pts.items()])
                       st.info(f"💡 **推奨点数**: {pts_str}")
                       
                       strategy_type = strategy_data.get('type', 'standard')
                       if strategy_type in ['snipe', 'chaos']:
                           st.warning("⚠️ 穴気配あり！点数は広めに")
                       
                       # Helper to display tickets cleanly
                       st.markdown("---")
                       for t in strategy_data.get('tickets', []):
                           st.write(f"- {t}")
                       
                       # ==========================================
                       # AUTO SAVE (Single Race View) - Classic Logic
                       # ==========================================
                       # Check for Suji-Fix (激熱) using hybrid check for exclusion
                       hybrid_check = logic_v2.generate_betting_strategy(df_scored, score_col='final_score')
                       race_type_for_exclusion = hybrid_check.get('type', 'standard')
                       
                       # Skip suji_fix (激熱) races from saving
                       if race_type_for_exclusion != 'suji_fix':
                           try:
                               p_name = meta.get('place', '')
                               r_num = meta.get('race_num', '??R')
                               d_raw = meta.get('date', '')
                               d_clean = d_raw.replace('-', '年').replace('/', '年')
                               if '年' not in d_clean: 
                                   try: d_clean = datetime.strptime(d_raw, '%Y-%m-%d').strftime('%Y年%m月%d日')
                                   except: pass
                               
                               # Generate Hash ID if race_id missing
                               race_id = meta.get('race_id')
                               if not race_id:
                                    import hashlib
                                    raw_str = f"{d_clean}{p_name}{r_num}"
                                    race_id = hashlib.md5(raw_str.encode()).hexdigest()

                               # --- Classic Strategy ---
                               st_title = strategy_data.get('title', '標準')
                               st_type = strategy_data.get('type', 'standard')
                               st_reason = strategy_data.get('reason', '')
                               
                               pred_data_classic = {
                                   'race_id': race_id, 
                                   'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                   'place': p_name,
                                   'race_num': r_num,
                                   'date': d_clean,
                                   'prediction_text': f'【{st_title}】{st_reason}',
                                   'tickets': strategy_data.get('tickets', []),
                                   'structured_bets': strategy_data.get('structured_bets', []),  # Added for stats calc
                                   'strategy_title': st_title,
                                   'strategy_type': 'classic',  # Changed to Classic
                                   'race_type': st_type,
                                   'ai_indices': df_scored[['車番', 'final_score', 'ai_bonus', '選手名']].fillna(0).to_dict('records') if 'final_score' in df_scored.columns else []
                               }
                               
                               if pred_data_classic['tickets']:
                                   db_utils.save_prediction(pred_data_classic)
                                       
                           except Exception as e_save:
                               print(f"Auto Save Error: {e_save}")
                       else:
                           # Display message for skipped suji_fix race
                           st.info("このレースは激熱(suji_fix)のため、予想履歴への保存は行いません。")
                       
                       # --- Manual Odds Input (Optional) ---
                       st.markdown("---")
                       with st.expander("📊 オッズ手動入力（任意）", expanded=False):
                           st.caption("kdreamsからオッズをコピペして期待値を計算できます")
                           st.caption("形式例: `5-2: 7.5` または `1-2-3: 25.0`")
                           
                           odds_input_key = f"odds_input_{selected_label}"
                           odds_text = st.text_area(
                               "オッズ貼り付け",
                               height=100,
                               key=odds_input_key,
                               placeholder="例:\n5-2: 7.5\n1-2-3: 25.0\n1=2=3: 8.5"
                           )
                           
                           if odds_text.strip():
                                # Parse pasted odds
                                import re
                                parsed_odds = {}
                                for line in odds_text.strip().split('\n'):
                                    line = line.strip()
                                    if not line:
                                        continue
                                    
                                    # Format 1: "7-9-4: 35.5" or "7-9-4 35.5" (combo and odds separated)
                                    match1 = re.match(r'^[\d]*[\s\t]*(\d+[-=]\d+(?:[-=]\d+)?)[\:\s\t]+(\d+\.?\d*)$', line)
                                    if match1:
                                        combo = match1.group(1)
                                        odds = float(match1.group(2))
                                        parsed_odds[combo] = odds
                                        continue
                                    
                                    # Format 2: "7-9-435.5" (3連単 with odds directly after, e.g. from kdreams copy)
                                    # Pattern: single digit X-X-X followed by decimal number (keirin car numbers are 1-9)
                                    match2 = re.match(r'^[\d]*[\s\t]*(\d)-(\d)-(\d)(\d+\.\d+)$', line)
                                    if match2:
                                        c1, c2, c3, odds_str = match2.groups()
                                        combo = f"{c1}-{c2}-{c3}"
                                        odds = float(odds_str)
                                        parsed_odds[combo] = odds
                                        continue
                                    
                                    # Format 3: "5-235.5" (2車単 with odds directly after)
                                    match3 = re.match(r'^[\d]*[\s\t]*(\d)-(\d)(\d+\.\d+)$', line)
                                    if match3:
                                        c1, c2, odds_str = match3.groups()
                                        combo = f"{c1}-{c2}"
                                        odds = float(odds_str)
                                        parsed_odds[combo] = odds
                                        continue
                                
                                if parsed_odds:
                                    st.success(f"✅ {len(parsed_odds)}件のオッズを解析しました")
                                    
                                    # Extract all combos from strategy_data tickets
                                    # Tickets format: "3連単 (フォーメーション): 2,9 - 2,9,4 - 9,4,7,8"
                                    all_ai_combos = []
                                    tickets = strategy_data.get('tickets', [])
                                    
                                    for ticket in tickets:
                                        if '3連単' in ticket and 'フォーメーション' in ticket:
                                            # Parse formation: "2,9 - 2,9,4 - 9,4,7,8"
                                            try:
                                                parts = ticket.split(':')[1].strip().split(' - ')
                                                if len(parts) == 3:
                                                    pos1 = [x.strip() for x in parts[0].split(',')]
                                                    pos2 = [x.strip() for x in parts[1].split(',')]
                                                    pos3 = [x.strip() for x in parts[2].split(',')]
                                                    # Generate all combinations
                                                    for p1 in pos1:
                                                        for p2 in pos2:
                                                            for p3 in pos3:
                                                                if p1 != p2 and p2 != p3 and p1 != p3:
                                                                    all_ai_combos.append(f"{p1}-{p2}-{p3}")
                                            except:
                                                pass
                                    
                                    # Also add top 3 as fallback
                                    sorted_df = df_scored.sort_values('final_score', ascending=False)
                                    top_cars = [str(sorted_df['車番'].iloc[i]) for i in range(min(3, len(sorted_df)))]
                                    if len(top_cars) >= 3:
                                        all_ai_combos.append(f"{top_cars[0]}-{top_cars[1]}-{top_cars[2]}")
                                    
                                    # Find matches between AI combos and parsed odds
                                    matched_combos = []
                                    for combo in all_ai_combos:
                                        if combo in parsed_odds:
                                            matched_combos.append((combo, parsed_odds[combo]))
                                    
                                    # Display matched combos with EV
                                    if matched_combos:
                                        st.markdown("**🎯 AI推奨 × オッズ照合結果:**")
                                        base_win_rate = strategy_data.get('top_win_rate', 15)  # Base rate for primary
                                        
                                        for i, (combo, odds) in enumerate(sorted(matched_combos, key=lambda x: x[1])):
                                            # Adjust win rate based on position (lower odds = higher rate)
                                            adjusted_rate = base_win_rate * (1 - i * 0.15)  # Decay for lower priority
                                            ev = (adjusted_rate / 100) * odds - 1
                                            ev_color = "🟢" if ev > 0 else "🔴"
                                            st.write(f"  {ev_color} **{combo}**: {odds}倍 → 期待値 {ev:+.2f}")
                                    else:
                                        st.info("AI推奨買い目と一致するオッズが見つかりませんでした")
                                        st.caption(f"AI推奨: {', '.join(all_ai_combos[:5])}...")
                                    
                                    # Show all parsed odds
                                    st.write("**解析済みオッズ:**")
                                    for combo, odds in sorted(parsed_odds.items(), key=lambda x: x[1])[:10]:
                                        st.write(f"  {combo}: {odds}倍")


                       # Optional: JSON debug (collapsed)
                       with st.expander("🔍 フォーメーションデータ (JSON)"):
                           st.json(structured_bets)
                   
                # --- NEW: Player Detail Analysis (Old Wing Restoration) ---
                st.markdown("---")
                st.markdown("### 🔎 出場選手 詳細分析 (Old Wing)")
                
                # Check if we have valid player names
                p_names = df_race['選手名'].unique().tolist() if '選手名' in df_race.columns else []
                
                if p_names:
                    # Use unique key per race
                    selected_player = st.selectbox("選手を選択して詳細データを分析:", p_names, key=f"p_select_{selected_label}")
                    
                    if selected_player:
                        # Find the row
                        p_row = df_race[df_race['選手名'] == selected_player].iloc[0]
                        
                        # Call Logic
                        with st.spinner(f"{selected_player} 選手の詳細データを分析中..."):
                            # Pass 'meta' instead of undefined 'meta_info'
                            detail_res = logic_v2.analyze_player_detailed_stats(p_row, meta)
                        
                        if detail_res and 'basic' in detail_res:
                            # Display Labels
                            labels = detail_res.get('labels', [])
                            if labels:
                                st.success(" ".join(labels))
                            else:
                                st.info("特筆すべき属性（魔人・サバイバー等）は検出されませんでした")
                            
                            # Display Stats Columns
                            c1, c2, c3 = st.columns(3)
                            
                            # 1. Basic (Last 1 year)
                            bs = detail_res['basic']
                            with c1:
                                st.markdown("#### 📊 直近1年成績")
                                st.metric("勝率", f"{bs['win_rate']:.1f}%")
                                st.metric("2連対率", f"{bs['ren2_rate']:.1f}%")
                                st.metric("3連対率", f"{bs['ren3_rate']:.1f}%")
                                st.caption(f"対象: 直近 {bs['total']} 走")

                            # 2. Condition Match
                            cs = detail_res.get('condition', {})
                            with c2:
                                st.markdown("#### 🔧 同条件成績")
                                if cs:
                                    st.metric("勝率", f"{cs['win_rate']:.1f}%", delta=f"{cs['win_rate']-bs['win_rate']:.1f}%")
                                    st.metric("2連対率", f"{cs['ren2_rate']:.1f}%", delta=f"{cs['ren2_rate']-bs['ren2_rate']:.1f}%")
                                    st.metric("3連対率", f"{cs['ren3_rate']:.1f}%", delta=f"{cs['ren3_rate']-bs['ren3_rate']:.1f}%")
                                    match_names = ",".join(cs.get('match_conditions', []))
                                    st.caption(f"今回のライン長・位置と同じ時の成績 ({cs['match_count']}走)")
                                else:
                                    st.warning("該当データなし")

                            # 3. Bank Match
                            bks = detail_res.get('bank', {})
                            with c3:
                                st.markdown("#### 🏰 類似バンク成績")
                                if bks:
                                    st.metric("勝率", f"{bks['win_rate']:.1f}%", delta=f"{bks['win_rate']-bs['win_rate']:.1f}%")
                                    st.metric("2連対率", f"{bks['ren2_rate']:.1f}%", delta=f"{bks['ren2_rate']-bs['ren2_rate']:.1f}%")
                                    st.metric("3連対率", f"{bks['ren3_rate']:.1f}%", delta=f"{bks['ren3_rate']-bs['ren3_rate']:.1f}%")
                                    match_names = ",".join(bks.get('match_banks', []))
                                    st.caption(f"類似: {match_names} など ({bks['total']}走)")
                                else:
                                    st.warning("該当データなし")
                                    
                            # 4. History Table (User Request)
                            if 'history_df' in detail_res:
                                st.markdown("#### 📜 過去走データ (ライン構成・着順)")
                                h_df = detail_res['history_df']
                                
                                # Select & Rename Columns for Display
                                target_cols = [
                                    ('日付', '日付'), 
                                    ('競輪場', '場'), 
                                    ('レース番号', 'R'), 
                                    ('着順', '着'), 
                                    ('決まり手', '決'), 
                                    ('line_length', 'ライン長'), 
                                    ('line_pos', '位置'),
                                    ('ポジション', '位置'), # Fallback
                                    ('lines_parsed', '並び') # Optional
                                ]
                                
                                disp_cols = []
                                rename_dict = {}
                                
                                for col, label in target_cols:
                                    if col in h_df.columns:
                                        if label not in rename_dict.values(): # Avoid duplicate columns
                                            disp_cols.append(col)
                                            rename_dict[col] = label
                                
                                if disp_cols:
                                    st.dataframe(
                                        h_df[disp_cols].rename(columns=rename_dict).head(50), 
                                        use_container_width=True,
                                        height=300
                                    )
                                else:
                                    st.info("表示可能な履歴カラムが見つかりませんでした")
                        else:
                            st.error("詳細データの取得に失敗しました (過去データ不足の可能性)")
                else:
                    st.warning("選手名データが見つかりません")
                


                # --- 新ロジック解説エリア (Local) ---
                st.markdown("---")
                st.caption("📝 **データで熱狂をつかめ! ロジック解説**")
                
                def check_reason(df, keyword):
                    if 'bonus_reasons' not in df.columns: return []
                    return df[df['bonus_reasons'].astype(str).str.contains(keyword, na=False)]['車番'].tolist()

                # 1. 💣 魔人 (千切れ)
                majin_cars = check_reason(df_scored, "魔人")
                if majin_cars:
                    cars_str = ",".join(map(str, majin_cars))
                    st.info(f"**💣 ラインクラッシャー (車番: {cars_str})**\n\n"
                            "自分が逃げ残った（2着以内）のに、後ろの選手を置き去り（4着以下）にする傾向があります。\n"
                            "→ **スジ違い（ライン不成立）**を狙うチャンスです。")

                # 2. 🗡️ 差し逆転 (ズブズブ)
                zubu_cars = check_reason(df_scored, "差逆")
                if zubu_cars:
                    cars_str = ",".join(map(str, zubu_cars))
                    st.info(f"**🗡️ 差し脚鋭い (車番: {cars_str})**\n\n"
                            "番手から1着を取りつつ、前の選手も2着に残す（ズブズブ）傾向があります。\n"
                            "→ **ラインワンツー（差し目）**を厚めに。")

                # 3. 🏃 サバイバー
                survivor_cars = check_reason(df_scored, "サバイバー")
                if survivor_cars:
                    cars_str = ",".join(map(str, survivor_cars))
                    st.info(f"**🏃 サバイバー (車番: {cars_str})**\n\n"
                            "前の選手がボロ負けしても、自分だけ3着以内に突っ込んでくる「穴選手」です。\n"
                            "→ ラインが弱くても、混戦になれば**ヒモ（3着）**や頭で浮上します。")

                # 4. 🇪🇺 欧州穴 (事故要員)
                euro_cars = check_reason(df_scored, "欧州")
                if euro_cars:
                    cars_str = ",".join(map(str, euro_cars))
                    st.info(f"**🇪🇺 事故要員 (車番: {cars_str})**\n\n"
                            "4・6・8番車などの人気薄・単騎構成で3着以内に来る「一発屋」です。\n"
                            "→ 高配当狙いなら、3連単の3着に入れておく価値があります。")

                # 5. 💒 相性良
                love_cars = check_reason(df_scored, "相性良")
                if love_cars:
                    cars_str = ",".join(map(str, love_cars))
                    st.success(f"**💒 バンク相性抜群 (車番: {cars_str})**\n\n"
                               "この競輪場での連対率（2着以内率）が50%を超えています。\n"
                               "→ 理屈抜きで買い目に入れるべき「得意バンク」の選手です。")


                # --- AI Reporter Section (API) ---
                st.markdown("---")
                try:
                    if st.button("🤖 AI戦況レポート作成 (記者モード)"):
                        if not api_key_input:
                            st.error("サイドバーでGemini APIキーを設定してください。")
                        else:
                            with st.spinner("敏腕記者が記事を執筆中..."):
                                # Create Meta - include parsed lines for accurate reporting
                                meta_info = {
                                    'place': place_name, 
                                    'race_class': race_class, 
                                    'race_num': df_scored['レース番号'].iloc[0] if 'レース番号' in df_scored.columns else '?',
                                    'lines_parsed': meta.get('lines_parsed', '')  # Add correct line configuration
                                }
                                
                                # Generate Special Bonus Strategy as Main Strategy
                                strategy_data = logic_v2.generate_bonus_strategy(df_scored, score_col='final_score')
                                strategy_data['title'] = "特注予想 (ボーナス重視)"
                                
                                report_text = logic_v2.generate_race_report(df_scored, meta_info, strategy_data, api_key_input)
                                
                                # Save Prediction History (As Main)
                                try:
                                    # Normalize Date
                                    d_raw = meta.get('date', datetime.now().strftime('%Y年%m月%d日'))
                                    d_clean = d_raw.replace('-', '年').replace('/', '年')
                                    if '年' not in d_clean:
                                         try: d_clean = datetime.strptime(d_raw, "%Y-%m-%d").strftime("%Y年%m月%d日")
                                         except: pass
                                    
                                    # Normalize Race Num (Fix 1RR bug)
                                    cur_r_num = str(meta_info.get('race_num', '1R')).replace('R', '')
                                    cur_r_num = f"{cur_r_num}R"
                                    
                                    pred_data = {
                                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        "place": place_name,
                                        "race_num": cur_r_num,
                                        "date": d_clean,
                                        "prediction_text": report_text,
                                        "tickets": strategy_data.get('tickets', []),
                                        "structured_bets": strategy_data.get('structured_bets', []),  # Added for stats calc
                                        "strategy_title": strategy_data.get('title', 'Special'),
                                        "strategy_type": "special_bonus", # Mark as special
                                        "ai_indices": df_scored[['車番', 'final_score', '選手名', 'ai_tag']].to_dict('records') if 'final_score' in df_scored.columns else []
                                    }
                                    if db_utils.save_prediction(pred_data):
                                        st.toast("✅ 特注予想を履歴に保存しました")
                                except Exception as e_save:
                                    print(f"History Save Error: {e_save}")
                                    

                                        
                                except Exception as e_save:
                                    print(f"History Save Error: {e_save}")
                                
                                st.subheader("📰 本日の予想コラム")
                                st.markdown(report_text, unsafe_allow_html=True)
                                st.info("※ この記事はデータ分析に基づきAIが自動生成しています。")

                except Exception as e:
                    st.error(f"記者レポート生成エラー (API): {e}")
                
                # --- AI Chat Assistant Section ---
                st.markdown("---")
                with st.expander("💬 AIアシスタントに質問する（このレースについて）"):
                    # Initialize chat history in session state (per race)
                    chat_key = f"chat_history_{selected_label}"
                    if chat_key not in st.session_state:
                        st.session_state[chat_key] = []
                    
                    # Build context data for the AI
                    # (Only recalculate when needed, not on every rerun)
                    context_key = f"chat_context_{selected_label}"
                    if context_key not in st.session_state:
                        # Build player text
                        p_lines = []
                        for _, row in df_scored.iterrows():
                            c = row['車番']
                            n = row['選手名']
                            score = row.get('final_score', row.get('競走得点', 0))
                            reasons = str(row.get('bonus_reasons', ''))
                            p_lines.append(f"{c}番: {n} (AIスコア:{score:.1f}) {reasons}")
                        
                        players_text = "\n".join(p_lines)
                        
                        # Strategy info
                        strat_title = strategy_data.get('title', '標準')
                        tickets = ", ".join(strategy_data.get('tickets', []))
                        strategy_info = f"戦略: {strat_title}\n推奨: {tickets}"
                        
                        # Logic info (detected flags)
                        logic_parts = []
                        if majin_cars: logic_parts.append(f"魔人系: {majin_cars}")
                        if survivor_cars: logic_parts.append(f"サバイバー: {survivor_cars}")
                        if euro_cars: logic_parts.append(f"欧州穴: {euro_cars}")
                        if love_cars: logic_parts.append(f"相性良: {love_cars}")
                        logic_info = "\n".join(logic_parts) if logic_parts else "特筆すべきフラグなし"
                        
                        st.session_state[context_key] = {
                            'place': place_name,
                            'race_num': df_scored['レース番号'].iloc[0] if 'レース番号' in df_scored.columns else '?',
                            'players_text': players_text,
                            'strategy_info': strategy_info,
                            'logic_info': logic_info
                        }
                    
                    context_data = st.session_state[context_key]
                    
                    # Display chat history
                    for message in st.session_state[chat_key]:
                        with st.chat_message(message["role"]):
                            st.markdown(message["content"])
                    
                    # Chat input
                    if prompt := st.chat_input("質問を入力（例: 1番は買える？ このレースは荒れそう？）"):
                        # Add user message
                        st.session_state[chat_key].append({"role": "user", "content": prompt})
                        with st.chat_message("user"):
                            st.markdown(prompt)
                        
                        # Generate AI response
                        if not api_key_input:
                            response = "APIキーが設定されていません。サイドバーから設定してください。"
                        else:
                            with st.spinner("AI考え中..."):
                                response = logic_v2.generate_chat_response(
                                    st.session_state[chat_key],
                                    context_data,
                                    api_key_input
                                )
                        
                        # Add assistant message
                        st.session_state[chat_key].append({"role": "assistant", "content": response})
                        with st.chat_message("assistant"):
                            st.markdown(response)
                        
                        
                        # Note: Removed st.rerun() to prevent article from disappearing
                        
            except Exception as e:
                st.error(f"計算エラー: {e}")



# ------------------------------------------
# Tab 2: 選手・ロジック検索 (新機能)
# ------------------------------------------
with tab2:
    st.header("🕵️ 選手解析データベース")
    st.caption("「Final Analysis」ファイルを検索し、条件に合致する危険な選手（魔人・サバイバー等）を抽出します。")
    
    # Place Selection (To load specific file)
    l_place = st.selectbox("競輪場データ選択", ["松阪", "防府", "前橋", "平塚", "小倉"], index=0)
    
    if st.button("データ読み込み"):
        # Load stats
        df_logic = engine.get_final_analysis_stats(l_place)
        if df_logic is not None:
            st.session_state['logic_df'] = df_logic
            st.success(f"{l_place}のデータをロードしました ({len(df_logic)}名)")
        else:
            st.error(f"{l_place}のデータファイルが見つかりません (logic_data/{l_place}/..._final.xlsx)")

    if 'logic_df' in st.session_state:
        df_l = st.session_state['logic_df']
        
        # Filters
        c1, c2, c3, c4 = st.columns(4)
        f_chigire = c1.checkbox("� 魔人 (千切れ)", value=False)
        f_hyena = c2.checkbox("🏃 サバイバー", value=False)
        f_predator = c3.checkbox("🗡️ 差し逆転", value=False)
        f_europe = c4.checkbox("🇪🇺 欧州穴", value=False)
        
        # Filter Logic
        filtered_df = df_l.copy()
        
        if f_chigire and 'A_千切れフラグ' in filtered_df.columns:
            # Need Mean? The final analysis file usually has aggregated stats OR raw race rows.
            # If Raw Rows: Group by Player and calc mean.
            # If the file is "Analysis Final", it might be raw history.
            # Let's check columns. Step 867 showed: 'A_千切れフラグ', '選手名' etc.
            # And many rows per player. So we need to aggregate.
            
            # Aggregate Mode
            st.info("集計中... (初回は時間がかかります)")
            
            # Group by Player Name
            # We want players who have HIGH rate.
            # Calculate means for all flags first?
            g_cols = ['選手名', 'A_千切れフラグ', 'B_ハイエナフラグ', 'A_差し逆転フラグ', 'B_穴適性_欧州', '競走得点']
            # Ensure cols exist
            g_cols = [c for c in g_cols if c in filtered_df.columns]
            
            grp = filtered_df.groupby('選手名')[g_cols].mean(numeric_only=True).reset_index()
            
            # Apply Filters
            if f_chigire:
                grp = grp[grp['A_千切れフラグ'] >= 0.30]
            if f_hyena:
                grp = grp[grp['B_ハイエナフラグ'] >= 0.15]
            if f_predator:
                grp = grp[grp['A_差し逆転フラグ'] >= 0.20]
            if f_europe:
                grp = grp[grp['B_穴適性_欧州'] >= 0.10]
            
            st.dataframe(grp.sort_values('競走得点', ascending=False))
        else:
            # Show Raw if no filter or aggregate all
            st.dataframe(df_l.head(100))
            st.caption("※フィルタ未適用時は先頭100行のみ表示")

# ------------------------------------------
# Tab 3: エンジン状態
# ------------------------------------------
with tab3:
    st.header("データ分析 (Polars Engine Status)")
    if engine.current_place:
        st.success(f"現在ロード中のデータ: {engine.current_place}")
        # db keys are 'ALL' or PlaceName
        st.json({k: str(type(v)) for k, v in engine.db.items()})
    else:
        st.info("出走表をアップロードすると、対応する競輪場の統計データがロードされます。")

with tab4:
    st.header("設定")
    if st.button("DB接続テスト"):
        try:
            conn = sqlite3.connect(db_utils.DB_PATH)
            st.success("OK: 接続成功")
            conn.close()
        except Exception as e:
            st.error(f"接続失敗: {e}")

# ------------------------------------------
# Tab 5: AI的中履歴
# ------------------------------------------
with tab5:
    st.header("📜 AI的中履歴 & 回収率分析")
    
    if st.button("🔄 履歴と分析を更新", use_container_width=False, key="refresh_hist"):
        st.rerun()

    # --- History Loading Optimization ---
    st.markdown("---")
    col_filter, _ = st.columns([2, 3])
    with col_filter:
        hist_mode = st.radio(
            "表示対象期間",
            ["運用開始後 (12/31以降)", "昨日・今日のみ", "全期間 (重い)"],
            horizontal=True,
            index=0,
            help="「運用開始後」は2025年12月31日以降の全データを表示します。"
        )

    # Filter Logic
    from datetime import datetime, timedelta
    now = datetime.now()
    
    cutoff = None
    
    if "12/31以降" in hist_mode:
        # Fixed Start Date: 2025-12-31
        cutoff = datetime(2025, 12, 31)
    elif "昨日" in hist_mode:
        # Rolling: Today + Yesterday
        cutoff = now - timedelta(days=2)
        cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Optimized Load
    history = db_utils.load_prediction_history(min_date=cutoff)
        
    if cutoff:
        d_label = cutoff.strftime('%Y/%m/%d')
        st.caption(f"{d_label} 以降の履歴を表示中: {len(history)}件")
    else:
        st.caption(f"全期間の履歴を表示中: {len(history)}件")


    if not history:
        st.info("予測履歴がありません。「出走表・AI予測」タブでAI予測を作成するとここに保存されます。")
    else:
        # Analyze data
        with st.spinner("レース結果と照合中..."):
            try:
                # df_res: Race Level, df_tickets: Ticket Level
                df_res, stats, df_tickets = logic_v2.analyze_prediction_history(history)
            except Exception as e:
                st.error(f"分析エラー: {e}")
                df_res = pd.DataFrame()
                stats = {}
                df_tickets = pd.DataFrame()
            
        if df_res.empty:
            st.warning("履歴データの解析に失敗しました（またはデータなし）。")
        else:
            # --- Check for Missing Results ---
            if 'hit_detail' in df_res.columns:
                missing_res = df_res[df_res['hit_detail'] == "結果未着"]
                if not missing_res.empty:
                    st.warning(f"⚠️ 結果未取得のレースが {len(missing_res)} 件あります。回収率に反映するには結果を取得してください。")
                    
                    if st.button(f"対象の{len(missing_res)}レースの結果を取得・更新する"):
                        # Group by Place and Date to minimize requests
                        # missing_res has 'place', 'date'
                        targets = missing_res[['place', 'date']].drop_duplicates()
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        total_tasks = len(targets)
                        success_cnt = 0
                        
                        for idx, (i, row) in enumerate(targets.iterrows()):
                            p_name = row['place']
                            d_str = row['date']
                            
                            # Standardize Date for Scraper (YYYY-MM-DD)
                            try:
                                if "年" in d_str:
                                    dt_obj = datetime.strptime(d_str, "%Y年%m月%d日")
                                    search_date = dt_obj.strftime("%Y-%m-%d")
                                else:
                                    search_date = d_str
                            except:
                                search_date = d_str
                            
                            status_text.text(f"取得中 ({idx+1}/{total_tasks}): {p_name} {d_str}")
                            
                            try:
                                # Fetch data (This gets the WHOLE day, which includes results if available)
                                # date format in history is usually YYYY-MM-DD. scraper expects YYYY-MM-DD.
                                scraped = scraper.fetch_race_data(p_name, search_date, search_date)
                                
                                if scraped:
                                    # Save to DB
                                    count, msg = db_utils.save_race_data(scraped, overwrite=True)
                                    if count > 0:
                                        success_cnt += 1
                                else:
                                    st.warning(f"{p_name} {d_str}: データが取得できませんでした")
                            except Exception as e:
                                st.error(f"Error {p_name} {d_str}: {e}")
                                
                            progress_bar.progress((idx + 1) / total_tasks)
                            
                        status_text.text("完了！")
                        if success_cnt > 0:
                            st.success(f"{success_cnt} 開催日のデータを更新しました！")
                            st.rerun()
                        else:
                            st.error("データの更新に失敗しました（結果がまだ公開されていない可能性があります）")

            # 1. Filters (Top Level)
            st.markdown("### 🔍 フィルタリング")
            all_places = ["全場"] + sorted(df_res['place'].unique().tolist())
            col_f1, col_f2 = st.columns([1, 3])
            sel_place = col_f1.selectbox("開催場を選択", all_places)
            
            # --- Filter Data ---
            df_disp = df_res.copy()
            
            # Ensure race_id exists for filtering sync
            if 'race_id' not in df_disp.columns and not df_disp.empty:
                df_disp['race_id'] = df_disp.apply(
                    lambda x: f"{x.get('place')}_{x.get('date')}_{str(x.get('race_num','')).replace('R','')+'R'}", 
                    axis=1
                )
            
            df_tick_disp = df_tickets.copy()
            
            # --- Feature Engineering for Filters ---
            if not df_disp.empty:
                # 1. Calculate Bonus Value from AI Indices (Tag Parsing)
                def calc_bonus_from_indices(row):
                    try:
                        indices = row.get('ai_indices', [])
                        if isinstance(indices, str):
                            import json
                            indices = json.loads(indices)
                        
                        max_bonus = 0.0
                        
                        for item in indices:
                            # Parse tags from ai_tag string or bonus_reasons columns if available
                            # Batch saves 'ai_tag'.
                            tags = str(item.get('ai_tag', '')) 
                            # Naive parsing of known tags
                            b = 0.0
                            if '[地元]' in tags: b += 3.0
                            if 'No.1' in tags: b += 2.0 * tags.count('No.1') # Each No.1 is +2
                            if '直線' in tags or '傾斜' in tags: b += 2.0 # Bank specs
                            if 'ライン' in tags and '3' in tags: b += 1.0 # Line bonus (approx)
                            
                            if b > max_bonus: max_bonus = b
                            
                        return max_bonus
                    except:
                        return 0.0

                if 'bonus_value' not in df_disp.columns:
                    df_disp['bonus_value'] = df_disp.apply(calc_bonus_from_indices, axis=1)
                else:
                    # If it exists but is all 0, recalc
                    if df_disp['bonus_value'].sum() == 0:
                         df_disp['bonus_value'] = df_disp.apply(calc_bonus_from_indices, axis=1)
                
                df_disp['bonus_value'] = pd.to_numeric(df_disp['bonus_value'], errors='coerce').fillna(0.0)
                
                # 2. Calculate Score Gap
                def get_score_gap(row):
                    try:
                        indices = row.get('ai_indices', [])
                        if isinstance(indices, str): # Handle stringified JSON
                            import json
                            indices = json.loads(indices)
                        if not isinstance(indices, list) or len(indices) < 2:
                            return 0.0
                        
                        # Extract scores, handling potential malformed data
                        scores = []
                        for x in indices:
                            try: scores.append(float(x.get('final_score', 0)))
                            except: pass
                        
                        if len(scores) < 2: return 0.0
                        
                        scores.sort(reverse=True)
                        return scores[0] - scores[1]
                    except:
                        return 0.0

                df_disp['score_gap'] = df_disp.apply(get_score_gap, axis=1)
            else:
                 df_disp['score_gap'] = 0.0

            # --- Extended Filters ---
            col_f3, col_f4 = st.columns(2)
            min_bonus = col_f3.number_input("最小ボーナス加点 (0~20)", min_value=0, max_value=20, value=0, step=1)
            min_gap = col_f4.number_input("最小指数差 (大差:7~)", min_value=0.0, max_value=30.0, value=0.0, step=1.0)
            
            # Apply Filters
            if min_bonus > 0:
                df_disp = df_disp[df_disp['bonus_value'] >= min_bonus]
            
            if min_gap > 0:
                df_disp = df_disp[df_disp['score_gap'] >= min_gap]
            
            # Sync Tickets Filter
            if 'strategy_type' in df_tick_disp.columns:
                df_tick_disp = df_tick_disp[df_tick_disp['strategy_type'] == 'special_bonus']
            
            # Logic to sync tick_disp with filtered races is tricky because ticket DF lacks race-level metrics like Gap/Bonus easily
            # But we can filter by race_id or index matching?
            # df_disp has indices from df_res. 
            # Easiest: Filter tick_disp to include only races present in filtered df_disp
            if not df_disp.empty:
                valid_rids = df_disp['race_id'].unique() if 'race_id' in df_disp.columns else []
                # Fallback if race_id missing
                if 'race_id' in df_tick_disp.columns:
                     df_tick_disp = df_tick_disp[df_tick_disp['race_id'].isin(valid_rids)]
            else:
                df_tick_disp = df_tick_disp.iloc[0:0] # Empty it
            
            if sel_place != "全場":
                df_disp = df_disp[df_disp['place'] == sel_place]
                if not df_tick_disp.empty:
                    df_tick_disp = df_tick_disp[df_tick_disp['place'] == sel_place]
            
            # --- Recalculate Stats for Display ---
            # Include only Settled Races for statistics (User Request)
            df_calc = df_disp[~df_disp['hit_detail'].isin(["結果未着", "結果待/無"])]
            
            if not df_calc.empty:
                disp_invest = df_calc['investment'].sum()
                disp_return = df_calc['benefit'].sum()
                disp_bal = disp_return - disp_invest
                disp_rec = (disp_return / disp_invest * 100) if disp_invest > 0 else 0.0
                disp_hit = df_calc['is_hit'].sum()
                
                disp_hit_rate = (disp_hit / len(df_calc) * 100) if len(df_calc) > 0 else 0.0
                disp_cnt = len(df_calc)
            else:
                disp_invest = 0
                disp_return = 0
                disp_bal = 0
                disp_rec = 0.0
                disp_hit = 0
                disp_hit_rate = 0.0
                disp_cnt = 0


            st.divider()

            # 2. Summary Metrics (Filtered)
            st.markdown(f"### 📊 成績サマリー ({sel_place})")
            
            # Counts
            total_cnt = len(df_disp)
            settled_cnt = len(df_calc)
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("総予想レース", f"{total_cnt}R", f"うち確定 {settled_cnt}R")
            
            if settled_cnt > 0:
                m2.metric("的中数 (率)", f"{disp_hit}R ({disp_hit_rate:.1f}%)")
                rec_delta = disp_rec - 100.0
                m3.metric("回収率", f"{disp_rec:.1f}%", delta=f"{rec_delta:.1f}%")
                m4.metric("総収支", f"{int(disp_bal) if pd.notna(disp_bal) else 0:,}円", delta=f"{int(disp_bal) if pd.notna(disp_bal) else 0:,}円")
            else:
                 m2.metric("的中数 (率)", "-")
                 m3.metric("回収率", "-")
                 m4.metric("総収支", "-")
                 st.caption("※ 結果が確定したレースがありません（すべて結果未着または除外）")
                 m4.metric("総収支", "-")
                 st.caption("※ 結果が確定したレースがありません（すべて結果未着または除外）")
            
            st.divider()
            
            # 3. Ticket Type Stats (Filtered)
            st.markdown(f"### 🎯 券種別成績 ({sel_place})")
            
            if not df_tick_disp.empty:
                # Group by Ticket Type
                grp = df_tick_disp.groupby('type').agg({
                    'invest': 'sum',
                    'return': 'sum',
                    'is_hit': 'sum',
                    'type': 'count'
                }).rename(columns={'type':'ticket_count'})
                
                # Calc Rates
                grp['balance'] = grp['return'] - grp['invest']
                grp['recovery_rate'] = (grp['return'] / grp['invest'] * 100).fillna(0.0)
                grp['hit_rate'] = (grp['is_hit'] / grp['ticket_count'] * 100).fillna(0.0)
                
                # Format for Display
                grp = grp.reset_index()
                # Sort by invest desc
                disp_grp = grp.sort_values('invest', ascending=False)
                
                # Rename Columns
                disp_grp.columns = ['券種', '購入額', '払戻額', '的中数', '総数', '収支', '回収率', '的中率']
                
                # Reorder
                disp_grp = disp_grp[['券種', '総数', '的中数', '的中率', '購入額', '払戻額', '収支', '回収率']]
                
                # Format
                st.dataframe(
                    disp_grp.style.format({
                        '的中率': "{:.1f}%",
                        '購入額': "{:,.0f}",
                        '払戻額': "{:,.0f}",
                        '収支': "{:,.0f}",
                        '回収率': "{:.1f}%"
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("券種別データがありません")
            
            st.divider()

            # 4. Line Strategy Analysis
            st.markdown(f"### 🚴 ライン予想傾向分析 ({sel_place})")
            st.caption("AIの予想とレース結果が「ライン（スジ）」決着だったか、「別線（スジ違い）」決着だったかを分析します。")
            
            # Simple Filter based on UI selection
            if history:
                # 1. Base Filter by Place
                if sel_place == "全場":
                    base_history = history
                else:
                    base_history = [h for h in history if h.get('place') == sel_place]
                
                # 2. Sync with df_disp filters (Bonus/Gap)
                # Valid filtered IDs
                if not df_disp.empty and 'race_id' in df_disp.columns:
                    valid_ids = set(df_disp['race_id'].unique())
                    
                    # Need to ensure history items also have race_id to match
                    target_history = []
                    for h in base_history:
                        # Construct ID if missing
                        rid = h.get('race_id')
                        if not rid:
                            r_num = str(h.get('race_num','')).replace('R','') + 'R'
                            rid = f"{h.get('place')}_{h.get('date')}_{r_num}"
                        
                        if rid in valid_ids:
                            target_history.append(h)
                else:
                    # If df_disp is empty (filtered to 0), target_history is empty
                    target_history = []
            
            if target_history:
                # st.write(f"DEBUG: Analyzed {len(target_history)} races") # Debug
                with st.spinner("ライン傾向を分析中..."):
                    l_stats = logic_v2.analyze_line_strategy_bias(target_history)
                
                if l_stats and l_stats.get('total_races', 0) > 0:
                    tot = l_stats['total_races']
                    
                    # Columns
                    la1, la2 = st.columns(2)
                    
                    with la1:
                        st.subheader("🤖 AIの予想傾向")
                        ai_same = l_stats['ai_same_line']
                        ai_sep = l_stats['ai_separate']
                        ai_same_r = ai_same / tot * 100
                        ai_sep_r = ai_sep / tot * 100
                        st.write(f"**ライン決着予想**: {ai_same}R ({ai_same_r:.1f}%)")
                        st.write(f"**別線(スジ違)予想**: {ai_sep}R ({ai_sep_r:.1f}%)")
                        st.progress(ai_same_r / 100)
                        
                    with la2:
                        st.subheader("🏁 実際のレース結果")
                        res_same = l_stats['res_same_line']
                        res_sep = l_stats['res_separate']
                        res_same_r = res_same / tot * 100
                        res_sep_r = res_sep / tot * 100
                        st.write(f"**ライン決着**: {res_same}R ({res_same_r:.1f}%)")
                        st.write(f"**別線(スジ違)**: {res_sep}R ({res_sep_r:.1f}%)")
                        st.progress(res_same_r / 100)
                    
                    st.write("---")
                    # Match Analysis
                    # AI Same hit rate / AI Sep hit rate (Accuracy of tendency)
                    # Note: l_stats['ai_same_line_hit'] means AI voted Same AND Result was Same.
                    
                    acc_same = l_stats['ai_same_line_hit'] / ai_same * 100 if ai_same > 0 else 0.0
                    acc_sep = l_stats['ai_separate_hit'] / ai_sep * 100 if ai_sep > 0 else 0.0
                    
                    st.markdown(f"**💡 AIの狙い方の精度**")
                    st.write(f"- ライン(スジ)を狙った時の的中(傾向一致)率: **{acc_same:.1f}%** (予想数 {ai_same}R中 {l_stats['ai_same_line_hit']}R正解)")
                    st.write(f"- 別線(スジ違)を狙った時の的中(傾向一致)率: **{acc_sep:.1f}%** (予想数 {ai_sep}R中 {l_stats['ai_separate_hit']}R正解)")
                    
                else:
                    st.info("ライン情報がデータベースに見つからないため分析できません。")
            else:
                st.info("表示対象のデータがありません")

                # End of Ticket Stats Block

            st.divider()

            # 5. AI Score Analysis
            st.markdown(f"### 🤖 AI評価点分析 ({sel_place})")
            st.caption("AI評価点1位・2位の選手の成績、および評価点差（自信度）と勝率の関係などを分析します。")

            if target_history:
                with st.spinner("AIスコア傾向を分析中..."):
                    s_stats = logic_v2.analyze_ai_score_performance(target_history)
                
                if s_stats and s_stats.get('total_races', 0) > 0:
                    stot = s_stats['total_races']
                    
                    # 1. Basic Stats Comparison (1st vs 2nd)
                    st.markdown("**🥇 AI評価点 1位 vs 2位 成績比較**")
                    sc1, sc2, sc3 = st.columns(3)
                    
                    # 1st Pick Stats
                    w1 = s_stats['ai_top_win']
                    r1 = s_stats['ai_top_rentai']
                    f1 = s_stats['ai_top_fukusho']
                    
                    # 2nd Pick Stats
                    w2 = s_stats.get('ai_2nd_win', 0)
                    r2 = s_stats.get('ai_2nd_rentai', 0)
                    f2 = s_stats.get('ai_2nd_fukusho', 0)

                    sc1.metric("1着回数 (勝率)", 
                               f"1位: {w1} ({w1/stot*100:.1f}%)", 
                               f"2位: {w2} ({w2/stot*100:.1f}%)", delta_color="off")
                    sc2.metric("2連対回数 (連対率)", 
                               f"1位: {r1} ({r1/stot*100:.1f}%)", 
                               f"2位: {r2} ({r2/stot*100:.1f}%)", delta_color="off")
                    sc3.metric("3連対回数 (複勝率)", 
                               f"1位: {f1} ({f1/stot*100:.1f}%)", 
                               f"2位: {f2} ({f2/stot*100:.1f}%)", delta_color="off")
                    
                    st.write("---")
                    
                    # 2. Relation with Competition Score Rank
                    st.markdown("**📊 AI1位の選手は「競走得点」で何位か？**")
                    c_dist = s_stats['comp_rank_dist']
                    c_keys = sorted(c_dist.keys())
                    c_data = {"競走得点順位": [f"{k}位" for k in c_keys], "回数": [c_dist[k] for k in c_keys]}
                    st.bar_chart(pd.DataFrame(c_data).set_index("競走得点順位"))
                    
                    st.write("---")
                    
                    # 3. Score Gap Analysis (Detailed)
                    st.markdown("**📏 1位と2位の評価点差による勝率・連対率の変化**")
                    st.caption("評価点差が大きいほどAIが「1位と2位の実力差がある」と判断しています。")
                    
                    gap_data = s_stats['gap_data']
                    if gap_data:
                        df_gap = pd.DataFrame(gap_data)
                        # Binning
                        bins = [-100, 0, 2.0, 5.0, 8.0, 1000]
                        labels = ["逆転(2位>1位)", "僅差(0-2点)", "小差(2-5点)", "中差(5-8点)", "大差(8点以上)"]
                        
                        df_gap['bin'] = pd.cut(df_gap['gap'], bins=bins, labels=labels)
                        
                        # Aggregation
                        gap_grp = df_gap.groupby('bin', observed=False).agg({
                            'is_win': ['count', 'sum'],
                            'is_rentai': 'sum',
                            'is_fukusho': 'sum'
                        })
                        gap_grp.columns = ['レース数', '1着回数', '2連対回数', '3連対回数']
                        
                        # Rate Calc
                        gap_grp['勝率'] = (gap_grp['1着回数'] / gap_grp['レース数'] * 100).fillna(0)
                        gap_grp['連対率'] = (gap_grp['2連対回数'] / gap_grp['レース数'] * 100).fillna(0)
                        gap_grp['3連対率'] = (gap_grp['3連対回数'] / gap_grp['レース数'] * 100).fillna(0)
                        
                        # Display Table with Clean Columns
                        show_cols = ['レース数', '1着回数', '勝率', '2連対回数', '連対率', '3連対回数', '3連対率']
                        st.dataframe(
                            gap_grp[show_cols].style.format({
                                '勝率': '{:.1f}%',
                                '連対率': '{:.1f}%',
                                '3連対率': '{:.1f}%'
                            }),
                            use_container_width=True
                        )
                        st.info("💡 **見方**: 「中差(5-8点)」や「大差(8点以上)」の時に勝率が高ければ、AIの自信度が信頼できることを示します。")
                    
                    st.write("---")
                    
                    # 4. Bonus Player Analysis
                    st.markdown("**🎁 AI加点(ボーナス)最大選手の成績**")
                    st.caption("「地元」「逃げNo.1」「バンク相性」などの加点が最も大きい選手の成績です。")
                    
                    bonus_data = s_stats.get('bonus_data', [])
                    if bonus_data:
                        df_bonus = pd.DataFrame(bonus_data)
                        btot = len(df_bonus)
                        
                        # Overall Stats
                        b_win = df_bonus['is_win'].sum()
                        b_rentai = df_bonus['is_rentai'].sum()
                        b_fukusho = df_bonus['is_fukusho'].sum()
                        
                        bc1, bc2, bc3 = st.columns(3)
                        bc1.metric("勝率", f"{b_win/btot*100:.1f}%", f"{b_win}/{btot}R")
                        bc2.metric("連対率", f"{b_rentai/btot*100:.1f}%", f"{b_rentai}/{btot}R")
                        bc3.metric("3連対率", f"{b_fukusho/btot*100:.1f}%", f"{b_fukusho}/{btot}R")
                        
                        # By Comp Rank
                        st.markdown("**競走得点順位別（加点1位選手）**")
                        rank_grp = df_bonus.groupby('comp_rank').agg({
                            'is_win': ['count', 'mean'],
                            'is_rentai': 'mean',
                            'is_fukusho': 'mean'
                        })
                        rank_grp.columns = ['回数', '勝率', '連対率', '3連対率']
                        rank_grp['勝率'] *= 100
                        rank_grp['連対率'] *= 100
                        rank_grp['3連対率'] *= 100
                        rank_grp.index = [f"{i}位" for i in rank_grp.index]
                        st.dataframe(
                            rank_grp.style.format({'勝率': '{:.1f}%', '連対率': '{:.1f}%', '3連対率': '{:.1f}%'}),
                            use_container_width=True
                        )
                        
                        # By Bonus Amount (Breakpoints)
                        st.markdown("**加点量による断層**")
                        bins_b = [0, 5.0, 7.0, 9.0, 100]
                        labels_b = ["〜5点", "5〜7点", "7〜9点", "9点以上"]
                        df_bonus['bonus_bin'] = pd.cut(df_bonus['bonus'], bins=bins_b, labels=labels_b)
                        
                        bonus_grp = df_bonus.groupby('bonus_bin', observed=False).agg({
                            'is_win': ['count', 'mean'],
                            'is_rentai': 'mean',
                            'is_fukusho': 'mean'
                        })
                        bonus_grp.columns = ['回数', '勝率', '連対率', '3連対率']
                        bonus_grp['勝率'] *= 100
                        bonus_grp['連対率'] *= 100
                        bonus_grp['3連対率'] *= 100
                        st.dataframe(
                            bonus_grp.style.format({'勝率': '{:.1f}%', '連対率': '{:.1f}%', '3連対率': '{:.1f}%'}),
                            use_container_width=True
                        )
                        st.info("💡 **断層**: 「7点以上」で勝率・連対率が跳ね上がる傾向があれば、AI加点を信頼できるサインです。")
                else:
                    st.info("詳細な競走得点データが見つからないため分析できません。")
            else:
                st.info("データなし")
                
            st.divider()
            
            # 4. History Table
            st.markdown(f"### 📜 詳細履歴 ({sel_place})")
            
            # Reset df_disp to ignore strategy filters (Gap/Bonus) for the main history table
            # But keep the Place filter
            df_disp = df_res.copy()
            if sel_place != "全場":
                df_disp = df_disp[df_disp['place'] == sel_place]
                
            # --- Badge Calculation Logic ---
            def get_ai_badges(row):
                badges = []
                
                # Parse AI Indices
                try:
                    ai_indices = row.get('ai_indices', [])
                    if not ai_indices: return ""
                    
                    # Ensure properly parsed score
                    parsed = []
                    for item in ai_indices:
                        try: s = float(item.get('final_score', 0))
                        except: s = 0.0
                        c = int(item.get('車番', 0))
                        parsed.append({'c': c, 's': s})
                    parsed.sort(key=lambda x: x['s'], reverse=True)
                    
                    if not parsed: return ""
                    
                    # Gap Badge
                    ai_top = parsed[0]
                    ai_2nd = parsed[1] if len(parsed)>1 else None
                    if ai_2nd:
                        gap = ai_top['s'] - ai_2nd['s']
                        if gap >= 8.0:
                            badges.append(f"🔥大差({gap:.1f})")
                        elif gap >= 5.0:
                            badges.append(f"✨中差({gap:.1f})")
                    
                    # Upset Badge (Need Comp Rank info from somewhere)
                    # We might not have comp rank in `df_disp` easily unless we join or kept it.
                    # As a proxy, let's assume if gap is NEGATIVE or very small it's risky? No.
                    # The user specifically asked for "Competitor Score Rank 1 vs AI Top".
                    # `df_res` (which `df_disp` comes from) comes from `analyze_prediction_history`.
                    # logic_v2.analyze_prediction_history loads DB info including `comp_ranks`.
                    # But does it save it to the DataFrame? 
                    # Let's check logic_v2.py or just use what we have.
                    # If we can't get Comp Rank easily here without DB hit, we might skip Upset Badge for now
                    # OR we can try to infer from 'strategy_title' if it says "穴"?
                    
                    # However, to be accurate for "AI top is not Comp top", we need Comp Top Car.
                    # For now, let's just show Gap Confidence which is high value.
                    
                except:
                    pass
                return " ".join(badges)
            
            # If `ai_indices` is not in df_disp columns, we can't do it.
            # `analyze_prediction_history` returns `df_res` constructed from history dicts.
            # It usually flattens specific cols. We might need to ensure `ai_indices` is kept.
            # Actually, `df_disp` IS `df_res`. logic_v2 constructs it.
            # If `ai_indices` isn't in columns, we must rely on what we have.
            # Let's assume it's NOT there by default.
            
            # Re-map from `history` list based on index or ID?
            # `df_disp` rows correspond to `history` items processed.
            # `history` is available here (`history` variable).
            # Let's map badges via finding matching history item.
            
            id_map = {}
            
            # Pre-load DB connection for bonus calculation
            import sqlite3
            conn_badge = sqlite3.connect(db_utils.DB_PATH)
            
            for h in history:
                rid = h.get('race_id') # or construct
                if not rid:
                    r_num = str(h.get('race_num','')).replace('R','') + 'R'
                    rid = f"{h.get('place')}_{h.get('date')}_{r_num}"
                
                # Calc Badges
                b = []
                ai_indices = h.get('ai_indices', [])
                if ai_indices:
                    parsed = []
                    for item in ai_indices:
                        try: s = float(item.get('final_score', 0))
                        except: s = 0.0
                        c = int(item.get('車番', 0))
                        parsed.append({'c': c, 's': s})
                    parsed.sort(key=lambda x: x['s'], reverse=True)
                    
                    if len(parsed) >= 2:
                        gap = parsed[0]['s'] - parsed[1]['s']
                        if gap >= 8.0: b.append(f"🔥大差{gap:.1f}")
                        elif gap >= 5.0: b.append(f"✨中差{gap:.1f}")
                
                # Bonus Badge - Calculate max bonus for this race
                try:
                    query_race = "SELECT * FROM race_result WHERE race_id = ?"
                    df_race = pd.read_sql(query_race, conn_badge, params=[rid])
                    if not df_race.empty:
                        df_scored = logic_v2.calculate_ai_score(df_race)
                        if 'base_score' in df_scored.columns and 'ai_score' in df_scored.columns:
                            df_scored['bonus'] = df_scored['ai_score'] - df_scored['base_score']
                            max_bonus = df_scored['bonus'].max()
                            # Check for NaN
                            if pd.notna(max_bonus):
                                if max_bonus >= 9.0:
                                    b.append(f"🎁加点{int(max_bonus)}")
                                elif max_bonus >= 7.0:
                                    b.append(f"⭐加点{int(max_bonus)}")
                except:
                    pass
                
                id_map[rid] = " ".join(b)
            
            conn_badge.close()
                
            # Ensure race_id exists
            if 'race_id' not in df_disp.columns:
                # Reconstruct
                # Assuming date format is consistent or available
                # Logic_v2 usually preserves 'race_id' if in input. 
                # If missing, we construct: place + date + race_num
                # Warning: date format might differ (YYYY-MM-DD vs YYYY年...)
                # But 'id_map' keys were constructed using h.get('date').
                # df_disp['date'] comes from h.get('date').
                # So they should match.
                df_disp['race_id'] = df_disp.apply(
                    lambda x: f"{x.get('place')}_{x.get('date')}_{str(x.get('race_num','')).replace('R','')+'R'}", 
                    axis=1
                )

            df_disp['ai_memo'] = df_disp['race_id'].map(id_map).fillna("")

            df_disp['race_str'] = df_disp['place'] + " " + df_disp['race_num'].astype(str)
            
            def fmt_tickets(t):
                if isinstance(t, list): 
                    return "\n".join(t)
                return str(t)
            
            df_disp['tickets_str'] = df_disp['tickets'].apply(fmt_tickets)
            
            disp_cols = [
                'timestamp', # HIDDEN
                'date', 'race_str', 'strategy_title', 'ai_memo', 'tickets_str', 
                'result_top3', 'hit_detail', 'benefit', 'balance'
            ]
            
            final_cols = [c for c in disp_cols if c in df_disp.columns]
            
            column_config = {
                'date': "日付",
                'race_str': "レース",
                'strategy_title': "戦略名",
                'ai_memo': st.column_config.TextColumn("AI自信度", width="small", help="点数差5点以上で表示"),
                'tickets_str': st.column_config.TextColumn("推奨買い目", width="large"),
                'result_top3': st.column_config.TextColumn("結果 (1-2-3)", width="small"),
                'hit_detail': st.column_config.TextColumn("的中判定", width="medium"),
                'benefit': st.column_config.NumberColumn("払戻金", format="%d円"),
                'balance': st.column_config.NumberColumn("収支", format="%d円"),
            }
            
            try:
                if 'timestamp' in df_disp.columns:
                    disp_df_final = df_disp[final_cols].sort_values('timestamp', ascending=False)
                    # Optional: drop timestamp from view if desired, but keeping it is useful for exact time
                    # disp_df_final = disp_df_final.drop(columns=['timestamp']) 
                else:
                    disp_df_final = df_disp[final_cols]
                
                st.dataframe(
                    disp_df_final,
                    column_config=column_config,
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )
            except Exception as e:
                st.error(f"テーブル表示エラー: {e}")
                st.dataframe(df_disp) # Fallback raw

    # ==========================================
    # AI Chat Assistant (Tab1 Bottom)
    # ==========================================
    # Only show if in Analysis Mode (Tab1)
    # Actually, tab1 scope ended way up. We need to check indentation or placement.
    # The previous code was Tab 3 (History). 
    # Chat should be available for the ACTIVE race analysis.
    # So it should be part of the race analysis flow, likely after the "Today's Prediction Column".
    # Or as a global floating element? No, Streamlit doesn't float easily.
    # We will place it at the very bottom of the main area (outside tabs probably, or specifically in Tab1).
    
# Moving back to indent level 0 to ensure it's outside Tab 3 loop
# But we need access to 'df_scored', 'strategy_data' etc. which are local to Tab 1.
# So we must insert this INSIDE Tab 1, after the reporter section.
# The previous view was lines 1400+, which is inside Tab 3.
# I need to target the end of Tab 1.
# Tab 1 ends around line 1250-1300? 
# Let's abort this replace and view Tab 1 end first.
