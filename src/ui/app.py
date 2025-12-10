import os
import streamlit as st
import polars as pl
import pandas as pd
from src.core.state_manager import StateManager
from src.core.ingestion import IngestionEngine
from src.core.ai_fixer import AIFixer
from src.ui.components import render_header, render_status_metrics, render_history_sidebar

# --- 1. SESSION STATE INITIALIZATION ---
if "state_manager" not in st.session_state:
    st.session_state.state_manager = StateManager("session_default")
if "processing_complete" not in st.session_state:
    st.session_state.processing_complete = False
if "manual_edit_mode" not in st.session_state:
    st.session_state.manual_edit_mode = False
if "split_stats" not in st.session_state:
    st.session_state.split_stats = None
if "ai_preview_data" not in st.session_state:
    st.session_state.ai_preview_data = None 

manager = st.session_state.state_manager
engine = IngestionEngine()

def main():
    st.set_page_config(page_title="Local AI Data Cleaner", layout="wide")
    render_header()
    render_history_sidebar(manager.get_history())

    # --- 2. FILE UPLOAD ---
    uploaded_file = st.file_uploader("Upload Data", type=["csv", "xlsx", "txt"])
    
    if not uploaded_file:
        # Reset state if file is removed
        st.session_state.processing_complete = False
        st.session_state.split_stats = None
        st.session_state.ai_preview_data = None
        return

    # Path Setup
    temp_path = os.path.join(manager.upload_dir, uploaded_file.name)
    clean_path = os.path.join(manager.processed_dir, "clean_temp.csv")
    quarantine_path = os.path.join(manager.quarantine_dir, "bad_rows.csv")
    pq_path = os.path.join(manager.processed_dir, "master.parquet")

    # Save File (Only once)
    if not os.path.exists(temp_path):
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        manager.log_event("Phase 1: Ingestion", "Upload", f"File '{uploaded_file.name}' received.")

    # --- 3. ANALYSIS & CHECKS (Run once per file) ---
    if st.session_state.split_stats is None:
        with st.spinner("Running System Checks..."):
            
            # Check 1: Access
            access = engine.validate_file_access(temp_path)
            if not access["valid"]:
                st.error(access["error"])
                return
            manager.log_event("Phase 1: Ingestion", "Access Check", "✅ File permissions verified.")

            # Check 2: Encoding
            enc_res = engine.detect_encoding(temp_path)
            encoding = enc_res["encoding"]
            manager.log_event("Phase 1: Ingestion", "Encoding", f"✅ Detected {encoding} ({enc_res['confidence']*100:.0f}%)")

            # Check 3: Structure (Stream Split)
            stats = engine.stream_clean_and_split(temp_path, clean_path, quarantine_path, encoding)
            st.session_state.split_stats = stats
            
            if stats["bad"] > 0:
                manager.log_event("Phase 1: Ingestion", "Structure", f"❌ Ragged Rows: {stats['bad']} found.")
            else:
                manager.log_event("Phase 1: Ingestion", "Structure", "✅ Structure perfectly aligned.")

            # Force re-render to update sidebar immediately
            st.rerun()

    # Load Stats from State
    stats = st.session_state.split_stats
    
    # --- 4. DASHBOARD ---
    # Determine Health Label
    health_status = "Healthy" if stats["bad"] == 0 else "Needs Repair"
    render_status_metrics(True, "UTF-8", health_status)

    # --- 5. LOGIC FOR HEALTHY VS BROKEN ---
    if stats["bad"] == 0 or st.session_state.processing_complete:
        st.success("✅ File is clean and loaded into Master Parquet.")
        if os.path.exists(pq_path):
            df = pl.read_parquet(pq_path)
            st.dataframe(df.head(50), use_container_width=True)
    else:
        st.error(f"⚠️ {stats['bad']} rows failed structure check.")
        
        tab_view, tab_repair = st.tabs(["🔍 Inspector", "🛠️ Repair Station"])
        
        # TAB 1: INSPECTOR
        with tab_view:
            col1, col2 = st.columns(2)
            with col1: 
                st.caption("✅ Good Data Sample")
                st.dataframe(pl.read_csv(clean_path, n_rows=50), use_container_width=True)
            with col2:
                st.caption("❌ Bad Data (Raw Text)")
                with open(quarantine_path, "r") as f:
                    st.text("".join(f.readlines()[:20]))

        # TAB 2: REPAIR STATION
        with tab_repair:
            
            # === SUB-STATE: REVIEW MODE ===
            if st.session_state.ai_preview_data:
                st.markdown("### 🤖 Review AI Proposals")
                st.info("The AI has analyzed the bad rows. Please review the proposed fixes before applying.")
                
                # Create a comparison dataframe for display
                preview_list = st.session_state.ai_preview_data
                df_preview = pd.DataFrame(preview_list)
                
                # Show Editable Dataframe (User can tweak AI's fix if it's slightly off)
                edited_preview = st.data_editor(
                    df_preview, 
                    column_config={
                        "original": st.column_config.TextColumn("Original Raw Row", disabled=True),
                        "fixed": st.column_config.TextColumn("AI Proposed Fix (Editable)", width="large")
                    },
                    use_container_width=True,
                    num_rows="fixed"
                )

                col_accept, col_discard = st.columns([1, 4])
                
                # ACCEPT BUTTON
                if col_accept.button("✅ Accept & Apply"):
                    # 1. Save the APPROVED fixes (from edited_preview)
                    with open(clean_path, 'r') as f: header = f.readline().strip().split(',')
                    
                    fixed_file = os.path.join(manager.processed_dir, "fixed_ai.csv")
                    
                    # --- FIX: ADD encoding='utf-8' HERE TO PREVENT CHARMAP ERROR ---
                    with open(fixed_file, 'w', encoding='utf-8', newline='') as f:
                        f.write(",".join(header) + "\n")
                        for index, row in edited_preview.iterrows():
                            # If AI failed on a row, skip it or write raw
                            if "Error:" in row['fixed']: continue
                            f.write(row['fixed'] + "\n")
                    
                    # 2. Merge
                    res = engine.merge_and_finalize(clean_path, fixed_file, pq_path)
                    if res["success"]:
                        manager.log_event("Phase 1: Ingestion", "AI Fix", f"User approved AI repairs for {len(edited_preview)} rows.")
                        st.session_state.processing_complete = True
                        st.session_state.ai_preview_data = None # Clear state
                        st.rerun()

                # DISCARD BUTTON
                if col_discard.button("❌ Discard"):
                    st.session_state.ai_preview_data = None
                    st.rerun()

            # === SUB-STATE: ACTION BUTTONS ===
            else:
                st.write("Choose a strategy to handle the bad rows:")
                
                c1, c2, c3 = st.columns(3)
                
                # --- STRATEGY A: DROP ---
                if c3.button("🗑️ Drop Bad Rows", use_container_width=True):
                    engine.merge_and_finalize(clean_path, "", pq_path)
                    manager.log_event("Phase 1: Ingestion", "Action", "Dropped quarantine rows.")
                    st.session_state.processing_complete = True
                    st.rerun()

                # --- STRATEGY B: AI FIX ---
                if c1.button("🤖 AI Auto-Fix", use_container_width=True):
                    # Use the specific model name
                    fixer = AIFixer(model_name="phi4-mini-reasoning:3.8b")
                    
                    with st.status("AI Agent analyzing rows...", expanded=True) as status:
                        # Read Header
                        with open(clean_path, 'r') as f: header_list = f.readline().strip().split(',')
                        header_str = ",".join(header_list).strip()
                        
                        # Read Bad Rows
                        with open(quarantine_path, 'r') as f: bad_rows = f.readlines()
                        
                        preview_data = []
                        for i, row in enumerate(bad_rows):
                            clean_row = row.strip()
                            if not clean_row: continue
                            
                            # --- CRITICAL FIX: SKIP HEADER IF PRESENT IN BAD ROWS ---
                            if clean_row == header_str:
                                continue
                            # --------------------------------------------------------

                            status.update(label=f"Reasoning on row {i+1}...")
                            
                            # Get fix
                            fixed_str = fixer.fix_ragged_row(header_list, clean_row)
                            
                            # Add to preview list
                            preview_data.append({
                                "original": clean_row,
                                "fixed": fixed_str
                            })
                        
                        st.session_state.ai_preview_data = preview_data
                        status.update(label="Analysis Complete! Waiting for review.", state="complete")
                        st.rerun()

                # --- STRATEGY C: MANUAL EDIT ---
                if c2.button("🖊️ Manual Edit", use_container_width=True):
                    st.session_state.manual_edit_mode = True
                
                # The Editor (Only shows if mode is True)
                if st.session_state.manual_edit_mode:
                    st.divider()
                    st.markdown("### ✏️ Editor Workbench")
                    
                    # Load for editor
                    with open(quarantine_path, 'r') as f: lines = f.readlines()
                    grid_data = [{"raw_text": line.strip()} for line in lines if line.strip()]
                    
                    edited_df = st.data_editor(grid_data, num_rows="dynamic", use_container_width=True)
                    
                    col_save, col_cancel = st.columns([1, 4])
                    if col_save.button("Save & Merge"):
                        fixed_file = os.path.join(manager.processed_dir, "fixed_manual.csv")
                        with open(clean_path, 'r') as f: header = f.readline().strip()
                        
                        # --- FIX: ADD encoding='utf-8' HERE ALSO ---
                        with open(fixed_file, 'w', encoding='utf-8', newline='') as f:
                            f.write(header + "\n")
                            for item in edited_df:
                                f.write(item["raw_text"] + "\n")
                                
                        engine.merge_and_finalize(clean_path, fixed_file, pq_path)
                        manager.log_event("Phase 1: Ingestion", "Manual Fix", "User manually corrected rows.")
                        st.session_state.processing_complete = True
                        st.session_state.manual_edit_mode = False
                        st.rerun()
                        
                    if col_cancel.button("Cancel"):
                        st.session_state.manual_edit_mode = False
                        st.rerun()

if __name__ == "__main__":
    main()