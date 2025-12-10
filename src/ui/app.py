import os
import streamlit as st
import polars as pl
from src.core.state_manager import StateManager
from src.core.ingestion import IngestionEngine
from src.ui.components import render_header, render_status_metrics, render_history_sidebar

# Initialize State Manager (Singleton-ish pattern for Streamlit)
if "state_manager" not in st.session_state:
    st.session_state.state_manager = StateManager("session_default")

manager = st.session_state.state_manager
engine = IngestionEngine()

def main():
    st.set_page_config(page_title="Local AI Data Cleaner", layout="wide")
    render_header()
    
    # Render Sidebar
    render_history_sidebar(manager.get_history())
    
    # --- Zone 1: File Upload ---
    uploaded_file = st.file_uploader("Upload your data (CSV, TSV, Excel)", type=["csv", "xlsx", "txt"])
    
    if uploaded_file:
        # 1. Save to temp location (Simulate ingestion)
        temp_path = os.path.join(manager.upload_dir, uploaded_file.name)
        
        # Only save if not already processed to avoid reload loops
        if not os.path.exists(temp_path):
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            manager.log_event("Phase 1: Ingestion", "Upload", f"File {uploaded_file.name} saved.")

        # --- Zone 2: Integrity Checks ---
        with st.spinner("Running Integrity & Structural Analysis..."):
            
            # A. Access Check
            access_result = engine.validate_file_access(temp_path)
            if not access_result["valid"]:
                st.error(f"File Access Error: {access_result['error']}")
                return

            # B. Encoding Check
            encoding_res = engine.detect_encoding(temp_path)
            encoding = encoding_res["encoding"]
            confidence = encoding_res["confidence"]
            
            if confidence < 0.90:
                st.warning(f"Low confidence ({confidence:.2f}) for encoding: {encoding}. Please verify.")
                # Future: Add manual encoding selector here
            
            # C. Stream Split & Clean
            clean_path = os.path.join(manager.processed_dir, "clean_temp.csv")
            quarantine_path = os.path.join(manager.quarantine_dir, "bad_rows.csv")
            
            # Only run split if we haven't done it yet for this file
            if "split_stats" not in st.session_state:
                stats = engine.stream_clean_and_split(temp_path, clean_path, quarantine_path, encoding)
                st.session_state.split_stats = stats
                manager.log_event("Phase 1: Ingestion", "Structure", f"Split: {stats['good']} Good, {stats['bad']} Bad.")
            
            stats = st.session_state.split_stats

        # --- Zone 3: Dashboard ---
        
        # Determine Health Status
        health_status = "Healthy"
        if stats["bad"] > 0:
            health_status = "Needs Repair"
            
        render_status_metrics(
            access=True, 
            encoding=f"{encoding} ({int(confidence*100)}%)", 
            structural_health=health_status
        )
        
        # --- Zone 4: The Quarantine Workbench ---
        
        if stats["bad"] == 0:
            st.success("✅ File Structure is Perfect! Converting to Parquet...")
            # Trigger Parquet Conversion
            pq_path = os.path.join(manager.processed_dir, "master.parquet")
            if engine.convert_to_parquet(clean_path, pq_path):
                st.info(f"Ready for Phase 2. Loaded {stats['good']} rows.")
                # Preview
                df = pl.read_parquet(pq_path)
                st.dataframe(df.head(50), use_container_width=True)
                
        else:
            st.error(f"⚠️ Structural Issues Detected: {stats['bad']} rows found with mismatching columns.")
            
            tab_good, tab_bad, tab_repair = st.tabs(["✅ Healthy Data", "🗑️ Quarantine Zone", "🛠️ Repair Station"])
            
            with tab_good:
                st.caption("These rows matched the header structure.")
                # Load a small sample of the clean CSV since Parquet conversion waits for clean state
                df_good = pl.read_csv(clean_path, n_rows=100)
                st.dataframe(df_good)
                
            with tab_bad:
                st.caption("These rows failed structural validation (Wrong column count).")
                # Read raw bad rows
                with open(quarantine_path, "r") as f:
                    bad_raw = f.readlines()
                st.text("".join(bad_raw[:20])) # Show first 20 lines raw
                
            with tab_repair:
                st.markdown("### Repair Strategies")
                col_r1, col_r2, col_r3 = st.columns(3)
                
                with col_r1:
                    st.button("🤖 AI Auto-Fix (Phi-4)", help="Send bad rows to AI to deduce correct structure.")
                with col_r2:
                    st.button("🖊️ Manual Edit", help="Open editor grid.")
                with col_r3:
                    if st.button("🗑️ Drop Bad Rows"):
                        st.warning("Dropping rows and finalizing file...")
                        # Logic to ignore bad rows and proceed would go here
                        
if __name__ == "__main__":
    main()