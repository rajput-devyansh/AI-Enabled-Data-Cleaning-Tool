import streamlit as st

def render_header():
    """Renders the application header."""
    st.markdown("""
    <h1 style='text-align: center; color: #4CAF50;'>
        🛠️ AI Local Data Cleaner
    </h1>
    <p style='text-align: center; color: #666;'>
        Phase 1: Ingestion & Structural Integrity
    </p>
    <hr>
    """, unsafe_allow_html=True)

def render_status_metrics(access: bool, encoding: str, structural_health: str):
    """
    Renders the 'Traffic Light' status row.
    """
    col1, col2, col3 = st.columns(3)
    
    with col1:
        color = "green" if access else "red"
        st.markdown(f"**File Access**")
        st.markdown(f":{color}[{'✅ Granted' if access else '❌ Denied'}]")
        
    with col2:
        st.markdown(f"**Encoding**")
        st.markdown(f":blue[{encoding}]")
        
    with col3:
        color = "green" if structural_health == "Healthy" else "orange" if structural_health == "Repaired" else "red"
        st.markdown(f"**Structure**")
        st.markdown(f":{color}[{structural_health}]")
    
    st.divider()

def render_history_sidebar(history_data: dict):
    """
    Renders the State Management History Tree in the Sidebar.
    """
    st.sidebar.title("🗂️ Project History")
    
    # Iterate through Phases
    for phase_name, phase_data in history_data.items():
        # Determine icon based on status
        status_icon = "🟢" if phase_data["status"] == "completed" else "🟡" if phase_data["status"] == "active" else "⚪"
        
        with st.sidebar.expander(f"{status_icon} {phase_name}", expanded=(phase_data["status"] == "active")):
            if not phase_data["logs"]:
                st.write("*No actions yet.*")
            else:
                for log in phase_data["logs"]:
                    st.markdown(f"`{log['time']}` **{log['category']}**")
                    st.caption(f"{log['message']}")