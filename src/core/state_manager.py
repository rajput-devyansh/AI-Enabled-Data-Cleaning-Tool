import os
import shutil
from datetime import datetime
from typing import List, Dict, Any

class StateManager:
    """
    Manages the session state, file paths, and action history.
    Acts as the Single Source of Truth for the UI.
    """
    def __init__(self, session_id: str):
        self.session_id = session_id
        # Define base paths
        self.base_dir = os.path.join("data", ".cache", session_id)
        self.upload_dir = os.path.join(self.base_dir, "uploads")
        self.processed_dir = os.path.join(self.base_dir, "processed")
        self.quarantine_dir = os.path.join(self.base_dir, "quarantine")
        
        # Initialize directories
        self._init_dirs()

        # state variables
        self.current_stage = "Ingestion"
        self.active_file_path: str = None # Path to current Parquet file
        self.quarantine_path: str = None  # Path to current Bad Rows CSV
        
        # The History Tree structure
        self.history: Dict[str, Dict[str, Any]] = {
            "Phase 1: Ingestion": {"status": "active", "logs": []},
            "Phase 2: Structure": {"status": "pending", "logs": []},
            "Phase 3: Cleaning": {"status": "pending", "logs": []}
        }

    def _init_dirs(self):
        """Creates necessary directories for the session."""
        for path in [self.upload_dir, self.processed_dir, self.quarantine_dir]:
            os.makedirs(path, exist_ok=True)

    def log_event(self, phase: str, category: str, message: str):
        """Adds an event to the history tree."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = {"time": timestamp, "category": category, "message": message}
        
        if phase in self.history:
            self.history[phase]["logs"].append(entry)
            # Auto-update status if needed
            if self.history[phase]["status"] == "pending":
                self.history[phase]["status"] = "active"

    def get_history(self):
        return self.history

    def clear_session(self):
        """Cleans up temporary files."""
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
            self._init_dirs()