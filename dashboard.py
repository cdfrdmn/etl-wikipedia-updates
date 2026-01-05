import streamlit as st
import pandas as pd
import sqlite3
import datetime
import time

from config import load_user_config

def main():

    config = load_user_config()

    st.title('Live Wikipedia Edit Monitor')

    # 1. Initialize 'initial_max_id' only once
    if 'initial_max_id' not in st.session_state:
        conn = sqlite3.connect(config.db_path)
        # Get the highest ID currently in the table
        res = conn.execute(f"SELECT MAX(id) FROM {config.db_table_name}").fetchone()
        # If DB is empty, start at 0
        st.session_state.initial_max_id = res[0] if res[0] is not None else 0
        st.session_state.start_time = datetime.datetime.now().strftime("%H:%M:%S")
        conn.close()

    metric_placeholder = st.empty()

    while True:
        conn = sqlite3.connect(config.db_path)
        # Get current total count AND the new max ID
        res = conn.execute(f"SELECT COUNT(*), MAX(id) FROM {config.db_table_name}").fetchone()
        conn.close()
        
        current_count = res[0] if res[0] else 0
        current_max_id = res[1] if res[1] else 0
        
        # 2. Calculate added rows based on ID growth, not row count
        added_since_start = max(0, current_max_id - st.session_state.initial_max_id)
        
        metric_placeholder.metric(
            label=f"New Edits (Since page load {st.session_state.start_time})", 
            value=f"{added_since_start:,}",
            # delta=f"{current_count:,} currently in DB"
        )
        
        time.sleep(1)

if __name__ == "__main__":
    main()
