import os
import streamlit as st
import pandas as pd
import sqlite3
import datetime
import time

def get_data():
    conn = sqlite3.connect(os.getenv('DB_PATH'))
    
    # Use parse_dates so Pandas handles the ISO-8601 strings for us
    df = pd.read_sql(f"SELECT event_timestamp FROM {os.getenv('DB_TABLE_NAME')}", conn, parse_dates=['event_timestamp'])
    conn.close()
    return df

def main():
    st.title('Live Wikipedia Edit Monitor')

    # 1. Initialize 'initial_max_id' only once
    if 'initial_max_id' not in st.session_state:
        conn = sqlite3.connect(os.getenv('DB_PATH'))
        # Get the highest ID currently in the table
        res = conn.execute(f"SELECT MAX(id) FROM {os.getenv('DB_TABLE_NAME')}").fetchone()
        # If DB is empty, start at 0
        st.session_state.initial_max_id = res[0] if res[0] is not None else 0
        st.session_state.start_time = datetime.datetime.now().strftime("%H:%M:%S")
        conn.close()

    metric_placeholder = st.empty()

    while True:
        conn = sqlite3.connect(os.getenv('DB_PATH'))
        # Get current total count AND the new max ID
        res = conn.execute(f"SELECT COUNT(*), MAX(id) FROM {os.getenv('DB_TABLE_NAME')}").fetchone()
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
