import streamlit as st
import pandas as pd
import sqlite3
import datetime
import time

def get_data():
    conn = sqlite3.connect('wikipedia-events.db') # maybe import this connection from the main script after database initialisation?
    
    # Use parse_dates so Pandas handles the ISO-8601 strings for us
    df = pd.read_sql("SELECT event_timestamp FROM wiki_events", conn, parse_dates=['event_timestamp']) # import config stuff like in main script, maybe move the config import to its own script!
    conn.close()
    return df

def main():
    start_time = datetime.datetime.now()
    st.title('Live Wikipedia Edit Monitor')

    # Placeholders
    metric_placeholder = st.empty()

    while True:
        df = get_data()
        
        if not df.empty:
            # 1. Update the Metric (Real-time count)
            total_edits = len(df)
            metric_placeholder.metric(f'Total Edits Processed since {start_time}', total_edits)
        
        time.sleep(1) # Re-run loop every second

if __name__ == "__main__":
    main()