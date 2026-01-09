import streamlit as st
import sqlite3
import pandas as pd
import time

from config import load_user_config

def get_pulse_data(db_path: str, db_table_name: str, time_period_minutes: int, time_granularity_seconds: int):
    """Fetches edit counts bucketed by second for the last 5 minutes."""
    conn = sqlite3.connect(db_path)
    
    # Query logic: 
    # 1. Filter row to all in time period
    # 2. Group by granularity in event_timestamp to create a time-series
    # Return two columns: a time bin start, and the number of events within that bin
    query = f'''
        SELECT 
            datetime((unixepoch(event_timestamp) / {time_granularity_seconds}) * {time_granularity_seconds}, 'unixepoch') as time, 
            COUNT(*) as edit_count
        FROM {db_table_name} 
        WHERE event_timestamp > datetime('now', '-{time_period_minutes} minutes')
        GROUP BY time 
        ORDER BY time ASC
    '''
    # Cast this SQL table into a dataframe
    df = pd.read_sql(query, conn)
    # df['time'] = pd.to_datetime(df['time'])
    print(df)
    conn.close()
    return df

def main():

    # Load the config
    config = load_user_config()

    # Set the browser tab title
    st.set_page_config(page_title='Wiki Live Monitor')
    # Page title
    st.title('Live Wikipedia Edit Monitor')
    # H2 header
    pulse_period_minuntes: int = 5
    st.header(f'Pulse (Last {pulse_period_minuntes} Minutes)')

    # Create a persistent placeholder for the chart, otherwise each refresh would append a new chart underneath
    chart_placeholder = st.empty()

    while True:
        # Get the data
        df = get_pulse_data(db_path=config.db_path, db_table_name=config.db_table_name, time_period_minutes=pulse_period_minuntes, time_granularity_seconds=1)

        # Update the chart
        if not df.empty:
            chart_placeholder.area_chart(
                df.set_index('time'),
                x_label='Time (UTC)',
                y_label='Total Edits',
                width='stretch' # stretch to fill placeholder
            )
        else:
            # If the data is empty, don't show a chart
            chart_placeholder.info("Waiting for data...")

        # Time to wait between refreshing (>= database time commit interval)
        time.sleep(5)

if __name__ == "__main__":
    main()
