import streamlit as st
import sqlite3, time
import src.db_helper as db_helper
from src.config import load_user_config

st.set_page_config(
    # Title and icon for the browser's tab bar:
    page_title='Wiki ETL Monitor',
    page_icon='📈',
    # layout='wide',
)
st.title('Data Pipeline Monitor: Wikipedia Recent Events')
"""
By Callum Friedman | [GitHub](https://github.com/cdfrdmn/etl-wikipedia-updates)
"""
""
""
"""
## Pipeline Metrics
"""

config = load_user_config()

# Capture the initial state
if 'last_state' not in st.session_state:
    with sqlite3.connect(config.db_path) as conn:
        newest_row_id = conn.execute(f"SELECT MAX(id) FROM {config.db_table_name}").fetchone()[0] or 0
        db_disk_usage = db_helper.get_db_disk_size(config.db_path)
    # (id, timestamp, last_velocity, total_rows, disk_size)
    st.session_state.last_state = (newest_row_id, time.time(), 0, 0, db_disk_usage)

velocity_placeholder        = st.empty()
latency_placeholder         = st.empty()
total_rows_placeholder      = st.empty()
db_disk_usage_placeholder   = st.empty()

while True:
    with sqlite3.connect(config.db_path) as conn:
        newest_row_id = conn.execute(f"SELECT MAX(id) FROM {config.db_table_name}").fetchone()[0] or 0
        current_total_rows = conn.execute(f'SELECT MAX(id) - MIN(id) + 1 FROM {config.db_table_name}').fetchone()[0] or 0
    
    # Retrieve previous state
    prev_newest_row_id, prev_time, prev_velocity, prev_total_rows, prev_db_disk_size = st.session_state.last_state

    # Calculate current state
    current_time = time.time()
    delta_rows = newest_row_id - prev_newest_row_id
    delta_time = current_time - prev_time
    velocity = int((delta_rows / delta_time) * 60) if delta_time > 0 else 0
    db_disk_usage = db_helper.get_db_disk_size(config.db_path)

    with velocity_placeholder.container():
        st.metric(
            label='Ingestion Rate (Events / minute)',
            value=velocity,
            delta=f'{velocity - prev_velocity} EPM'
        )

    with total_rows_placeholder.container():
        st.metric(
            label='Total Events in Database',
            value=f'{current_total_rows}',
            delta=f'{current_total_rows - prev_total_rows}'
        )
    
    with db_disk_usage_placeholder.container():
        st.metric(
            label='Database Disk Usage',
            value=f'{db_disk_usage:.2f} MiB',
            delta=f'{(db_disk_usage - prev_db_disk_size):.2f} MiB'
        )

    # Save state & Wait
    st.session_state.last_state = (newest_row_id, current_time, velocity, current_total_rows, db_disk_usage)
    time.sleep(5)
