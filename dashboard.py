import streamlit as st
import sqlite3, time
from config import load_user_config

config = load_user_config()

conn = sqlite3.connect(config.db_path, check_same_thread=False)
st.title("Data Pipeline Monitor: Wikipedia Recent Events")
velocity_placeholder = st.empty()
latency_placeholder = st.empty()

# Initial state
if 'last_state' not in st.session_state:
    with sqlite3.connect(config.db_path) as conn:
        start_id = conn.execute(f"SELECT MAX(id) FROM {config.db_table_name}").fetchone()[0] or 0
    # (id, timestamp, last_velocity)
    st.session_state.last_state = (start_id, time.time(), 0)

while True:
    with sqlite3.connect(config.db_path) as conn:
        current_id = conn.execute(f"SELECT MAX(id) FROM {config.db_table_name}").fetchone()[0] or 0
    current_time = time.time()
    
    # Retrieve previous states
    prev_id, prev_time, prev_velocity = st.session_state.last_state

    # Calculate current states
    delta_events = current_id - prev_id
    delta_time = current_time - prev_time
    current_velocity = int((delta_events / delta_time) * 60) if delta_time > 0 else 0
    delta_velocity = current_velocity - prev_velocity

    latest_event_ts = conn.execute(f"SELECT unixepoch(event_timestamp) FROM {config.db_table_name} ORDER BY id DESC LIMIT 1").fetchone()[0]
    latency = time.time() - latest_event_ts

    with velocity_placeholder.container():
        st.metric(
            label='Ingestion rate (Events / minute)',
            value=current_velocity,
            delta=f'{delta_velocity} EPM'
        )

    # Save state & Wait
    st.session_state.last_state = (current_id, current_time, current_velocity)
    time.sleep(3)