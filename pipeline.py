from sseclient import SSEClient
import sqlite3
import json
import time
from config import load_user_config

def pipeline(db_connection: sqlite3.Connection, db_table_name: str, stream_url: str, user_agent: str, db_max_events: int) -> None:
    """Orchestrate the end-end pipeline for real-time SSE message ingestion into an SQLite database. 

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        stream_url (str): SSE endpoint URL.
        user_agent (str): Identity string for the stream request header.

    Returns:
        int: Number of rows added to the database table since execution start.
    """
    # Instantiate the iterator
    event_feed = sse_stream_iterator(stream_url, user_agent)

    rows_added_to_db = 0
    last_commit_time = time.time()
    commit_interval_seconds = 2

    # Iterate over each yielded message
    for event in event_feed:
        # Call the save function and check if it was successful
        if save_event_to_db(db_connection, db_table_name, event):
            rows_added_to_db+=1

            # Use a time-based commit schedule to limit database write calls
            if time.time() - last_commit_time >= commit_interval_seconds:
                db_connection.commit()
                last_commit_time = time.time()
                print(f'Events committed to database: {rows_added_to_db}')
                current_row_count: int = db_connection.execute(f"SELECT COUNT(*) FROM {db_table_name}").fetchone()[0]

                # Perform threshold based database cleanup, allowing a 10% buffer 
                if current_row_count >= int(1.1*db_max_events):
                    db_connection.execute(f'''
                        DELETE FROM {db_table_name} 
                        WHERE id < MAX(0, (SELECT MAX(id) FROM {db_table_name}) - {db_max_events})
                    ''')
                    db_connection.commit()
                    current_row_count = db_max_events
                    print("--- CLEANUP PERFORMED ---")

def sse_stream_iterator(url: str, user_agent: str):
    """Establish a connection to a HTTP SSE stream server and generate (yield) incoming messages one-by-one. 

    Args:
        url (str): The SSE endpoint URL.
        user_agent (str): The identity string for the stream request header.
    
    Yields:
        dict: The parsed (JSON) event message.
    """

    # Request the stream with the required headers
    request_headers = {'User-Agent': user_agent}
    stream_events = SSEClient(url, headers=request_headers)

    # Iterate over each yielded event
    for raw_message in stream_events:
        # Filter events to type 'message' which contain 'data'
        if raw_message.event == 'message' and raw_message.data:
            try:
                # Filter the data which contains properties:type: 'edit' or 'new' and not 'log'
                data = json.loads(raw_message.data)
                if data['type'] == 'edit' or data['type'] == 'new':
                    # Yield the data of the oldest filtered event in the stream buffer as a JSON dict
                    yield(data)
            # Catch malformed JSON
            except json.JSONDecodeError as e:
                print(f"Skipping event: {type(e).__name__}: {e}")
                continue
            # Catch missing dict keys (e.g. 'type')
            except KeyError as e:
                print(f"Skipping event: {type(e).__name__}: {e}")
                continue

def transform_data(data: dict):

    raw_dt              = str(data.get('meta', {}).get('dt'))
    clean_dt            = str(raw_dt.replace('T', ' ').replace('Z', ''))
    title_clean         = str(data.get('title'))
    title_url_clean     = str(data.get('title_url'))
    bot_clean           = int(data.get('bot'))
    username_clean      = str(data.get('user'))

    # Use .get() with a default of 0 to prevent KeyError
    length_data = data.get('length', {})
    length_old  = int(length_data.get('old', 0)) # Returns 0 if 'old' doesn't exist
    length_new  = int(length_data.get('new', 0)) # Returns 0 if 'new' doesn't exist
    length_diff_bytes   = int(length_old - length_new)

    clean_data = {
        'event_timestamp': clean_dt,
        'title': title_clean,
        'title_url': title_url_clean,
        'bot': bot_clean,
        'username': username_clean,
        'length_bytes_old': length_old,
        'length_bytes_new': length_new,
        'length_diff_bytes': length_diff_bytes
    }

    return clean_data

def save_event_to_db(db_connection: sqlite3.Connection, db_table_name: str, event: dict) -> bool:
    """Insert a new record into the specified database table.

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        event (dict): The event data.

    Returns:
        None
    """
    cursor = db_connection.cursor()

    clean_data: dict = transform_data(event)

    sql = f'''INSERT INTO {db_table_name} (
        raw_json,
        event_timestamp,
        title,
        title_url,
        bot,
        username,
        length_bytes_old,
        length_bytes_new
        ) VALUES (?,?,?,?,?,?,?,?)'''

    try:
        cursor.execute(sql, (json.dumps(event),
                             clean_data.get('event_timestamp'),
                             clean_data.get('title'),
                             clean_data.get('title_url'),
                             clean_data.get('bot'),
                             clean_data.get('username'),
                             clean_data.get('length_bytes_old'),
                             clean_data.get('length_bytes_new'))) # Data must be in a tuple, even if it's just one value
        return True # Signal a successful save
    except sqlite3.Error as e:
        print(f'Unable to save event to database: {type(e).__name__}: {e}')
        return False # Signal an unsuccessful/skipped save

def database_init(db_name: str, db_table_name: str):
    """Initialise an SQLite3 database and return the database connection object.

    If the database file doesn't exist, create it. If a table with the configured
    name doesn't exist, create it.

    Args:
        db_name (str): The name of the target database file (e.g. 'data.db').
        db_table_name (str): The name of the target database table.
        db_max_rows (int): Maximum number of rows the database shall store in a rolling fashion.

    Returns:
        sqlite3.Connection: An active SQLite3 database connection object.

    Raises:
        sqlite3.Error: If the database cannot be initialized or the table creation fails.
    """

    # Create a database file if it doesn't exist and connect to it
    connection = sqlite3.connect(db_name)
    # Configure the database to enable Write-Ahead Logging (allows multiple readers and
    #   one writer to work simultaneously)
    connection.execute('PRAGMA journal_mode=WAL;')
    # The cursor object is python's interface to the databse manager (SQLite)
    cursor = connection.cursor()
    print('Database connection established.')

    # Create a table
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {db_table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_json TEXT,
        event_timestamp DATETIME,
        title TEXT,
        title_url TEXT,
        bot BOOLEAN,
        username TEXT,
        length_bytes_old INTEGER,
        length_bytes_new INTEGER
    )''')

    connection.commit()

    return connection

def main():
    """Execute the SSE ingestion.

    Loads the environment configuration and manages the lifecycle of the 
    database connection while the pipeline processes real-time messages.
    """
    config = load_user_config()
    db_connection = database_init(config.db_path, config.db_table_name)

    try:
        pipeline(
            db_connection,
            db_table_name=config.db_table_name,
            stream_url=config.stream_url,
            user_agent=config.user_agent,
            db_max_events=config.db_max_events
        )
    except KeyboardInterrupt:
        print('\nReceived stop signal from user.')
    finally:
        # Graceful database shutdown
        if db_connection:
            db_connection.commit()
            db_connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
