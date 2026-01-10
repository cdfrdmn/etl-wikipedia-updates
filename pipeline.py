import requests
import sseclient
import sqlite3
import json
import time
from typing import cast, Iterator, Generator, Any
from config import load_user_config

def pipeline(db_connection: sqlite3.Connection, db_table_name: str, stream_url: str, user_agent: str, db_max_events: int) -> None:
    """ Manage the connection with the SSE server and trigger the insertion of received data into the database.

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        stream_url (str): SSE endpoint URL.
        user_agent (str): Identity string for the stream request header.
        db_max_events (int): Maximum number of rows in the database.
    
    Returns:
        None
    """
    rows_added_to_db = 0
    last_commit_time = time.time()
    commit_interval_seconds = 2
    while True:
        try:
            # Iterate over each yielded event
            for event in sse_event_generator(stream_url, user_agent):
                # Call the save function and check if it was successful
                if db_insert_event(db_connection, db_table_name, event):
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
        # Handle stream interruptions due to connection timeout or corrupted chunks
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            print(f'Stream interrupted: {e}\nRetying in 5 seconds')
            time.sleep(5)
            continue

def sse_event_generator(url: str, user_agent: str) -> Generator[dict[str, Any], None, None]:
    """Establish a connection to a HTTP SSE stream server and generate (yield) incoming events one-by-one. 

    Args:
        url (str): The SSE endpoint URL.
        user_agent (str): The identity string for the stream request header.
    
    Yields:
        dict[str, Any]: The parsed event message in JSON.
    """
    # Set headers to request rhe server keeps the connection open and sends a stream of events
    request_headers: dict[str, str] = {'User-Agent': user_agent, 'Accept': 'text/event-stream'}
    # Create the raw byte stream object
    response = requests.get(url, stream=True, headers=request_headers)
    print('Stream request sent')
    print(f'Response: {response.status_code}')
    # Parse the raw bytestream according to the SSE protocol. Use cast() to tell Pylance to treat the reponse object as an iterator
    client = sseclient.SSEClient(cast(Iterator[bytes], response))

    # Iterate over each parsed event object [sseclient.Event] when available
    for event in client.events():
        # Filter events to type 'message' which contain data
        if event.event == 'message' and event.data:
            try:
                # Cast raw message into a dict of JSON
                data = json.loads(event.data)
                # Filter eventa to only types 'edit' or 'new'
                if data.get('type') in ('edit', 'new'):
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

def transform_data(data: dict[str, Any]) -> dict[str, Any]: # JSON dictionary has a mix of strings, integers, and nested objects
    """Accept a dictionary of JSON data, and output a normalized/cleaned version.

    Args:
        data (dict[str, Any]): Data to be cleaned.

    Returns:
        dict[str, Any]: Cleaned data.
    """
    raw_dt              = str(data.get('meta', {}).get('dt'))
    length_data         = data.get('length', {})
    length_old          =  int(length_data.get('old', 0)) # Returns 0 if 'old' doesn't exist
    length_new          =  int(length_data.get('new', 0)) # Returns 0 if 'new' doesn't exist
    length_diff_bytes   = int(length_old - length_new)

    # Set default values where possible to prevent KeyError
    clean_data: dict[str, Any] = {
        'event_timestamp':  str(raw_dt.replace('T', ' ').replace('Z', '')), # Remove 'T' and 'Z' to make it SQLite compatible, e.g. "2026-01-08 22:35:51"
        'title':            str(data.get('title')),
        'title_url':        str(data.get('title_url')),
        'bot':              int(data.get('bot')),
        'username':         str(data.get('user')),
        'length_bytes_old': length_old,
        'length_bytes_new': length_new,
        'length_diff_bytes': length_diff_bytes
    }

    return clean_data

def db_insert_event(db_connection: sqlite3.Connection, db_table_name: str, event: dict[str, Any]) -> bool:
    """Accepts an event, transforms the data and inserts it into the database.

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        event (dict[str, Any]): The event data.

    Returns:
        bool: The status of the insertion. Successful=True, Unsuccessful=False.
    """
    cursor = db_connection.cursor()

    clean_data = transform_data(event)

    sql = f'''INSERT INTO {db_table_name} (
        raw_json,
        event_timestamp,
        title,
        title_url,
        bot,
        username,
        length_bytes_old,
        length_bytes_new,
        length_diff_bytes
        ) VALUES (?,?,?,?,?,?,?,?,?)'''

    try:
        cursor.execute(sql, (json.dumps(event),
                             clean_data.get('event_timestamp'),
                             clean_data.get('title'),
                             clean_data.get('title_url'),
                             clean_data.get('bot'),
                             clean_data.get('username'),
                             clean_data.get('length_bytes_old'),
                             clean_data.get('length_bytes_new'),
                             clean_data.get('length_diff_bytes')
                            )
        ) # Data must be in a tuple, even if it's just one value
        return True
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

    Returns:
        sqlite3.Connection: An active SQLite3 database connection object.

    Raises:
        sqlite3.Error: If the database cannot be initialized or the table creation fails.
    """
    # Create a database file if it doesn't exist and connect to it
    connection = sqlite3.connect(db_name)
    # Allow one writer and multiple readers to access the db simultaneously
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
        length_bytes_new INTEGER,
        length_diff_bytes INTEGER
        )'''
    )
    connection.commit()

    return connection

def main():
    """Load the application configuration and start the ETL pipeline."""
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
