from sseclient import SSEClient
import sqlite3
import json
from config import load_user_config

def pipeline(db_connection: sqlite3.Connection, db_table_name: str, stream_url: str, user_agent: str, batch_size: int) -> None:
    """Orchestrate the end-end pipeline for real-time SSE message ingestion into an SQLite database. 

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        stream_url (str): SSE endpoint URL.
        user_agent (str): Identity string for the stream request header.
        batch_size (int): The number of rows to be added to the database before committing changes.

    Returns:
        int: Number of rows added to the database table since execution start.
    """
    # Instantiate the iterator
    event_feed = sse_stream_iterator(stream_url, user_agent)

    rows_added_to_db = 0

    # Iterate over each yielded message
    for event in event_feed:
        # Call the save function and check if it was successful
        if save_event_to_db(db_connection, db_table_name, event):
            rows_added_to_db+=1
            # Limit database calls by batch committing
            if rows_added_to_db % batch_size == 0:
                db_connection.commit()
                print(f"Batch committed since execution start: {rows_added_to_db} total rows.")

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
                    yield(json.dumps(data))
            # Catch malformed JSON
            except json.JSONDecodeError as e:
                print(f"Skipping event: {type(e).__name__}: {e}")
                continue
            # Catch missing dict keys (e.g. 'type')
            except KeyError as e:
                print(f"Skipping event: {type(e).__name__}: {e}")
                continue

def save_event_to_db(db_connection: sqlite3.Connection, db_table_name: str, event: str) -> bool:
    """Insert a new record into the specified database table.

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): Name of the target database table.
        event (dict): The event data.

    Returns:
        None
    """
    cursor = db_connection.cursor()

    # Convert event dict to string for the database
    event_timestamp = json.loads(event)['meta']['dt'] # Already in ISO-8601 format

    sql = f'INSERT INTO {db_table_name} (raw_json, event_timestamp) VALUES (?,?)'

    try:
        cursor.execute(sql, (event, event_timestamp)) # Data must be in a tuple, even if it's just one value
        return True # Signal a successful save
    except sqlite3.Error as e:
        print(f'Unable to save event to database: {type(e).__name__}: {e}')
        return False # Signal an unsuccessful/skipped save

def database_init(db_name: str, db_table_name: str, db_max_rows: int):
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
        event_timestamp DATETIME
    )''')

    # Add the "Rolling" Trigger which fires automatically after every INSERT.
    # It deletes any record older than the 'db_max_rows' newest records.
    cursor.execute(f'''
    CREATE TRIGGER IF NOT EXISTS rolling_limit_{db_table_name}
    AFTER INSERT ON {db_table_name}
    BEGIN
        DELETE FROM {db_table_name}
        WHERE id <= (
            SELECT id FROM {db_table_name}
            ORDER BY id DESC
            LIMIT 1 OFFSET {db_max_rows}
        );
    END;
    ''')
    connection.commit()

    return connection

def main():
    """Execute the SSE ingestion.

    Loads the environment configuration and manages the lifecycle of the 
    database connection while the pipeline processes real-time messages.
    """
    config = load_user_config()
    db_connection = database_init(config.db_path, config.db_table_name, config.db_max_events)

    try:
        pipeline(
            db_connection,
            db_table_name=config.db_table_name,
            stream_url=config.stream_url,
            user_agent=config.user_agent,
            batch_size=config.db_batch_size
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
