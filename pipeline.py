import os
from sseclient import SSEClient
import yaml
import sqlite3

def pipeline(db_connection, db_table_name, stream_url, user_agent, batch_size) -> None:
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
    stream = sse_stream_iterator(stream_url, user_agent)

    # Count number of rows added to the database for logging/information purposes
    rows_added = 0

    # Iterate over each yielded message
    try:
        for raw_json_message in stream:
            save_message_to_db(db_connection, db_table_name, str(raw_json_message))
            rows_added+=1
            # Commit changes to database in batches
            if rows_added % batch_size == 0:
                db_connection.commit()
    except KeyboardInterrupt:
        pass

    return rows_added

def sse_stream_iterator(url, user_agent):
    """Establish a connection to a HTTP SSE stream server and generate (yield) incoming messages one-by-one. 

    Args:
        url (str): The SSE endpoint URL.
        user_agent (str): The identity string for the stream request header.
    
    Yields:
        str: The raw 'data' string from each event of type 'message'.
    """

    # Request the stream with the required headers
    request_headers = {'User-Agent': user_agent}
    messages = SSEClient(url, headers=request_headers)

    # Iterate over each yielded event
    for message in messages:
        # Filter events to type 'message' which contain 'data'
        if message.event == 'message' and message.data:
            # Yield the data string of the oldest event in the stream buffer
            yield(message.data)

def save_message_to_db(db_connection, db_table_name, data) -> None:
    """Insert a new record into the specified database table.

    Args:
        db_connection (sqlite3.Connection): An active SQLite3 database connection object.
        db_table_name (str): The name of the target database table.
        data (Any): The payload data.

    Returns:
        None
    """
    raw_json = data

    cursor = db_connection.cursor()
    sql = f'INSERT INTO {db_table_name} (raw_json) VALUES (?)'

    try:
        cursor.execute(sql, (raw_json,)) # Data must be in a tuple, even if it's just one value
    except sqlite3.Error as e:
        print(f'SQLite3 Error: {e}')
    except Exception as e:
        print(f'Unexpected Error: {e}')

def database_init(db_name, db_table_name):
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
    print('Database connection established.')
    # The cursor object is python's interface to the databse manager (SQLite)
    cursor = connection.cursor()

    # Define the database schema
    sql = f'''CREATE TABLE IF NOT EXISTS {db_table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_json TEXT)'''
    # Create the new table and commit the change
    cursor.execute(sql)
    connection.commit()

    return connection

def load_config(config_path='config.yaml') -> dict:
    """Load the required pipeline configuration parameters.

    The dynamic configuration parameters come from a YAML file, while the
    static configuration parameters come from environment variables.

    Args:
        config_path (str, optional): Path to the YAML configuration file. Defaults to 'config.yaml'.

    Returns:
        dict: Dictionary containing all configuration parameters.

    Raises:
        FileNotFoundError: If the YAML configuration file cannot be found.
        yaml.YAMLError: If the YAML file contains invalid syntax.
    """

    # Load the dynamic config from file
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Combine all config parameters into a dict
    config_dict = {
        # Sensitive: from environment variables
        'user-agent': os.getenv('ETL_USER_AGENT'),

        # Structural: from YAML
        'stream-url': config.get('stream-url'),
        'db-name': config.get('db-name'),
        'db-table-name': config.get('db-table-name'),
        'batch-size': config.get('batch-size')
    }
    
    return config_dict

def main():
    """Execute the SSE ingestion.

    Loads the environment configuration and manages the lifecycle of the 
    database connection while the pipeline processes real-time messages.
    """
    config = load_config()
    db_connection = database_init(config['db-name'], config['db-table-name'])

    try:
        rows_added = pipeline(
            db_connection,
            db_table_name=config['db-table-name'],
            stream_url=config['stream-url'],
            user_agent=config['user-agent'],
            batch_size=int(config['batch-size'])
        )
    except KeyboardInterrupt:
        pass
    finally:
        # Graceful database shutdown
        db_connection.commit()
        db_connection.close()
        print("\nDatabase connection closed.")
        print(f'\nRows added: {rows_added}')

if __name__ == "__main__":
    main()
