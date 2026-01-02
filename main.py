import os
from sseclient import SSEClient
import yaml
from dotenv import load_dotenv
import sqlite3

def pipeline(config, db_connection):

    stream = sse_stream_iterator(config['stream-url'], config['user-agent'])

    try:
        for message in stream:
            save_message_to_db(db_connection, config['db-table-name'], str(message))
    except KeyboardInterrupt:
        print('Stop signal received')
    finally:
        db_connection.commit()
        db_connection.close()
        print("Database connection closed.")

def sse_stream_iterator(url, user_agent):
    """
    Connects to an HTTP SSE stream server and generates stream messages.

    :param url: The URL of the desired SSE stream source.
    :param user_agent: The identifier of the application requesting the stream (the default python user agent is usually blocked by a 403).
    """
    request_headers = {'User-Agent': user_agent}
    messages = SSEClient(url, headers=request_headers) # This iterable provides access to received stream messages

    for message in messages:
        if message.event == 'message' and message.data: # filter events to type 'message' which contain 'data'
            yield(message.data) # return the oldest event in the stream buffer

def save_message_to_db(connection, table_name, data):
    """
    Accept a database connection object, a message string, and write the data to a new row in the database.
    """
    cursor = connection.cursor()
    sql = f'INSERT INTO {table_name} (message) VALUES (?)'
    try:
        cursor.execute(sql, (data,)) # Data must be in a tuple, even if it's just one value
    except sqlite3.Error as e:
        print(f'SQLite3 Error: {e}')
    except Exception as e:
        print(f'Unexpected Error: {e}')

def database_init(db_name, db_table_name):
    """
    Initialise an SQLite database and return the live connection object.
    """
    # Create a database file if it doesn't exist and connect to it
    connection = sqlite3.connect(db_name)
    print('Database connection established.')
    # Create a cursor object (the interface between Python and the databse manager (SQLite)
    cursor = connection.cursor()
    # Define the database schema
    sql = f'CREATE TABLE IF NOT EXISTS {db_table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, message STRING)'
    cursor.execute(sql)
    # Commit the new table
    connection.commit()

    return connection

def load_config(config_path='config.yaml'):
    """
    Load static configuration parameters from a YAML file and
    dynamic configuration parameters from the environment.
    """
    # Load environment variables from local .env file if available
    load_dotenv()

    # Load YAML config file
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Combine into a single Python object
    config_dict = {
        'user-agent': os.getenv('ETL_USER_AGENT'),
        'stream-url': config.get('stream-url'),
        'db-name': config.get('db-name'),
        'db-table-name': config.get('db-table-name')
    }
    
    return config_dict

def main():
    config = load_config()
    db_connection = database_init(config['db-name'], config['db-table-name'])
    pipeline(config, db_connection)

if __name__ == "__main__":
    main()
