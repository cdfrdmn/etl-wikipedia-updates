import os
from sseclient import SSEClient
import yaml
from dotenv import load_dotenv

def sse_stream_iterator(url, user_agent):
    """
    Connect to an HTTP SSE stream server and iterate over received messages.

    :param url: The URL of the desired SSE stream source.
    :param url: The identifier of the application requesting the stream data (the default python user agent is usually blocked by a 403).
    """
    request_headers = {'User-Agent': user_agent}
    messages = SSEClient(url, headers=request_headers)

    for message in messages:
        print(message)

def load_config(config_path='config.yaml'):
    """
    Load static config from YAML and dynamic config from environment.
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
        'stream-url': config.get('stream-url')
    }
    
    return config_dict

def main():
    config = load_config()
    sse_stream_iterator(config['stream-url'], config['user-agent'])

if __name__ == "__main__":
    main()
