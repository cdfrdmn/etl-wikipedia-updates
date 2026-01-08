from pydantic import Field
from pydantic_settings import BaseSettings
from pathlib import Path
from pprint import pprint
import yaml

class Settings(BaseSettings):
    # This app is dedicated to one URL, so hardcode the default
    stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    
    # Default to local DB path, overridden by DB_PATH in Docker
    db_path: str = "data/wikipedia-events.db"

    # Hard-code table name, no need to be configurable
    db_table_name: str = 'wiki_events'

    # User config:
    user_agent: str = Field(default='WikiETL-Bot', alias='user-agent')
    db_batch_size: int = Field(default=50, alias='db-batch-size')
    db_max_events: int = Field(default=100000, alias='db-max-events')

def load_user_config(config_path: Path | str = 'config.yaml') -> Settings:
    yaml_data = {}
    path = Path(config_path)
    
    # Check a user config yaml exists
    if not path.is_file():
        print(f'No config file found at {path.absolute()}. Using default settings.')
    else:
        with path.open('r') as f:
            # Load the dict, ensuring we handle empty files with 'or {}'
            yaml_data = yaml.safe_load(f)
    
    # Convert the dict into our model
    settings = Settings.model_validate(yaml_data)
    print('Starting with configuration:'); pprint(settings.model_dump()); print('\n')
    
    return settings
