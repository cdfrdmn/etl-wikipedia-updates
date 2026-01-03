import os
from dotenv import load_dotenv

load_dotenv() # If a local .env file exists, import its environment variables

class Config:
    """
    Centralised configuration for ETL pipeline.
    """
    WIKI_STREAM_URL = os.getenv('WIKI_STREAM_URL')

config = Config()