import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))
