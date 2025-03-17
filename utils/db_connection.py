import json
import psycopg2
from pathlib import Path
from utils.logging_config import logger  # Import logger from centralized logging

BASE_DIR = Path(__file__).resolve().parent.parent
DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"

def load_config_as_dict(config_path):
    """Loads database config from a JSON file."""
    if not config_path.exists():
        logger.warning(f"Config file {config_path} not found. Using defaults.")
        return {}
    with config_path.open("r") as f:
        return json.load(f)

def connect_db(config_path=DB_CONFIG_PATH):
    """Establishes a database connection using credentials from a JSON config."""
    params = load_config_as_dict(config_path)
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        logger.info("Connected to the database successfully.")
        return conn, cur
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None, None

