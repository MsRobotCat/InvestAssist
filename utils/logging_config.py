import json
import logging
import logging.config
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path

# Dynamically define paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"

def load_config():
    """Loads logging settings from JSON."""
    if not LOG_CONFIG_PATH.exists():
        print(f"Warning: {LOG_CONFIG_PATH} not found. Using defaults.")
        return {}
    with LOG_CONFIG_PATH.open("r") as f:
        return json.load(f)

def setup_logging():
    """Sets up logging with rotation support."""
    config = load_config()

    # Get log rotation settings
    rotation = config.get("rotation", {})
    rotation_type = rotation.get("type", "time")  # Default: time-based rotation

    if rotation_type == "size":
        handler = RotatingFileHandler(
            str(LOG_PATH),
            maxBytes=rotation.get("max_bytes", 5 * 1024 * 1024),  # Default 5MB
            backupCount=rotation.get("backup_count", 3)
        )
    else:  # Default to time-based rotation
        handler = TimedRotatingFileHandler(
            str(LOG_PATH),
            when=rotation.get("when", "midnight"),
            interval=rotation.get("interval", 1),
            backupCount=rotation.get("backup_count", 7)
        )

    # Set formatter
    log_format = config.get("format", "%(asctime)s - %(levelname)s - %(message)s")
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)

    # Set up logger
    logger = logging.getLogger("investassist")
    logger.setLevel(config.get("log_level", "INFO").upper())
    logger.addHandler(handler)

    return logger

# Initialize logger once
logger = setup_logging()
logger.info("Logging initialized.")