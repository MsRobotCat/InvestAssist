import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONSTANTS_PATH = BASE_DIR / "config" / "constants.json"


def load_constants():
    """Loads constants from the JSON config file."""
    if not CONSTANTS_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONSTANTS_PATH}")

    with CONSTANTS_PATH.open("r") as f:
        return json.load(f)


# Load constants once to use across scripts
CONSTANTS = load_constants()