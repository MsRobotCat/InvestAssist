from scripts.et_indicators import main as calculate_indicators
from scripts.email import main as send_email
from scripts.l_indicators import main as load_indicators
from scripts.et_price import main as extract_transform_price
from scripts.l_price import main as load_price
from pathlib import Path
from utils.config_loader import CONSTANTS
from utils.logging_config import logger

logger.info("This script started running.")

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent

# Determine the directory or path of the following
INDICATORS_CONFIG_PATH = BASE_DIR / "config" / "constants.json"
INDICATORS_CSV_PATH = BASE_DIR / "data" / "indicators.csv"
DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"
TICKERS_PATH = BASE_DIR / "config" / "tickers.json"
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_DIR = BASE_DIR / "logs"
LOG_PATH = BASE_DIR / "logs" / "etl.log"
EMAIL_CONFIG_PATH = BASE_DIR / "config" / "email_config.json"
STAGING_FOLDER = BASE_DIR / "data" / "staging"

PERIOD = '3mo'  # Options are '1d', '5d', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

def main():
    logger.info("Starting InvestAssist")

    # Step 1: make API call, transform data and save to staging directory
    logger.info("Fetching prices from yahoo and light transform the data")
    extract_transform_price(tickers_path=TICKERS_PATH, period=CONSTANTS["period"])

    # Step 2: Load transformed price data to db
    logger.info("Loading prices to db")
    load_price(STAGING_FOLDER)

    # Step 3: connect with db and calculate indicators, then save it to data dirctory
    logger.info("Calculating indicators")
    calculate_indicators(TICKERS_PATH)

    # Step 4: load indicators to db
    logger.info("Loading indicators to db")
    load_indicators(input_path=INDICATORS_CSV_PATH)

    # Step 5: Analyse the indicators, apply filter and send an email
    logger.info("Email sent!")
    send_email(INDICATORS_CSV_PATH, EMAIL_CONFIG_PATH)

if __name__ == "__main__":
    main()