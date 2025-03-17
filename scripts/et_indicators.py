import pandas as pd
from datetime import timedelta
from pathlib import Path
import json
from utils.db_connection import connect_db
from utils.config_loader import CONSTANTS
import time
from utils.logging_config import logger

logger.info("This script started running.")

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent

# Determine the directory or path of the following
INDICATORS_CONFIG_PATH = BASE_DIR / "config" / "constants.json"
INDICATORS_OUTPUT_PATH = BASE_DIR / "data" / "processed" / "indicators.csv"
DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"
TICKERS_PATH = BASE_DIR / "config" / "tickers.json"
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_DIR = BASE_DIR / "logs"
LOG_PATH = BASE_DIR / "logs" / "etl.log"



# get the windows from constants.json
def indicator_config():
    rsi_window = CONSTANTS["rsi_window"]
    short_sma_window = CONSTANTS["short_sma_window"]
    long_sma_window =  CONSTANTS["long_sma_window"]
    max_calculation_range = max(rsi_window, short_sma_window, long_sma_window) - 1
    return rsi_window, short_sma_window, long_sma_window, max_calculation_range


def fetch_price_data(cur, tickers_path):
    max_retries = 2
    retry_delay = 60  # 1 min delay
    rsi_window, short_sma_window, long_sma_window, max_calculation_range = indicator_config()
    latest_date_query = '''
    SELECT MAX(p.date) FROM price p JOIN asset a ON a.asset_id = p.asset_id WHERE a.yahoo_ticker = %s;
    ''' # we need to specify max date per asset as assets are in different stock markets with varying public holidays
    close_price_query =  '''
    SELECT date, close_price FROM price WHERE asset = %s AND date BETWEEN %s AND %s;
    '''
    with tickers_path.open("r") as f:
        ticker_dict = json.load(f)["tickers"]
    data_dict = {}
    for ticker in ticker_dict:
        for attempt in range(max_retries):
            print(f"Attempting to fetch close_prices of {ticker} for calculating indicators. (Attempt {attempt + 1}/{max_retries})")
            logger.info(f"Attempt to fetch close_prices of {ticker} for calculating indicators (Attempt {attempt + 1}/{max_retries}")
            try:
                cur.execute(latest_date_query, (ticker,))
                latest_date = cur.fetchone()[0]
                start_date = latest_date = timedelta(days=max_calculation_range)
                cur.execute(close_price_query, (ticker, start_date, latest_date))
                rows = cur.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["date", "close_price"])

                    # doing light transformation below before appending to data_dict
                    df.set_index("date", inplace=True) # set date as index
                    df.sort_index(inplace=True) # ensure date is in order
                    df['close_price'] = df['close_price'].astype(float) # ensure that it's float

                    data_dict[ticker] = df
                else:
                    print(f"No close_prices data found for {ticker} during the {start_date} and {latest_date}.")
                    continue
                break #exit retry loop on success
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Trying to fetch price data for {ticker} attempt {attempt + 1} failed with error: {e}")
                    logger.error(f"Trying to fetch price data for {ticker} attempt {attempt + 1} failed with error: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"All attempts to fetch data for {ticker} with error: {e} Skipping.")
                    logger.error(f"All attempts to fetch data for {ticker} failed with error: {e}. Skipping.")
    return data_dict



def calculate_rsi(data, window):
    delta = data['close_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Function to calculate SMA
def calculate_sma(data, window):
    return data['close_price'].rolling(window=window).mean()


def main(tickers_path):
    conn, cur = connect_db()
    rsi_window, short_sma_window, long_sma_window, max_calculation_range = indicator_config()
    if conn is None:
        print("Can't connect to database.")
        logger.info(f"Can't connect to database.")
        return
    else:
        price_data_dict = fetch_price_data(cur, tickers_path)
        data = pd.DataFrame(columns=['date', 'sma_5', 'sma_10', 'rsi', 'yahoo_ticker']) # setting columns that aligns with the table in db
        for ticker, close_price_df in price_data_dict.items():
            if not close_price_df.empty:
                ticker = ticker
                latest_date = close_price_df.index.max().date()
                rsi = calculate_rsi(close_price_df, rsi_window).iloc[-1]
                short_sma = calculate_sma(close_price_df, short_sma_window).iloc[-1]
                long_sma = calculate_sma(close_price_df, long_sma_window).iloc[-1]
                data = pd.concat([{
                    "date": latest_date,
                    "sma_5": short_sma,
                    "sma_10": long_sma,
                    "rsi": rsi,
                    "yahoo_ticker": ticker
                    }])
            else:
                print(f"No close_prices data found for {ticker}. Skipping.")
                logger.info(f"No close_prices data found for {ticker}. Skipping.")
        data.to_csv(INDICATORS_OUTPUT_PATH, index=False)
        print(f"Indicators written to {INDICATORS_OUTPUT_PATH}")
        logger.info(f"Indicators written to {INDICATORS_OUTPUT_PATH}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main(tickers_path=TICKERS_PATH)

