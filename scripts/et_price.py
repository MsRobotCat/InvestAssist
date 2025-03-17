import time
import pandas as pd
import yfinance as yf
import json
from pathlib import Path
from utils.config_loader import CONSTANTS
from utils.logging_config import logger

logger.info("This script started running.")

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent
TICKERS_PATH = BASE_DIR / "config" / "tickers.json"
# RAW_PATH = BASE_DIR / "data" / "raw"

STAGING_DIR = BASE_DIR / "data" / "staging"

# Ensure the staging directory exists
# RAW_PATH.mkdir(parents=True, exist_ok=True)

STAGING_DIR.mkdir(parents=True, exist_ok=True)


LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"


def api_call(tickers_path, period):
    """ This function fetches data from yfinance per the period defined and save it as csv file in the raw folder"""
    max_retries = 2
    retry_delay = 900  # 15 minutes in seconds
    for attempt in range(max_retries):
        with tickers_path.open("r") as f:
            ticker_dict = json.load(f)["tickers"]

            data_dict = {}
            for ticker in ticker_dict:
                print(f"Attempting to fetch data for {ticker} (Attempt {attempt + 1}/{max_retries})")
                try:
                    ticker_obj = yf.Ticker(ticker)
                    raw_data = ticker_obj.history(period=period)
                    if raw_data.empty:
                        logger.warning(f"No data found for {ticker}, skipping...")
                        continue
                    raw_df = pd.DataFrame(raw_data).reset_index()
                    data_dict[ticker] = raw_df # a dict that has ticker as the key and dataframe and the value
                    logger.info(f"Extracted {ticker} successfully")

                except Exception as e:
                    logger.error(e)
                    print(f"Error fetching data for {ticker} on attempt {attempt + 1}: {e}")
                    time.sleep(retry_delay)

            if data_dict:
                return data_dict # if there is any data fetched, return
        return {} # If all retries fail, return empty dictionary

def clean(ticker, raw_df):
    logger.info(f"Cleaning data for {ticker}")
    raw_df = raw_df.iloc[:, :6] #select only the first 6 columns
    raw_df.columns = ['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
    raw_df['date'] = pd.to_datetime(raw_df['date']).dt.date # convert timestamp to YYY-MM-DD format
    desired_column_order = ['date', 'close_price', 'open_price', 'high_price', 'low_price', 'volume']
    cleaned_df = raw_df[desired_column_order].copy() # Rearrange column order to align with db's table and explicitly create a copy
    cleaned_df['yahoo_ticker'] = ticker # add the ticker as a new column
    cleaned_df = cleaned_df.sort_values('date').dropna() #sort by date and remove NaN

    logger.info(f"Save price data for {ticker} as csv")
    cleaned_df.to_csv(STAGING_DIR / f"staging_{ticker}.csv", index=False)
    

def main(tickers_path, period):
    data_dict = api_call(tickers_path, period)
    for ticker, raw_dataframe in data_dict.items():
        clean(ticker, raw_dataframe)


if __name__ == "__main__":
    main(TICKERS_PATH, CONSTANTS["period"])
