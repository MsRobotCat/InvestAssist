======================================================
                   INVESTASSIST 1.5
       A Personal Stock & ETF Analysis Screening Tool
======================================================

1. ABOUT THE PROJECT
------------------------------------------------------
InvestAssist 1.5 is my first full ETL pipeline project, built to put my data engineering knowledge into practice while also improving my own investing workflow. I wanted to create a system that automates data collection, processing, and decision-making, making my investment process more structured and less reliant on emotions.

This project is not about chasing short-term trends—it’s about building a systematic approach that allows me to act decisively based on consistent data analysis rather than market noise.

Why I Built InvestAssist
- To put my data engineering skills into practice – This is my first full ETL pipeline, where I designed everything from data ingestion to processing and output automation.
- To avoid emotional trading – Instead of relying on gut feeling, I get clear signals based on RSI & SMA to help with my buy/sell decisions.
- To reduce time spent on market research – A daily summary email gives me a snapshot of opportunities in under a minute.
- To create a flexible tool for long-term investing – The system is built in a way that allows future adjustments, whether in indicator settings or data sources.

Lessons Learned & Future Plans

After using InvestAssist for a few months, I realized that the buy/sell scoring system was too simplistic to fully support my trading decisions. To address this, I plan to develop InvestAssist 2.0 in 2025, incorporating a more sophisticated momentum-based scoring model and expanded features.
Moreover, the current pipeline is not so scalable. In InvestAssist 2.0, I want it to be more scalable and move it to AWS.


Key Features
- Fetches ETF & stock price data and stores it in a structured database.
- Calculates key indicators: Relative Strength Index (RSI_7) and Simple Moving Averages (SMA_5, SMA_10).
- Flags potential trades based on predefined conditions (e.g., RSI < 30 = Buy Signal, RSI > 70 = Sell Signal).
- Sends a daily summary email so I can check opportunities quickly.
- Maintains a historical record of RSI & SMA trends to track market shifts over time.

------------------------------------------------------

2. PROJECT STRUCTURE
------------------------------------------------------
The project is organized as follows:

InvestAssist/
├── .venv/                   # Virtual environment (not included in Git)
├── config/                  # Configuration files
│   ├── constants.json       # Stores global settings (e.g., period, RSI/SMA settings)
│   ├── db_config.json       # Database credentials & settings
│   ├── email_config.json    # Email configuration for notifications
│   ├── logging_config.json  # Logging settings
│   ├── tickers.json         # List of tracked ETFs/stocks
├── data/                    # Stores input data
│   ├── staging/             # Cleaned data before processing
│   ├── processed/           # Transformed data
├── logs/                    # Stores log files
│   ├── etl.log              # Logs for ETL processes
├── scripts/                 # Main scripts
│   ├── email.py             # Sends daily email notifications
│   ├── et_indicators.py     # Extract and transform indicators from price data in DB
│   ├── et_price.py          # Extracts and transforms price data from yfinance
│   ├── l_indicators.py      # Loads indicators into DB
│   ├── l_price.py           # Loads price data into DB
│   ├── tl_transactions.py   # Cleans & loads transactions into DB (manual run)
├── utils/                   # Utility functions
│   ├── __init__.py          # Makes `utils/` a Python package
│   ├── config_loader.py     # Loads settings from JSON config files
│   ├── db_connection.py     # Handles database connections
│   ├── logging_config.py    # Centralized logging setup
├── main.py                  # Main entry point for running the pipeline
├── README.txt               # Documentation
├── requirements.txt         # Python dependencies

------------------------------------------------------

3. REQUIREMENTS
------------------------------------------------------
To run InvestAssist, you need the following:

- Python 3.12 or higher
- PostgreSQL installed and configured
- Required Python libraries (see requirements.txt)
- Gmail account for sending email notifications

Install the required Python libraries:
------------------------------------------------------

4. USAGE
------------------------------------------------------
Follow these steps to use InvestAssist:

1. **Set up PostgreSQL**:
   - Configure the `db_config.json` file in the `config/` folder with your database credentials.
   - See section 6 for for DB schema

2. **Run the ETL pipeline**:
   - Use `main.py` to execute the ETL process:
     ```
     python main.py
     ```

3. **Check Logs**:
   - Logs of the ETL process are stored in `logs/etl.log`.
   - Log files are automatically managed with a rotation policy

4. **View Results**:
   - Processed indicators and price data are stored in the PostgreSQL database.


5. **Daily Email Notification**:
   - Ensure `email.py` is configured with your Gmail credentials to receive a summary of processed data.

------------------------------------------------------

5. CRON JOB SETUP
------------------------------------------------------
Automate the ETL pipeline using a cron job. Add the following line to your crontab to trigger the pipeline every weekday at 5 AM:
0 5 * * 1-5 python /path/to/InvestAssist/main.py

6. DATABASE SCHEMA
------------------------------------------------------
The schema is intentionally denormalised for easy query. Please see the entity-relationship diagram (ERD) for reference.
Please note that I wished to use either ticker or ISIN as the asset's primary key but assets with particular ISIN does not always available on Yahoo. That is why asset_id is needed. You will need to match the asset that you buy with asset on Yahoo manually.