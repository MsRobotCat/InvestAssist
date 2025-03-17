from pathlib import Path
import json
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from utils.logging_config import logger

logger.info("This script started running.")

#TODO instead of reading input from local file, read input from DB when DB is accessible

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent

# Determine the directory or path of the following
INPUT_PATH = BASE_DIR / "data" / "indicators.csv"
EMAIL_CONFIG_PATH = BASE_DIR / "config" / "email_config.json"
LOG_DIR = BASE_DIR / "logs"
DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"
TICKERS_PATH = BASE_DIR / "config" / "tickers.json"
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"

def load_config_as_dict(config_path):
    if not config_path.exists():
        print(f"Warning: Config file {config_path} not found")
    with config_path.open("r") as f:
        return json.load(f)

def load_config_as_dict(config_dir):
    with config_dir.open("r") as f:
        return json.load(f)

def analyse_indicators(indicator_csv):
    # save indicators.csv as dataframe and enforce the data type
    df = pd.read_csv(indicator_csv)
    df["rsi"] = df["rsi"].astype(float)
    df["sma_5"] = df["sma_5"].astype(float)
    df["sma_10"] = df["sma_10"].astype(float)

    # Filter RSI-based buy/sell signals
    overbought = df[df["rsi"] > 70]  # RSI above 70 → Overbought (Sell)
    oversold = df[df["rsi"] < 30]  # RSI below 30 → Oversold (Buy)
    undervalued = df[(df["rsi"] >= 30) & (df["rsi"] < 40)]  # RSI 30-40 → Undervalued

    # Detect general trends using SMA comparison
    bullish_trend = df[df["sma_5"] > df["sma_10"]]  # SMA_5 > SMA_10 → Bullish
    bearish_trend = df[df["sma_5"] < df["sma_10"]]  # SMA_5 < SMA_10 → Bearish

    # Convert to lists (for easier email formatting)
    overbought_list = overbought[["yahoo_ticker", "rsi"]].values.tolist()
    oversold_list = oversold[["yahoo_ticker", "rsi"]].values.tolist()
    undervalued_list = undervalued[["yahoo_ticker", "rsi"]].values.tolist()
    bullish_list = bullish_trend["yahoo_ticker"].tolist()
    bearish_list = bearish_trend["yahoo_ticker"].tolist()
    return overbought_list, oversold_list, undervalued_list, bullish_list, bearish_list

def email_content(overbought_list, oversold_list, undervalued_list, bullish_list, bearish_list):
    today_date = datetime.now().strftime("%B %d, %Y")
    subject = f"Assets to watch for {today_date}"

    def format_list(lst, value_label="RSI"):
        return "\n".join([f"- {ticker}: {value_label} {value:.1f}" for ticker, value in lst]) if lst else "None"

    body = f"""**These assets meet the conditions for buy or sell for today({today_date})**
    
    **Potential Buy RSI < 30 (oversold) **:
    {format_list(oversold_list, "RSI")}  

    **Potential Sell RSI > 70 (Overbought)**:
    {format_list(overbought_list, "RSI")} 

    **Bullish Trend (SMA_5 > SMA_10)**:
    {', '.join(bullish_list) if bullish_list else 'None'}

    **Bearish Trend (SMA_5 < SMA_10)**:
    {', '.join(bearish_list) if bearish_list else 'None'}
    """
    return subject, body

# Function to send the email
def send_email(subject, body, email_config_path):
    email_config = load_config_as_dict(email_config_path)

    # credentials
    smtp_server = email_config["smtp_server"]
    smtp_port = email_config["smtp_port"]
    sender_email = email_config["sender_email"]
    receiver_email = email_config["receiver_email"]
    password = email_config["password"]

    # construct the email
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain")) # Body of an email needs to use .attach. Attach as plain text


    # Connect to SMTP server
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Secure the connection
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string()) # convert msg from MIME object to string
        server.quit()

        print("Email sent successfully!")
        logger.info("Email sent successfully!")

    except Exception as e:
        print(f"Failed to send email: {e}")
        logger.error(f"Failed to send email: {e}")


def main(indicator_csv, email_config_path):
    overbought_list, oversold_list, undervalued_list, bullish_list, bearish_list = analyse_indicators(indicator_csv)
    subject, body = email_content(overbought_list, oversold_list, undervalued_list, bullish_list, bearish_list)
    send_email(subject, body, email_config_path)

if __name__ == "__main__":
    main(INPUT_PATH, EMAIL_CONFIG_PATH)