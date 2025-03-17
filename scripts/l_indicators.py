from utils.db_connection import connect_db
from pathlib import Path
from utils.logging_config import logger

logger.info("This script started running.")

#TODO refactor l_price.py and l_indicators.py into a single script by making it parameterized.
# The differences are input source (entire folder vs single file), table names and queries

#TODO test this with db connection. DB has been inaccessible

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent

# Define the directories or paths of the following
INPUT_PATH = BASE_DIR / "data" / "processed" / "indicators.csv"
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
EMAIL_CONFIG_PATH = BASE_DIR / "config" / "email_config.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"
LOG_DIR = BASE_DIR / "logs"

def load_to_staging_indicator_table(conn, cur, csv_input_path):
    if conn is None:
        raise Exception("Failed to connect to database")

    query_create_table = """
        CREATE TABLE IF NOT EXISTS staging_indicator (
            id SERIAL PRIMARY KEY, 
            date DATE,
            sma_5 NUMERIC(10,2),
            sma_10 NUMERIC(10,2),
            rsi NUMERIC(5,2),
            yahoo_ticker VARCHAR(20), 
            ); 
        """

    logger.info(f"Loading price data of {csv_input_path} into staging table")
    print(f"Loading {csv_input_path} into staging table.")
    with csv_input_path.open("r") as f:
        next(f)  # skip the first line (header)
        try:
            cur.execute(query_create_table)
            print("Staging_indicator table exists or was created")
            cur.copy_from(f, 'staging_indicator', sep=',', null="NULL",columns=('date', 'sma_5', 'sma_10', 'rsi', 'yahoo_ticker'))
            print("Loaded data to staging_price table successfully")
        except Exception as e:
            print(f"Error {e}")
            if conn:
                conn.rollback()
                return False


def load_to_indicator_table(conn, cur):
    if conn is None:
        raise Exception("Failed to connect to database")

    query = """
    INSERT INTO indicator (date, sma_5, sma_10, rsi, asset_id)
    SELECT s.date, s.sma_5, s.sma_10, s.rsi, a.asset_id 
    FROM staging_indicator s 
    LEFT JOIN asset a ON a.yahoo_ticker = s.yahoo_ticker 
    WHERE NOT EXISTS (
        SELECT 1
        FROM price p 
        WHERE a.asset_id = p.asset_id 
        AND  s.date = p.date);
    """
    try:
        cur.execute(query)
        logger.info("Loaded price table successfully")
        print("Loaded to price table successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to load price table: {e}")
        print(f"Error {e}")
        if conn:
            conn.rollback()
        return False

def delete_rows_staging(conn, cur):
    if conn is None:
        return
    query = """
    DELETE FROM staging_indicator;
    """
    try:
        cur.execute(query)
        print("Deleted records from staging_indicator table successfully.")
        logger.info("Deleted records from staging_indicator table successfully.")
        return True
    except Exception as e:
        print(f"Error {e}")
        logger.error("Failed to delete records from staging_indicator table.")
        conn.rollback()
        if conn:
            conn.rollback()
        return False

def main(input_path):
    conn, cur = connect_db()
    if conn is None:
        print("Failed to connect to database. Exiting.")
        logger.error("Failed to connect to database. Exiting.")
        return
    try:
        load_to_staging_indicator_table(conn, cur, input_path)
        load_to_indicator_table(conn, cur)
        delete_rows_staging(conn, cur)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in main(): {e}")
    finally:
        if cur and not cur.closed:
            cur.close()
        if conn and not conn.closed:
            conn.close()

if __name__ == "__main__":
    main(input_path=INPUT_PATH)
