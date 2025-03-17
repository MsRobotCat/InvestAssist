import os
from pathlib import Path
from utils.db_connection import connect_db
from utils.logging_config import logger

logger.info("This script started running.")

#TODO refactor l_price.py and l_indicators.py into a single script by making it parameterized.
# The differences are input source (entire folder vs single file), table names and queries

#TODO test this with db connection. DB has been inaccessible

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent

# Input directories
STAGING_FOLDER = BASE_DIR / "data" / "staging"

DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"

# Ensure the input directories exist
os.makedirs(STAGING_FOLDER, exist_ok=True)

LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"


def load_to_staging_price_table(conn, cur, input_path):
    if conn is None:
        raise Exception("Failed to connect to database")

    query_create_table = """
        CREATE TABLE IF NOT EXISTS staging_price (
            id SERIAL PRIMARY KEY, 
            date DATE,
            open_price NUMERIC(10,2),
            high_price NUMERIC(10,2),
            low_price NUMERIC(10,2),
            close_price NUMERIC(10,2), 
            volume bigint, 
            yahoo_ticker VARCHAR(50)
            ); 
        """
    for csv_file in input_path.glob("staging_*.csv"):
        logger.info(f"Loading price data of {csv_file.name} into staging table")
        # ticker = csv_file.stem.split("_")[1]
        print(f"Loading {csv_file.name} into staging table.")
        with csv_file.open("r") as f:
            next(f)  # skip the first line (header)
            try:
                cur.execute(query_create_table)
                print("Staging_price table exists or was created")
                cur.copy_from(f, 'staging_price', sep=',', null="NULL",columns=('date', 'close_price', 'open_price', 'high_price', 'low_price', 'volume', 'yahoo_ticker'))
                print("Loaded data to staging_price table successfully")
            except Exception as e:
                print(f"Error {e}")
                if conn:
                    conn.rollback()
                    return False



def load_to_price_table(conn, cur):
    if conn is None:
        raise Exception("Failed to connect to database")

    query = """
    INSERT INTO price (date, close_price, open_price, high_price, low_price, volume, asset_id)
    SELECT s.date, s.close_price, s.open_price, s.high_price, s.low_price, s.volume, a.asset_id 
    FROM staging_price s 
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
    query = """
    DELETE FROM staging_price;
    """
    try:
        cur.execute(query)
        print("Deleted records from staging_price table successfully.")
        logger.info("Deleted records from staging_price table successfully.")
        return True
    except Exception as e:
        print(f"Error {e}")
        logger.error("Failed to delete records from staging_price table.")
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
        load_to_staging_price_table(conn, cur, input_path)
        load_to_price_table(conn, cur)
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
    main(DB_CONFIG_PATH, STAGING_FOLDER)
