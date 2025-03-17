import pandas as pd
from io import StringIO
from pathlib import Path
from utils.db_connection import connect_db
from utils.logging_config import logger

logger.info("This script started running.")

# Dynamically determine the base directory (root of the project)
BASE_DIR = Path(__file__).resolve().parent.parent

# TODO download transaction data from Degiro and manually put it in data dir

# TODO When Degiro offer official API, turn this into a batch ETL

INPUT_PATH = BASE_DIR / "data" / "Transactions.csv"

# Define the directories or paths of the following
LOG_CONFIG_PATH = BASE_DIR / "config" / "logging_config.json"
LOG_PATH = BASE_DIR / "logs" / "etl.log"
LOG_DIR = BASE_DIR / "logs"
DB_CONFIG_PATH = BASE_DIR / "config" / "db_config.json"


def clean_transaction(transaction_file_path):
    df = pd.read_csv(transaction_file_path)

    # Rename columns. Rename unnecceasry columns as 'extracol'
    df.columns = ['date', 'time', 'extracol1', 'isin', 'extracol2', 'extracol3', 'quantity', 'price', 'extracol4', 'extracol5', 'extracol6', 'value', 'extracol7', 'extracol8', 'fee', 'extracol9', 'extracol10', 'extracol11', 'extracol12']
    df.drop(columns=[col for col in df.columns if "extracol" in col], inplace=True)

    # Make value in fee absolute number
    df['fee'] = df['fee'].apply(lambda x: abs(x))

    # Drop rows with missing values
    df.dropna(inplace=True)

    print('Rename and drop columns successfully')
    logger.info(f"Rename and drop columns successfully")

    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
    print('convert date successfully')
    logger.info(f"Convert date successfully")
    return df

def load_to_staging_transaction_table(conn, cur, dataframe):
    if conn is None:
        raise Exception("Failed to connect to database")

    create_staging_table_query = """
        CREATE TABLE IF NOT EXISTS staging_transaction (
            id SERIAL PRIMARY KEY, 
            date DATE NOT NULL, 
            time TIME WITHOUT TIME ZONE NOT NULL,
            isin VARCHAR(12), 
            quantity INT NOT NULL, 
            price NUMERIC(10,2) NOT NULL, 
            value numeric(12,2) NOT NULL, 
            fee numeric(6,2)
            );
        """
    logger.info(f"Loading transactions into staging table")
    print(f"Loading transactions into staging table.")
    try:
        cur.execute(create_staging_table_query)
        print("The staging_transaction table is created or already created")
        output = StringIO()
        dataframe.to_csv(output, sep='\t', index=False, header=False)
        output.seek(0)
        cur.copy_from(output, "staging_transaction", null="NULL", columns=('date', 'time', 'isin', 'quantity', 'price', 'value', 'fee'))
        print('Insert csv data to staging table successfully')
        conn.commit()
    except Exception as e:
        print(f"Error {e}")
        conn.rollback()

def load_to_transaction_table(conn, cur):
    if conn is None:
        print("failed to connect to database")
        logger.info(f"failed to connect to database")
        return
    query = """
            INSERT INTO transaction (date, time, quantity, price, value, fee, asset_id)
            SELECT s.date, s.time, s.quantity, s.price, s.value, s.fee, a.asset_id
            FROM staging_transaction s 
            JOIN asset a ON s.isin = a.isin
            WHERE NOT EXISTS ( 
                SELECT 1 
                FROM transaction t
                WHERE t.date = s.date
                AND t.time = s.time
                AND t.quantity = s.quantity 
                AND t.price = s.price 
                AND t.value = s.value 
                AND t.fee = s.fee 
            )
            And a.asset_id IS NOT NULL;
            """

    try:
        cur.execute(query)
        conn.commit()
        print("Inserted new records into transaction table successfully.")
    except Exception as e:
        print(f"Error {e}")
        conn.rollback()

def delete_rows_staging(conn, cur):
    if conn is None:
        return

    query = """
        DELETE FROM staging_transaction;
        """
    try:
        cur.execute(query)
        conn.commit()
        print("Cleared staging table successfully.")
    except Exception as e:
        print(f"Error {e}")
        conn.rollback()

def main(raw_transaction_file_path):
    conn, cur = connect_db()
    if conn is None:
        print("Failed to connect to database. Exiting.")
        logger.error("Failed to connect to database. Exiting.")
        return
    try:
        df = clean_transaction(raw_transaction_file_path)
        load_to_staging_transaction_table(conn, cur, df)
        load_to_transaction_table(conn, cur)
        delete_rows_staging(conn, cur)
    except Exception as e:
        print(f"Error failed to load transactions to db: {e}")
        logger.error(f"Error failed to load transactions to db: {e}")
        conn.rollback()

