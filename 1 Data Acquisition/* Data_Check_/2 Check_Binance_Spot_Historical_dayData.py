import logging
import pymysql
import pandas as pd
import os
import time
from dotenv import load_dotenv
import requests


logging.basicConfig(
    filename='***',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


load_dotenv()
AWS_RDS_HOST = os.getenv('***')
AWS_RDS_USER = os.getenv('***')
AWS_RDS_PASSWORD = os.getenv('***')
AWS_RDS_DATABASE = os.getenv('***')


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


TOLERANCE_DICT = {
    'close': 0.1,
    'volume': 2.0,
    'quote_volume': 2.0,
    'trades': 2.0,
}


def connect_database():
    try:
        return pymysql.connect(
            host=AWS_RDS_HOST,
            user=AWS_RDS_USER,
            password=AWS_RDS_PASSWORD,
            database=AWS_RDS_DATABASE,
            charset='utf8',
            autocommit=True
        )
    except Exception as e:
        logging.error(f"❌ fail to connect database: {e}")
        return None


def get_symbols_time_range():
    connection = connect_database()
    if not connection:
        return {}

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT symbol, MIN(timestamp) AS start_ts, MAX(timestamp) AS end_ts
                FROM ***
                GROUP BY symbol
            """)
            data = cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ fail to read data from database: {e}")
        return {}
    finally:
        connection.close()

    return {row[0]: (row[1], row[2]) for row in data}


def get_data_from_database(symbol, start_ts, end_ts):
    connection = connect_database()
    if not connection:
        return pd.DataFrame()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, quote_volume, trades
                FROM ***
                WHERE symbol = %s AND timestamp BETWEEN %s AND %s
                ORDER BY timestamp ASC
            """, (symbol, start_ts, end_ts))
            data = cursor.fetchall()
    except Exception as e:
        logging.error(f"❌ get {symbol} failed : {e}")
        return pd.DataFrame()
    finally:
        connection.close()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades'
    ])


    df = df.astype({
        'timestamp': 'int',
        'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
        'volume': 'float', 'quote_volume': 'float', 'trades': 'int'
    })

    return df


def get_binance_API_data(symbol, start_ts, end_ts):
    params = {
        'symbol': symbol,
        'interval': '1d',
        'startTime': start_ts,
        'endTime': end_ts,
        'limit': 1000
    }

    try:
        response = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"⚠️ Binance API failed to get : {symbol} | {e}")
        return None

    data = response.json()
    if not isinstance(data, list) or len(data) == 0:
        logging.warning(f"⚠️ Binance API return empty data: {symbol}")
        return None

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades',
        'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'
    ])
    df.drop(columns=['close_time', 'ignore', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume'], inplace=True)
    df = df.astype({
        'timestamp': 'int',
        'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
        'volume': 'float', 'quote_volume': 'float', 'trades': 'int'
    })

    return df


def check_symbols(symbol, start_ts, end_ts):
    logging.info(f'🔍 checking {symbol} data')


    database_data = get_data_from_database(symbol, start_ts, end_ts)
    if database_data.empty:
        logging.warning(f'⚠️  {symbol} does not in database')
        return


    binance_data = get_binance_API_data(symbol, start_ts, end_ts)
    if binance_data is None or binance_data.empty:
        logging.warning(f'⚠️ Binance return {symbol} data none')
        return
    else:
        logging.info(f'✅ {symbol} Binance get data successfully')

    merged = database_data.merge(binance_data, on='timestamp', suffixes=('_db', '_api'))
    if merged.empty:
        logging.error(f'❌ {symbol} can not match timestamp and they can not be combained')
        return

    logging.info(f'✅ {symbol} timestamp matched successfully and beginning check data')


    all_good = True
    for col, tolerance in TOLERANCE_DICT.items():
        merged[f'{col}_diff'] = (merged[f"{col}_db"] - merged[f"{col}_api"]).abs()
        merged[f'{col}_error_rate'] = merged.apply(
            lambda row: (row[f'{col}_diff'] / row[f'{col}_api']) * 100 if row[f'{col}_api'] != 0 else 0, axis=1
        )

        inconsistent_rows = merged[merged[f'{col}_error_rate'] > tolerance]
        if not inconsistent_rows.empty:
            logging.warning(f'⚠️ {symbol}: {col} the error is too big, and the number of error data is {len(inconsistent_rows)}')
            all_good = False


    if all_good:
        logging.info(f'✅ {symbol} all data are checked well')


if __name__ == "__main__":
    symbols_time_range = get_symbols_time_range()


    for symbol, (start_ts, end_ts) in symbols_time_range.items():
        check_symbols(symbol, start_ts, end_ts)
        time.sleep(0.2)

