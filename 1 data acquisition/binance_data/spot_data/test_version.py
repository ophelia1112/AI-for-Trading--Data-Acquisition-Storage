# this version can also be used but can be done some revision
import ccxt
import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB
import talib
import time
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
import os
import logging
import gc
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(filename='***', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
BINANCE_API_KEY = os.getenv("***")
BINANCE_SECRET = os.getenv("***")


binance = ccxt.binance({
    'apiKey': ***,
    'secret': ***,
    'enableRateLimit': True,
    'timeout': 15000
})


market = binance.load_markets()
spot_symbols = [s for s in market.keys() if '/' in s and not ('UP/' in s or 'DOWN/' in s or 'PERP' in s or '-' in s)]
aim_spot_symbols = sorted(spot_symbols, key=lambda x: market[x]['info'].get('volume', 0), reverse=True)[:100]


POOL = PooledDB(
    creator=pymysql,
    maxconnections=10, 
    mincached=5,
    blocking=True,
    host=os.getenv('***'),
    user=os.getenv('***'),
    password=os.getenv('***'),
    database=os.getenv('***'),
    charset='utf8'
)

def calculate_VWAP(df):
    cumulative_volume = df['amount'].cumsum().replace(0, np.nan)
    cumulative_price_volume = (df['price'] * df['amount']).cumsum()
    return (cumulative_price_volume / cumulative_volume).fillna(0)

def detect_large_trades(trades, threshold=50000):
    large_trades = [trade for trade in trades if trade['amount'] * trade['price'] > threshold]
    return {
        'large_trade_count': len(large_trades),
        'large_trade_volume': sum(trade['amount'] for trade in large_trades),
        'large_trade_value': sum(trade['amount'] * trade['price'] for trade in large_trades)
    }



def fetch_with_retry(fetch_function, *args, retries=3, delay=5, **kwargs):
    for attempt in range(retries):
        try:
            return fetch_function(*args, **kwargs)
        except ccxt.RateLimitExceeded:
            logging.warning('API limitation, waiting 10 seconds...')
            time.sleep(10)
        except ccxt.NetworkError as e:
            logging.warning(f'web error：{e}，retrying ({attempt + 1}/{retries})...')
            time.sleep(delay)
        except Exception as e:
            logging.error(f'unknown error：{e}')
            time.sleep(delay)
    return None

def get_and_save_spot_seconds_data(symbol):
    connection = POOL.connection()
    cursor = connection.cursor()
    try:
        trades = fetch_with_retry(binance.fetch_trades, symbol)
        order_book = fetch_with_retry(binance.fetch_order_book, symbol, limit=5)

        if not trades or not order_book:
            return

        df = pd.DataFrame(trades)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['price'] = df['price'].astype(np.float32)
        df['amount'] = df['amount'].astype(np.float32)

        df['high'] = df['price'].rolling(window=5, min_periods=1).max()
        df['low'] = df['price'].rolling(window=5, min_periods=1).min()
        df['close'] = df['price']

        if len(df) >= 10:
            df['SMA_5'] = talib.SMA(df['price'], timeperiod=5)
            df['EMA_5'] = talib.EMA(df['price'], timeperiod=5)

            df['MACD'], df['MACD_signal'], df['MACD_histogram'] = talib.MACD(df['price'])

            df['RSI_6'] = talib.RSI(df['price'], timeperiod=6)

            df['VWAP'] = calculate_VWAP(df)

        large_trade_info = detect_large_trades(trades)
        latest = df.iloc[-1]

        data = (
            symbol, latest['timestamp'].strftime('%Y-%m-%d %H:%M:%S'), latest['price'], latest['amount'],
            order_book['bids'][0][0] if order_book['bids'] else 0,
            order_book['asks'][0][0] if order_book['asks'] else 0,
            latest.get('SMA_5', 0), latest.get('EMA_5', 0),
            latest.get('MACD', 0), latest.get('RSI_6', 0),
            latest.get('VWAP', 0),
            large_trade_info['large_trade_count'],
            large_trade_info['large_trade_volume'],
            large_trade_info['large_trade_value']
        )

        sql = """
            INSERT INTO spot_seconds_data (
                symbol, timestamp, price, volume,
                bid_price, ask_price,
                SMA_5, EMA_5, MACD, RSI_6, VWAP,
                large_trade_count, large_trade_volume, large_trade_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                volume = VALUES(volume),
                large_trade_value = VALUES(large_trade_value);
        """

        cursor.execute(sql, data)
        connection.commit()

        logging.info(f'✅ Data saved for {symbol}')

    except Exception as e:
        logging.error(f'❌ data processing error：{e}')
    finally:
        cursor.close()
        connection.close()
        gc.collect()

# multiple process
def run_data_collection():
    max_workers = 8  
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            futures = [executor.submit(get_and_save_spot_seconds_data, symbol) for symbol in aim_spot_symbols]
            for future in as_completed(futures):
                try:
                    future.result()  
                except Exception as e:
                    logging.error(f'❌ error catch：{e}')
            time.sleep(0.5) 

if __name__ == '__main__':
    run_data_collection()
