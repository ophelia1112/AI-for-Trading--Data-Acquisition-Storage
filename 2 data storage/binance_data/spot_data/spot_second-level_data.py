import ccxt
import pandas as pd
import pymysql
from dbutils.pooled_db import PooledDB
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
import threading
import multiprocessing

logging.basicConfig(filename='***',level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

load_dotenv()
BINANCE_API_KEY=os.getenv('***')
BINANCE_KEY=os.getenv('***')

binance=ccxt.binance({
    'apiKey': ***,
    'secret': ***,
    'enableRateLimit': True,
    'timeout':7000
})

market=binance.load_markets()
spot_symbols=[s for s in market.keys() if '/' in s and not ('UP/' in s or 'DOWN/' in s or 'PERP' in s or '-' in s)]

aim_spot_symbols=sorted(spot_symbols,key=lambda x:market[x]['info'].get('volume',0),reverse=True)[:100]

# database configuration
# configure database connection pool(no need to create function for database connection and reconnection)
# increase the speed of reconnection
# connection.close() let the connection back to the pool in case of exhaustion of the database source
POOL=PooledDB(
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
    cumulative_volume=df['amount'].cumsum().replace(0,np.nan)
    cumulative_price_volume=(df['price']*df['amount']).cumsum()
    return (cumulative_price_volume/cumulative_volume).fillna(0)

def detect_large_trades(trades, symbol):
    high_volume_symbols = ['BTC/USDT', 'ETH/USDT']
    threshold = 100000 if symbol in high_volume_symbols else 10000

    large_trades = [trade for trade in trades if trade['amount'] * trade['price'] > threshold]
    large_buy_volume = sum(trade['amount'] for trade in large_trades if trade.get('side') == 'buy')
    large_sell_volume = sum(trade['amount'] for trade in large_trades if trade.get('side') == 'sell')
    return {
        'large_trade_count': len(large_trades),
        'large_trade_volume': sum(trade['amount'] for trade in large_trades),
        'large_trade_value': sum(trade['amount'] * trade['price'] for trade in large_trades),
        'large_buy_volume': large_buy_volume,
        'large_sell_volume': large_sell_volume
    }

def API_reconnect(fetch_function,*args,retries=3,delay=5,**kwargs):
    for attempt in range(retries):
        try:
            return fetch_function(*args,**kwargs)
        except ccxt.RateLimitExceeded as e:
            logging.warning(f'API rate limitation, waiting for 10 seconds')
            time.sleep(10)
        except ccxt.NetworkError as e:
            logging.warning(f'Network error: {e},retrying ({attempt+1}/{retries})')
            time.sleep(delay)
        except Exception as e:
            logging.warning(f'unkonwn error: {e}')
            time.sleep(delay)
    return None

def get_and_save_spot_seconds_data(symbol):
    connection = POOL.connection()
    cursor = connection.cursor()
    try:
        trades=API_reconnect(binance.fetch_trades,symbol)
        order_book=API_reconnect(binance.fetch_order_book,symbol,limit=5)
        if not trades or not order_book:
            return
        df=pd.DataFrame(trades)
        df['timestamp']=pd.to_datetime(df['timestamp'],unit='ms')
        df['price']=df['price'].astype(np.float32)
        df['amount']=df['amount'].astype(np.float32)

        current_time = pd.Timestamp.utcnow()

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        df = df[df['timestamp'] >= current_time - pd.Timedelta(days=1)]

        if df.empty:
            logging.warning(f'⚠️ No recent data for {symbol}, skipping...')
            return

        df['high']=df['price'].rolling(window=5,min_periods=1).max()
        df['low']=df['price'].rolling(window=5,min_periods=1).min()
        df['close']=df['price']

        if len(df)>=10:

            df['SMA_5']=talib.SMA(df['price'],timeperiod=5)
            df['EMA_5']=talib.EMA(df['price'],timeperiod=5)
            df['SMA_10']=talib.SMA(df['price'],timeperiod=10)
            df['EMA_10']=talib.EMA(df['price'],timeperiod=10)
            df['MACD'],df['MACD_signal'],df['MACD_histogram']=talib.MACD(df['price'],fastperiod=6,slowperiod=12,signalperiod=3)
            df['RSI_6'] = talib.RSI(df['price'], timeperiod=6).clip(1, 99)
            df['RSI_9'] = talib.RSI(df['price'], timeperiod=9).clip(1, 99)
            df['bollinger_upper'],df['bollinger_middle'],df['bollinger_lower']=talib.BBANDS(df['price'],timeperiod=10)
            df['ATR']=talib.ATR(df['high'],df['low'],df['close'],timeperiod=5)
            df['VWAP']=calculate_VWAP(df)
            df['OBV']=talib.OBV(df['close'],df['amount'])
            large_trade_info=detect_large_trades(trades,symbol)
            bid_price=order_book['bids'][0][0] if order_book['bids'] else None
            ask_price=order_book['asks'][0][0] if order_book['asks'] else None
            bid_volume=order_book['bids'][0][1] if order_book['bids'] else None
            ask_volume=order_book['asks'][0][1] if order_book['asks'] else None

            if bid_price is None or ask_price is None:
                logging.warning(f'⚠️ Missing bid/ask data for {symbol}, skipping this entry.')
                return

            latest=df.iloc[-1]
            latest_timestamp = latest['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            trade_count = len(trades)

            data = (
                symbol, latest_timestamp, latest['price'], latest['amount'], trade_count,
                bid_price, ask_price, bid_volume, ask_volume,
                latest.get('SMA_5', 0), latest.get('EMA_5', 0),
                latest.get('SMA_10', 0), latest.get('EMA_10', 0),
                latest.get('MACD', 0), latest.get('MACD_signal', 0), latest.get('MACD_histogram', 0),
                latest.get('RSI_6', 0), latest.get('RSI_9', 0),
                latest.get('bollinger_upper', 0), latest.get('bollinger_middle', 0), latest.get('bollinger_lower', 0),
                latest.get('ATR', 0),
                latest.get('VWAP', 0), latest.get('OBV', 0),
                large_trade_info['large_trade_count'],
                large_trade_info['large_trade_volume'],
                large_trade_info['large_trade_value'],
                large_trade_info['large_buy_volume'],
                large_trade_info['large_sell_volume']
            )
            # insert the table in mysql terminal 
            sql = """
                INSERT INTO *** (
                    symbol, timestamp, price, volume, trade_count,
                    bid_price, ask_price, bid_volume, ask_volume,
                    SMA_5, EMA_5, SMA_10, EMA_10,
                    MACD, MACD_signal, MACD_histogram,
                    RSI_6, RSI_9,
                    bollinger_upper, bollinger_middle, bollinger_lower,
                    ATR, VWAP, OBV,
                    large_trade_count, large_trade_volume, large_trade_value,
                    large_buy_volume, large_sell_volume
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                volume = VALUES(volume),
                trade_count = VALUES(trade_count),
                large_trade_count = VALUES(large_trade_count),
                large_trade_volume = VALUES(large_trade_volume),
                large_trade_value = VALUES(large_trade_value),
                large_buy_volume = VALUES(large_buy_volume),
                large_sell_volume = VALUES(large_sell_volume);
                """
            cursor.execute(sql, data)
            connection.commit()
            logging.info(f'✅ Data saved for {symbol}')

    except Exception as e:
        logging.error(f'❌ data error：{e}')
    finally:
        cursor.close()
        connection.close()
        gc.collect()


def run_data_collection():
    max_workers = 8
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            futures = [executor.submit(get_and_save_spot_seconds_data, symbol) for symbol in aim_spot_symbols]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f'❌ 异常捕获：{e}')
            time.sleep(0.5)


if __name__ == '__main__':
    run_data_collection()



















