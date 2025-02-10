# this version is original "my version" 
import ccxt
import pandas as pd
import pymysql
import talib
import time
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import gc
from flatbuffers.packer import float32
import logging
import multiprocessing

logging.basicConfig(filename='***', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



# load environmental variables
load_dotenv()
BINANCE_API_KEY=os.getenv("***")
BINANCE_SECRET=os.getenv("***")



# connect the API of binance
binance=ccxt.binance({
    'apiKey': ***,
    'secret': ***,
    'enableRateLimit': True,
})



# load market data
market=binance.load_markets()
spot_symbols=[s for s in market.keys() if '/' in s and not ('UP/' in s or 'DOWN/' in s or 'PERP' in s or '-' in s)]
aim_spot_symbols = sorted(spot_symbols, key=lambda x: market[x]['info'].get('volume', 0), reverse=True)[:100]



# define a function to connect database and reconnect
def connect_database():
    return pymysql.connect(
        host=os.getenv('***'),
        user=os.getenv('***'),
        password=os.getenv('***'),
        database=os.getenv('***'),
        charset='utf8',
        autocommit=True
    )
database_connection = connect_database()
cursor = database_connection.cursor()




def keep_database_connection():
    global database_connection, cursor
    try:
        database_connection.ping(reconnect=True)
    except pymysql.OperationalError:
        print('🔄 Reconnecting to database...')
        database_connection = connect_database()
        cursor = database_connection.cursor()



# calculate financial indicators


# calculate VWAP(weighted average trade volume)
def calculate_VWAP(df):
    cumulative_volume = df['amount'].cumsum().replace(0, np.nan)
    cumulative_price_volume = (df['price'] * df['amount']).cumsum()
    return (cumulative_price_volume / cumulative_volume).fillna(0)


# detect large amount trade
def detect_large_trades(trades, threshold=50000):
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





# collect and save spot seconds data
def get_and_save_spot_seconds_data(symbol):
    try:
        trades = binance.fetch_trades(symbol)
        order_book = binance.fetch_order_book(symbol, limit=5)
        if not trades:
            return


        df = pd.DataFrame(trades)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['price'] = df['price'].astype(float32)
        df['amount'] = df['amount'].astype(float32)



        # calculate the high price and low price for ATR
        df['high'] = df['price'].rolling(window=5, min_periods=1).max().fillna(df['price'])
        df['low'] = df['price'].rolling(window=5, min_periods=1).min().fillna(df['price'])
        df['close'] = df['price']



        # if there are enough data then calculate the technical indicators
        if len(df) >= 10:
            
            # moving average line(every 5 and 10 seconds)
            df['SMA_5'] = talib.SMA(df['price'], timeperiod=5)
            df['EMA_5'] = talib.EMA(df['price'], timeperiod=5)
            df['SMA_10'] = talib.SMA(df['price'], timeperiod=10)
            df['EMA_10'] = talib.EMA(df['price'], timeperiod=10)

            # MACD
            df['MACD'], df['MACD_signal'], df['MACD_histogram'] = talib.MACD(df['price'], fastperiod=6, slowperiod=12,
                                                                             signalperiod=3)

            # RSI
            df['RSI_6'] = talib.RSI(df['price'], timeperiod=6)
            df['RSI_9'] = talib.RSI(df['price'], timeperiod=9)

            # bollinger
            df['bollinger_upper'], df['bollinger_middle'], df['bollinger_lower'] = talib.BBANDS(df['price'], timeperiod=10)

            # ATR
            df['ATR'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=5)

            # VWAP
            df['VWAP'] = calculate_VWAP(df)

            # OBV
            df['OBV'] = talib.OBV(df['close'], df['amount'])

        large_trade_info = detect_large_trades(trades)


        # get bid price and ask price and their volume
        bid_price = order_book['bids'][0][0] if order_book['bids'] else 0
        ask_price = order_book['asks'][0][0] if order_book['asks'] else 0
        bid_volume = order_book['bids'][0][1] if order_book['bids'] else 0
        ask_volume = order_book['asks'][0][1] if order_book['asks'] else 0

        # get latest data
        latest = df.iloc[-1]
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
        keep_database_connection()
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
        database_connection.commit()
        logging.info(f'✅ Data saved for {symbol}')

        if time.time() % 3600 < 1: 
            gc.collect()


    except ccxt.RateLimitExceeded:
        print(f'⏳ Rate limit exceeded for {symbol}. Retrying after cooldown...')
        time.sleep(20)
    except Exception as e:
        print(f'❌ Error with {symbol}: {e}')


# deal data with more threads

def run_data_collection():
    max_workers = multiprocessing.cpu_count() * 2
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            executor.map(get_and_save_spot_seconds_data, aim_spot_symbols)
            time.sleep(1)

if __name__ == '__main__':
    run_data_collection()








