
# get spot historical daily data

import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import pymysql
import os
import time
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB
from datetime import datetime, timedelta,timezone



# configure historical daily data collection log
logging.basicConfig(filename='',
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



# load environmental variables
load_dotenv()
# I use AWS_RDS
HOST = os.getenv('***')
USER=os.getenv('***')
PASSWORD=os.getenv('***')
DATABASE=os.getenv('***')

# binance API
BINANCE_KLINES_URL='https://api.binance.com/api/v3/klines'
BINANCE_EXCHANGE_URL='https://api.binance.com/api/v3/exchangeInfo'
API_TIMEOUT=5



# retry API connection
session = requests.Session()
retry=Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[ 500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retry))



# database connection pool
pool=PooledDB(
    creator=pymysql,
    maxconnections=8,
    mincached=2,
    maxcached=4,
    blocking=True,
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    charset='utf8',
    autocommit=True
)


# define database connection function
def get_database_connected():
    return pool.connection()

# use binance time
BINANCE_TIME_URL = "https://api.binance.com/api/v3/time"
def get_binance_server_time():
    try:
        response = requests.get(BINANCE_TIME_URL, timeout=API_TIMEOUT)
        if response.status_code == 200:
            server_time_ms = response.json().get('serverTime')
            return datetime.fromtimestamp(server_time_ms / 1000, tz=timezone.utc)
        else:
            logging.error(f"⚠️ fail to get server time : {response.text}")
            # if failed, use local time
            return datetime.now(timezone.utc)
    except Exception as e:
        logging.error(f"⚠️ unknown error : {e}")
        return datetime.now(timezone.utc)





# get aim symbols
def get_aim_symbols():
    try:
        response = session.get(BINANCE_EXCHANGE_URL, timeout=API_TIMEOUT)
        response.raise_for_status()
        exchange_symbols = response.json()
        all_symbols = [s['symbol'] for s in exchange_symbols['symbols'] if s['status'] == 'TRADING']

        # get binance server time and calculate timestamp
        binance_time = get_binance_server_time()
        start_date_short = (binance_time - timedelta(days=14)).strftime('%Y-%m-%d')
        start_date_long = (binance_time - timedelta(days=30)).strftime('%Y-%m-%d')

        # make two selection to select top 100 trading symbols
        # first selection
        volume_data_short = []
        for symbol in all_symbols:
            df = get_historical_klines(symbol, start_date_short)
            if df is not None and not df.empty:
                volume_data_short.append((symbol, df['quote_volume'].astype(float).mean()))
            time.sleep(0.25)

        sorted_symbols_short = sorted(volume_data_short, key=lambda x: x[1], reverse=True)
        top_200_symbols = [symbol[0] for symbol in sorted_symbols_short[:200]]

        # second selection
        volume_data_long = []
        for symbol in top_200_symbols:
            df = get_historical_klines(symbol, start_date_long)
            if df is not None and not df.empty:
                volume_data_long.append((symbol, df['quote_volume'].astype(float).mean()))
            time.sleep(0.25)

        sorted_symbols_long = sorted(volume_data_long, key=lambda x: x[1], reverse=True)
        return [symbol[0] for symbol in sorted_symbols_long[:100]]

    except requests.exceptions.RequestException as e:
        logging.error(f'⚠️ fail to fetch symbols : {e}')
        return []



# get historical klines data
# **********************************************************************************************

def get_historical_klines(symbol, start_date, interval='1d'):
    start_time = int(pd.Timestamp(start_date, tz='UTC').timestamp() * 1000)
    end_time = int(get_binance_server_time().timestamp() * 1000)
    all_data = []
    last_timestamp = None


    while start_time < end_time:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'limit': 1000
        }
        try:
            response = session.get(BINANCE_KLINES_URL, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            data = response.json()


            if not isinstance(data, list) or len(data) == 0:
                logging.warning(f"⚠️ {symbol} is empty or fail to fetch : {data}")
                break


            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades',
                'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'
            ])

            df.drop(columns=['close_time', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'],
                    inplace=True)

            df = df.astype({
                'open': float, 'high': float, 'low': float, 'close': float,
                'volume': float, 'quote_volume': float, 'trades': int
            })

            df['trade_date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date


            if last_timestamp and df.iloc[-1]['timestamp'] == last_timestamp:
                logging.warning(f"⚠️ {symbol} API return repeat data and break the loop")
                break

            if df.empty:
                logging.warning(f"⚠️ {symbol} is empty and break the loop")
                break

            all_data.append(df)
            last_timestamp = df.iloc[-1]['timestamp']


            interval_ms = {
                '1m': 60000, '3m': 180000, '5m': 300000, '15m': 900000,
                '30m': 1800000, '1h': 3600000, '2h': 7200000, '4h': 14400000,
                '6h': 21600000, '8h': 28800000, '12h': 43200000, '1d': 86400000
            }

            start_time = last_timestamp + interval_ms.get(interval, 86400000)

            logging.info(f"✅ {symbol} get {len(df)} rows data，all are {sum(len(d) for d in all_data)} rows")

            # API management
            headers = response.headers
            used_weight = int(headers.get('X-MBX-USED-WEIGHT-1M', 0))
            if used_weight > 1100:
                logging.warning("⏸️ API limitation, stop fpr 10 seconds")
                time.sleep(10)
            elif used_weight > 900:
                logging.warning("⏸️ API go on to the top limitation, stop for 5 seconds")
                time.sleep(5)
            else:
                time.sleep(0.2)


        except requests.exceptions.RequestException as e:
            logging.error(f"❌ {symbol} API fail to fetch : {e}")
            break
        except Exception as e:
            logging.error(f"❌ {symbol} data process error : {e}")
            break

    return pd.concat(all_data, ignore_index=True) if all_data else None


# calculate the financial indicators
def calculate_financial_indicators(df):

    # SMA and EMA
    for period in [5,10,20,50,100,200]:
        df[f'SMA_{period}']=df['close'].rolling(period,min_periods=period).mean()
        df[f'EMA_{period}']=df['close'].ewm(span=period,min_periods=period,adjust=False).mean()

    # volume moving averages
    for period in [5,10,20]:
        df[f'volume_MA_{period}']=df['volume'].rolling(window=period,min_periods=period).mean()


    # RSI
    delta=df['close'].diff()
    gain=delta.clip(lower=0).rolling(window=14,min_periods=14).mean()
    loss=(-delta.clip(upper=0)).rolling(window=14,min_periods=14).mean()

    # in case of dividing "0" so plus "1e-10"
    rs=gain/(loss+1e-10)
    df['RSI_14']=100-(100/(1+rs))

    # VWAP
    typical_price=(df['high']+df['low']+df['close'])/3
    df['cumulative_typical_price_volume']=(typical_price*df['volume']).cumsum()
    df['cumulative_volume']=df['volume'].cumsum()
    df['VWAP']=df['cumulative_typical_price_volume']/df['cumulative_volume']


    # OBV
    df['OBV']=(np.sign(df['close'].diff())*df['volume']).fillna(0).cumsum()


    # MACD
    df['EMA_12']=df['close'].ewm(span=12,adjust=False).mean()
    df['EMA_26']=df['close'].ewm(span=26,adjust=False).mean()
    df['MACD']=df['EMA_12']-df['EMA_26']
    df['MACD_single']=df['MACD'].ewm(span=9,adjust=False).mean()

    # KDJ
    df['lowest_low_14']=df['low'].rolling(window=14,min_periods=14).min()
    df['highest_high_14']=df['high'].rolling(window=14,min_periods=14).max()
    df['RSV']=(df['close']-df['lowest_low_14'])/(df['highest_high_14']-df['lowest_low_14']+1e-10)*100
    df['K']=df['RSV'].ewm(com=2,adjust=False,min_periods=14).mean()
    df['D']=df['K'].ewm(com=2,adjust=False,min_periods=14).mean()
    df['J']=3*df['K']-2*df['D']

    # ATR
    tr1=df['high']-df['low']
    tr2=(df['high']-df['close'].shift(1)).abs()
    tr3=(df['low']-df['close'].shift(1)).abs()
    df['TR']=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    df['ATR']=df['TR'].rolling(window=14,min_periods=14).mean()

    # Bollinger bands
    df['bollinger_middle']=df['SMA_20']
    rolling_std=df['close'].rolling(window=20,min_periods=20).std()
    df['bollinger_upper']=df['bollinger_middle']+(rolling_std*2)
    df['bollinger_lower']=df['bollinger_middle']-(rolling_std*2)

    # Risk management indicators___MDD and sharpe_ratio
    df['max_drawdown'] = (df['close'] / df['close'].cummax()) - 1
    # sharpe ratio
    df['return']=df['close'].pct_change()
    df['rolling_mean_return']=df['return'].rolling(window=20,min_periods=20).mean()
    df['rolling_std_return']=df['return'].rolling(window=20,min_periods=20).std()
    df['sharpe_ratio']=df['rolling_mean_return']/(df['rolling_std_return']+1e-10)

    df.replace({np.nan: None}, inplace=True)
    return df


# save data into database
def save_data_into_database(df, symbol):

    if df is None or df.empty:
        logging.warning(f"{symbol} is empty, skip saving")
        return
    connection=None
    try:
        connection=get_database_connected()
        with connection.cursor() as cursor:
            # save data before saving data
            cursor.execute("SELECT COUNT(*) FROM *** WHERE symbol = %s", (symbol,))
            before_count = cursor.fetchone()[0]
            logging.info(f"Before saving {symbol} there are already {before_count} rows data")
            sql = """
                INSERT INTO *** (
                    symbol, trade_date, timestamp,open, high, low, close, volume, quote_volume, trades,
                    SMA_5, SMA_10, SMA_20, SMA_50, SMA_100, SMA_200,
                    EMA_5, EMA_10, EMA_20, EMA_50, EMA_100, EMA_200,
                    volume_MA_5, volume_MA_10, volume_MA_20, 
                    RSI_14, VWAP, OBV, 
                    MACD, MACD_single, K, D, J, ATR, 
                    bollinger_middle, bollinger_upper, bollinger_lower,
                    max_drawdown, sharpe_ratio
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    quote_volume = VALUES(quote_volume),
                    trades = VALUES(trades),
                    SMA_5 = VALUES(SMA_5),
                    SMA_10 = VALUES(SMA_10),
                    SMA_20 = VALUES(SMA_20),
                    SMA_50 = VALUES(SMA_50),
                    SMA_100 = VALUES(SMA_100),
                    SMA_200 = VALUES(SMA_200),
                    EMA_5 = VALUES(EMA_5),
                    EMA_10 = VALUES(EMA_10),
                    EMA_20 = VALUES(EMA_20),
                    EMA_50 = VALUES(EMA_50),
                    EMA_100 = VALUES(EMA_100),
                    EMA_200 = VALUES(EMA_200),
                    volume_MA_5 = VALUES(volume_MA_5),
                    volume_MA_10 = VALUES(volume_MA_10),
                    volume_MA_20 = VALUES(volume_MA_20),
                    RSI_14 = VALUES(RSI_14),
                    VWAP = VALUES(VWAP),
                    OBV = VALUES(OBV),
                    MACD = VALUES(MACD),
                    MACD_single = VALUES(MACD_single),
                    K = VALUES(K),
                    D = VALUES(D),
                    J = VALUES(J),
                    ATR = VALUES(ATR),
                    bollinger_middle = VALUES(bollinger_middle),
                    bollinger_upper = VALUES(bollinger_upper),
                    bollinger_lower = VALUES(bollinger_lower),
                    max_drawdown = VALUES(max_drawdown),
                    sharpe_ratio = VALUES(sharpe_ratio)
            """
            values=[(
                symbol,row['trade_date'],row['timestamp'],
                    row['open'], row['high'], row['low'], row['close'],
                    row['volume'], row['quote_volume'], row['trades'],
                    row['SMA_5'], row['SMA_10'], row['SMA_20'], row['SMA_50'], row['SMA_100'], row['SMA_200'],
                    row['EMA_5'], row['EMA_10'], row['EMA_20'], row['EMA_50'], row['EMA_100'], row['EMA_200'],
                    row['volume_MA_5'], row['volume_MA_10'], row['volume_MA_20'],
                    row['RSI_14'], row['VWAP'], row['OBV'],
                    row['MACD'], row['MACD_single'],
                    row['K'], row['D'], row['J'],
                    row['ATR'], row['bollinger_middle'], row['bollinger_upper'], row['bollinger_lower'],
                    row['max_drawdown'], row['sharpe_ratio']
                )for _, row in df.iterrows()]
            cursor.executemany(sql, values)
            connection.commit()
            # check data rows after saving
            cursor.execute("SELECT COUNT(*) FROM *** WHERE symbol = %s", (symbol,))
            after_count = cursor.fetchone()[0]
            logging.info(f"{symbol} after saving data, the rows of data are {after_count}")
        logging.info(f"{symbol} data saved successfully.")
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"{symbol} failed to save data:{e}")
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    start_date='**'
    symbols = get_aim_symbols()
    for symbol in symbols:
        df = get_historical_klines(symbol,start_date)
        if df is not None:
            df = calculate_financial_indicators(df)
            save_data_into_database(df, symbol)



















