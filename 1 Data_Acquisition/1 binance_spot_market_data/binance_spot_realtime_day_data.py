# get real time day data

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

# configure the log
logging.basicConfig(filename='***',level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



# load environmental variables to connect EC2 terminal
load_dotenv()
AWS_RDS_HOST=os.getenv('***')
AWS_RDS_USER=os.getenv('***')
AWS_RDS_PASSWORD=os.getenv('***')
AWS_RDS_DATABASE=os.getenv('***')



# binance API
# this API for the acquisition for symbols and sort them for the first 100 symbols
BINANCE_TICKER_URL_FOR_SPOT="https://api.binance.com/api/v3/ticker/24hr"
# this API for data collection
BINANCE_TICKER_URL_FOR_DATA="https://api.binance.com/api/v3/klines"
API_TIMEOUT=5

# retry the API connection
session = requests.Session()
retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))




# establish a connection to connect with EC2 terminal's mysql
pool=PooledDB(
    creator=pymysql, # use pymysql to be the activator
    maxconnections=10, # max connection amount
    mincached=3, # the amount of min connection amount when activated
    maxcached=6, # the max amount of connection in pool when pool is not busy
    blocking=True, # if the pool is full if we should wait
    host=AWS_RDS_HOST,
    user=AWS_RDS_USER,
    password=AWS_RDS_PASSWORD,
    database=AWS_RDS_DATABASE,
    charset='utf8',
    autocommit=True

)
def get_database_connected():
    return pool.connection()



# get all the symbols in spot market and sort them using trading amount then get the top 100 symbols
def get_aim_symbols(limit=100):
    try:
        response=session.get(BINANCE_TICKER_URL_FOR_SPOT, timeout=API_TIMEOUT)
        tickers=response.json()
        if not isinstance(tickers, list):
            logging.error(f'error to return {tickers}')

        sorted_tickers=sorted(tickers,key=lambda x: float(x.get('quoteVolume',0)),reverse=True)
        return [ticker['symbol'] for ticker in sorted_tickers[:limit]]
    except (requests.RequestException,ValueError) as e:
        logging.error(f'fail to get the data: {e}')
        return []


# get spot market real time day line data
def get_daily_klines(symbol,interval='1d',limit=1):
    try:
        params={
            'symbol':symbol,
            'interval':interval,
            'limit':limit
        }
        response=session.get(BINANCE_TICKER_URL_FOR_DATA,params=params,timeout=API_TIMEOUT)
        data=response.json()

        if not isinstance(data,list):
            logging.error(f'fail to save {symbol}:{data}')
            return []

        df=pd.DataFrame(data,columns=[
            'timestamp',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'close_time',
            'quote_volume',
            'trades',
            'takerBuyBaseAssetVolume',
            'takerBuyQuoteAssetVolume',
            'ignore'
        ])
        # convert data types
        df=df.astype({
            'open':'float',
            'high':'float',
            'low':'float',
            'close':'float',
            'volume':'float',
            'quote_volume':'float',
            'trades':'int',
            'takerBuyBaseAssetVolume':'float',
            'takerBuyQuoteAssetVolume':'float'

        })

        # convert timestamp to date
        df['trade_date']=pd.to_datetime(df['timestamp'],unit='ms').dt.date
        df.drop(columns=['timestamp','close_time','ignore'],inplace=True)
        required_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades','takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume']
        if df[required_columns].isna().any().any():
            logging.warning(f"{symbol} contains NaN，skipping")
            return None
        return df


    except requests.RequestException as e:
        logging.error(f'fail to get the {symbol}:{e}')
        return None



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
    if df.empty:
        logging.warning(f"{symbol} is empty, skipping.")
        return
    connection=None
    try:
        connection=get_database_connected()

        with connection.cursor() as cursor:
            sql = """
                INSERT INTO *** (
                    symbol, trade_date, open, high, low, close, volume, quote_volume, trades,
                    takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume,
                    SMA_5, SMA_10, SMA_20, SMA_50, SMA_100, SMA_200,
                    EMA_5, EMA_10, EMA_20, EMA_50, EMA_100, EMA_200,
                    volume_MA_5, volume_MA_10, volume_MA_20, 
                    RSI_14, VWAP, OBV, 
                    MACD, MACD_single, K, D, J, ATR, 
                    bollinger_middle, bollinger_upper, bollinger_lower,
                    max_drawdown, sharpe_ratio
                ) VALUES (
                    %s, %s, %s, 
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
                    takerBuyBaseAssetVolume = VALUES(takerBuyBaseAssetVolume),
                    takerBuyQuoteAssetVolume = VALUES(takerBuyQuoteAssetVolume),
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
                symbol,row['trade_date'],
                    row['open'], row['high'], row['low'], row['close'],
                    row['volume'], row['quote_volume'], row['trades'],
                    row['takerBuyBaseAssetVolume'], row['takerBuyQuoteAssetVolume'],
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
        logging.info(f"{symbol} data saved successfully.")
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"{symbol} failed to save data:{e}")
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    symbols = get_aim_symbols()
    for symbol in symbols:
        df = get_daily_klines(symbol)
        if df is not None:
            df = calculate_financial_indicators(df)
            save_data_into_database(df, symbol)












