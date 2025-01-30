# data acquisition
import ccxt
import pandas as pd
import os
from dotenv import load_dotenv
import time
import pymysql
import talib
import random
import numpy as np

time.sleep(random.uniform(0.5, 1.5))
# Load .env documents to get the key and secret
load_dotenv()
BINANCE_API_KEY = os.getenv("***")
BINANCE_API_SECRET = os.getenv("***")

# Initialize Binance
binance_interaction = ccxt.binance({
    "apiKey": '***',
    "secret": '***',
    'options': {'adjustForTimeDifference': True},
    'enableRateLimit': True
})
binance_markets=binance_interaction.load_markets()

# get all the trade symbols
all_symbols=list(binance_markets.keys())

# sort the trade symbols and select the aim symbols
spotTrade_pairs = [s for s in all_symbols if '/' in s and not ('UP/' in s or 'DOWN/' in s)]
futuresTrade_pairs=[s for s in all_symbols if 'PERP' in s]
etfTrade_pairs = [s for s in all_symbols if s.endswith('UP/USDT') or s.endswith('DOWN/USDT')]
optionTrade_pairs = [s for s in all_symbols if '-' in s and (s.endswith('C') or s.endswith('P'))]

# sort the pairs according to the trade amount
def sort_by_trade_volume(symbols):
    return sorted(symbols, key=lambda x: binance_markets[x]['info'].get('volume',0), reverse=True)[:100]

top_spotTrade_pairs=sort_by_trade_volume(spotTrade_pairs)
top_futureTrade_pairs=sort_by_trade_volume(futuresTrade_pairs)
top_etfTrade_pairs=sort_by_trade_volume(etfTrade_pairs)
top_optionTrade_pairs=sort_by_trade_volume(optionTrade_pairs)

# deduplication
# get the final trade symbols
symbols=list(set(top_spotTrade_pairs+top_futureTrade_pairs+top_etfTrade_pairs+top_optionTrade_pairs))

# Config and connect database
database_connection = pymysql.connect(
    # all information from your cloud storage account
    host=os.getenv("***"),
    user=os.getenv("***"),
    password=os.getenv("***"),
    database=os.getenv("***"),
    charset='utf8mb4',
    autocommit=True
)
cursor = database_connection.cursor()


# Get the new trend of the trade
def get_market_trade_data():
    for symbol in symbols:
        try:
            if symbol not in binance_interaction.symbols:
                continue
            # Get the trend of the share
            ticker = binance_interaction.fetch_ticker(symbol)
            order_book=binance_interaction.fetch_order_book(symbol)

            # calculate moving average
            ohlcv=binance_interaction.fetch_ohlcv(symbol, timeframe='1d',limit=50)
            df=pd.DataFrame(ohlcv,columns=['timestamp','open','high','low','close','volume'])

            if len(df)>=14:
                # calculate the technical indicators
                df['rsi_14']=talib.RSI(df['close'],timeperiod=14)
                df['macd_line'],df['macd_signal'],df['macd_histogram']=talib.MACD(df['close'])
                df['bollinger_upper'],df['bollinger_middle'],df['bollinger_lower']=talib.BBANDS(df['close'])
                df['moving_avg_10']=df['close'].rolling(window=10).mean()
                df['moving_avg_50']=df['close'].rolling(window=50).mean()
                df['moving_avg_200']=df['close'].rolling(window=200).mean()
            else:
                df[['rsi_14','macd_line','macd_signal','macd_histogram','bollinger_upper','bollinger_middle','bollinger_lower','moving_avg_10','moving_avg_50','moving_avg_200']]=None
            # get the latest value, if the whole row of value are empty then None
            latest_data=df.iloc[-1] if not df.empty else None

            # calculate the market depth data indicators
            bid_price_1=order_book['bids'][0][0] if order_book['bids'] else None
            ask_price_1=order_book['asks'][0][0] if order_book['asks'] else None
            bid_volume_1=order_book['bids'][0][1] if order_book['bids'] else None
            ask_volume_1=order_book['asks'][0][1] if order_book['asks'] else None
            spread=ask_price_1-bid_price_1 if ask_price_1 and ask_price_1 else None
            buy_sell_ratio=bid_volume_1/ask_volume_1 if ask_volume_1 else None

            # test if funding_rate is supported by binance
            try:
                funding_rate = binance_interaction.fetch_funding_rate(symbol)['fundingRate'] if 'PERP' in symbol else None
            except Exception as e:
                print(f"❌ Skipping funding rate for {symbol}: {e}")
                funding_rate = None

            # get contract market data indicators
            funding_rate=binance_interaction.fetch_funding_rate(symbol)['fundingRate'] if 'PERP' in symbol else None
            open_interest_futures=binance_interaction.fetch_open_interest(symbol)['openInterest'] if 'PERP' in symbol else None


            # process the data
            data = (
                symbol,
                pd.to_datetime(ticker.get('timestamp', 0) or 0, unit='ms'),
                ticker.get('open', 0) or 0, # opening price
                ticker.get('last', 0) or 0, # lastest price
                ticker.get('high', 0) or 0, # highest price
                ticker.get('low', 0) or 0, # lowest price
                ticker.get('close', 0) or 0, # closing price
                ticker.get('baseVolume', 0) or 0, # trading volume
                bid_price_1,
                ask_price_1,
                bid_volume_1,
                ask_volume_1,
                None if pd.isna(latest_data.get('moving_avg_10', None)) else latest_data.get('moving_avg_10', None),
                None if pd.isna(latest_data.get('moving_avg_50', None)) else latest_data.get('moving_avg_50', None),
                None if pd.isna(latest_data.get('moving_avg_200', None)) else latest_data.get('moving_avg_200', None),
                None if pd.isna(latest_data.get('rsi_14', None)) else latest_data.get('rsi_14', None),
                None if pd.isna(latest_data.get('macd_line', None)) else latest_data.get('macd_line', None),
                None if pd.isna(latest_data.get('macd_signal', None)) else latest_data.get('macd_signal', None),
                None if pd.isna(latest_data.get('macd_histogram', None)) else latest_data.get('macd_histogram', None),
                None if pd.isna(latest_data.get('bollinger_upper', None)) else latest_data.get('bollinger_upper', None),
                None if pd.isna(latest_data.get('bollinger_middle', None)) else latest_data.get('bollinger_middle',
                                                                                                None),
                None if pd.isna(latest_data.get('bollinger_lower', None)) else latest_data.get('bollinger_lower', None),
                None if pd.isna(spread) else spread,
                None if pd.isna(buy_sell_ratio) else buy_sell_ratio,
                None if pd.isna(funding_rate) else funding_rate,
                None if pd.isna(open_interest_futures) else open_interest_futures,
            )

            # Insert binance data under the measurement of the indicators
            sql = """
            INSERT INTO ***(
            symbol, timestamp, open_price,last_price, high_price, low_price,close_price,volume,
            bid_price_1,ask_price_1,bid_volume_1,ask_volume_1,
            moving_avg_10,moving_avg_50,moving_avg_200,
            rsi_14,macd_line,macd_signal,macd_histogram,
            bollinger_upper,bollinger_middle,bollinger_lower,
            spread,buy_sell_ratio,funding_rate,open_interest_futures
            ) 
            VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cursor.execute(sql, data)
            database_connection.commit()
            print(f"{symbol} is successfully saved and data is {data}")

        except Exception as e:
            print(f"❌ Error with {symbol}: {e}")


# Main program
if __name__ == '__main__':
    while True:
        get_market_trade_data()
        time.sleep(120)  # 每 2 分钟更新一次
