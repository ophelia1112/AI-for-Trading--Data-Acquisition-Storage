# data acquisition
import ccxt
import pandas as pd
import os
from dotenv import load_dotenv
import time
import pymysql

# Load .env documents to get the key and secret
load_dotenv()
BINANCE_API_KEY = os.getenv("****")
BINANCE_API_SECRET = os.getenv("****")

# Initialize Binance
binance_interaction = ccxt.binance({
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_API_SECRET,
    'options': {'adjustForTimeDifference': True}
})

# Config and connect database
database_connection = pymysql.connect(
    host='***',
    user='***',
    password=os.getenv("***"),
    database=os.getenv("***"),
    charset='utf8mb4',
    autocommit=True
)
cursor = database_connection.cursor()

# Get the newest data of the major coins
symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']


# Get the new trend of the trade
def get_market_trade_data():
    for symbol in symbols:
        try:
            # Get the trend of the share
            ticker = binance_interaction.fetch_ticker(symbol)

            data = (
                symbol,
                pd.to_datetime(ticker.get('timestamp', 0), unit='ms'),
                ticker.get('last', 0),
                ticker.get('high', 0),
                ticker.get('low', 0),
                ticker.get('baseVolume', 0)
            )

            # Insert data
            sql = """
            INSERT INTO binance_market_data(symbol, timestamp, last_price, high_price, low_price, volume) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            print(f"Inserting: {data}")

            cursor.execute(sql, data)
            database_connection.commit()
            print(f"{symbol} is successfully saved and data is {data}")

        except Exception as e:
            print(f"❌ Error with {symbol}: {e}")


# Main program
if __name__ == '__main__':
    while True:
        get_market_trade_data()
        time.sleep(120)  
