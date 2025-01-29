# data acquisition
import ccxt
import pandas as pd
import os
from dotenv import load_dotenv
import time
import pymysql

# upload .env documents to get the key and secret
load_dotenv()
BINANCE_API_KEY = os.getenv("***")
BINANCE_API_SECRET = os.getenv("***")

# initialize the binance
give_information=ccxt.binance({
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_API_SECRET,
})

# config and connect database
database_config={
    "host": "localhost",
    "user": "root",
    "password": "******",
    "database": "binance_database"
}
connection=pymysql.connect(**database_config)





# get the newest data of the major coins
objects=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT']

# get the new trend of the trade
def create_data_table():
    with connection.cursor() as cursor:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(10),
            timestamp DATETIME,
            last_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            volume FLOAT,
            )
            ''')
        connection.commit()
def get_market_trade():
    data_results=[]
    for object in objects:
        try:
            # get the trend of the share
            ticker=give_information.fetch_ticker(object)
            data={
                "symbol":object,
                'timestamp':pd.to_datetime(ticker['timestamp'],unit='ms'),
                'last_price':ticker['last'],
                'high_price':ticker['high'],
                'low_price':ticker['low'],
                'volumn':ticker['quoteVolume'],
            }
            data_results.append(data)
            print(data_results)
        except Exception as e:
            print(f"get{object}wrong and the error is '{e}'")
        return data_results

def save_data_to_mysql(data):
    with connection.cursor() as cursor:
        query="""
        INSERT INTO data(symbol,timestamp,last_price,high_price,low_price,volume)
        values(%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(query,data)
        connection.commit()
        print('all data saved in database-mysql')

# main program
if __name__=='__main__':
    create_data_table()
    while True:
        market_data_results=get_market_trade()
        if market_data_results:
            save_data_to_mysql(market_data_results)
        time.sleep(60)


