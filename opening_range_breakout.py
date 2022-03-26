from lib2to3.pgen2.pgen import DFAState
from multiprocessing import connection
import sqlite3
import config
import alpaca_trade_api as tradeapi
import datetime as date

connection =sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    select id from strategy where name ='opening_range_breakout'
""")

strategy_id= cursor.fetchone()['id']

cursor.execute("""
    select symbol, name
    from stock
    join stock_strategy on stock_strategy.stock_id = stock.id
    where stock_strategy.strategy_id=?
""", (strategy_id,))

stocks = cursor.fetchall()
symbols =[ stock['symbol'] for stock in stocks]

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)


for symbol in symbols:
    minute_bars = api.polygon.historic_agg_v2(symbol,1,'minute', _from='2022-03-14', to='2022-03-15').df

    print(symbol)
    print(minute_bars)