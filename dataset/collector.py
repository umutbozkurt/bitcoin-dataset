from db import session
from bitstamp import Ticker, Trades, Transactions, OrderBook
import time


def ticker(message):
    print '======TICKER======'
    print message


def trades(message):
    print '======TRADES======'
    print message


def trans(message):
    print '======TRANSACTIONS======'
    print message


def orders(message):
    print '======ORDERS======'
    print message


Ticker.subscribe(ticker)
Trades.subscribe(trades)
Transactions.subscribe(trans)
OrderBook.subscribe(orders)


while True:
    time.sleep(1)
