from db import insert_trade, update_trade_for_transactions, update_trade_for_ticker
from signals import Ticker, Trades, Transactions, OrderBook

import threading
import logging
import time


log = logging.getLogger('collector')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
log.addHandler(handler)

lock = threading.Lock()


class BTCCollector(object):
    """
    BTC Dataset Collector Class
    """
    latest_orders = None
    last_ticker_transaction_ids = []

    @classmethod
    def trades_callback(cls, message):
        log.info('New Trade')
        insert_trade(
            tid=message['id'],
            price=message['price'],
            amount=message['amount'],
            asks=cls.latest_orders['asks'][:5],
            bids=cls.latest_orders['bids'][:5]
        )
        with lock:
            cls.last_ticker_transaction_ids.append(message['id'])

    @classmethod
    def orders_callback(cls, message):
        log.info('New Orders')
        cls.latest_orders = message

    @classmethod
    def ticker_callback(cls, message):
        log.info('New Ticker')

        update_trade_for_ticker(message, cls.last_ticker_transaction_ids)

        with lock:
            cls.last_ticker_transaction_ids = []

    @classmethod
    def transactions_callback(cls, message):
        log.info('New Transactions')
        update_trade_for_transactions(message)


Ticker.subscribe(BTCCollector.ticker_callback)
Trades.subscribe(BTCCollector.trades_callback)
Transactions.subscribe(BTCCollector.transactions_callback)
OrderBook.subscribe(BTCCollector.orders_callback)


while True:
    time.sleep(1)
