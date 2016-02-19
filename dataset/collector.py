from db import insert_trade, update_trade_for_transactions, update_trade_for_ticker
from signals import Ticker, Trades, Transactions, OrderBook

import threading
import datetime
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
        log.info('New Trade | %s: %s @ %s' % (message['id'], message['amount'], message['price']))

        insert_trade(
            tid=message['id'],
            price=message['price'],
            amount=message['amount'],
            asks=cls.latest_orders['asks'][:10],
            bids=cls.latest_orders['bids'][:10]
        )

        with lock:
            cls.last_ticker_transaction_ids.append(message['id'])

    @classmethod
    def orders_callback(cls, message):
        cls.latest_orders = message

    @classmethod
    def ticker_callback(cls, message):
        log.debug('New Ticker | High %s - Low %s' % (message.daily_high, message.daily_low))

        update_trade_for_ticker(message, cls.last_ticker_transaction_ids)

        with lock:
            cls.last_ticker_transaction_ids = []

    @classmethod
    def transactions_callback(cls, message):
        beginning = datetime.datetime.fromtimestamp(float(message[-1]['date']))
        end = datetime.datetime.fromtimestamp(float(message[0]['date']))
        log.debug('New Transactions | From %s To %s' % (beginning.strftime('%H:%M'), end.strftime('%H:%M')))

        update_trade_for_transactions(message)


Ticker.subscribe(BTCCollector.ticker_callback)
Trades.subscribe(BTCCollector.trades_callback)
Transactions.subscribe(BTCCollector.transactions_callback)
OrderBook.subscribe(BTCCollector.orders_callback)


while True:
    time.sleep(1)
