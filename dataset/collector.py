from db import insert_trade, update_trade_for_transactions, update_trade_for_ticker
from signals import Ticker, Trades, Transactions, OrderBook

import threading
import datetime
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
lock = threading.Lock()


class BTCCollector(object):
    """
    BTC Dataset Collector Class
    """
    latest_orders = None
    last_ticker_transaction_ids = []

    @classmethod
    def trades_callback(cls, message):
        logging.info('New Trade | %s: %s @ %s' % (message['id'], message['amount'], message['price']))

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
        logging.debug('New Ticker | High %s - Low %s' % (message.daily_high, message.daily_low))

        update_trade_for_ticker(message, cls.last_ticker_transaction_ids)

        with lock:
            cls.last_ticker_transaction_ids = []

    @classmethod
    def transactions_callback(cls, message):
        beginning = datetime.datetime.fromtimestamp(float(message[-1]['date']))
        end = datetime.datetime.fromtimestamp(float(message[0]['date']))
        logging.debug('New Transactions | From %s To %s' % (beginning.strftime('%H:%M'), end.strftime('%H:%M')))

        update_trade_for_transactions(message)


def start():
    Ticker.subscribe(BTCCollector.ticker_callback)
    Trades.subscribe(BTCCollector.trades_callback)
    Transactions.subscribe(BTCCollector.transactions_callback)
    OrderBook.subscribe(BTCCollector.orders_callback)


if __name__ == '__main__':
    start()
