from db import insert_trade, update_trade_for_transactions, update_trade_for_ticker, get_statistics, update_stat
from signals import Ticker, Trades, Transactions, OrderBook
from notifier import notify

import threading
import datetime
import logging
import time


logging.basicConfig(filename='collector.log', level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
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

        update_stat(1, 1)

        with lock:
            cls.last_ticker_transaction_ids.append(message['id'])

    @classmethod
    def orders_callback(cls, message):
        cls.latest_orders = message

    @classmethod
    def ticker_callback(cls, message):
        logging.info('New Ticker | High %s - Low %s' % (message.daily_high, message.daily_low))

        update_trade_for_ticker(message, cls.last_ticker_transaction_ids)
        update_stat(0, - len(cls.last_ticker_transaction_ids))

        with lock:
            cls.last_ticker_transaction_ids = []

    @classmethod
    def transactions_callback(cls, message):
        beginning = datetime.datetime.fromtimestamp(float(message[-1]['date']))
        end = datetime.datetime.fromtimestamp(float(message[0]['date']))
        logging.info('New Transactions | From %s To %s' % (beginning.strftime('%H:%M'), end.strftime('%H:%M')))

        update_trade_for_transactions(message)


def start():
    Ticker.subscribe(BTCCollector.ticker_callback)
    Trades.subscribe(BTCCollector.trades_callback)
    Transactions.subscribe(BTCCollector.transactions_callback)
    OrderBook.subscribe(BTCCollector.orders_callback)


def send_status_notification():
    stats = get_statistics()

    message = 'Total: %s | Null Containing: %s | Last Update: %s' % (stats.inserted_rows,
                                                                     stats.null_containing_rows,
                                                                     stats.updated_at.strftime('%H:%M'))
    notify('Collector Statistics', message)


def check_timers():
    stats = get_statistics()
    alert_threshold = datetime.timedelta(minutes=3)
    passes_threshold = datetime.datetime.utcnow() - stats.updated_at > alert_threshold

    if Ticker().timer is None or Transactions().timer is None or passes_threshold:
        notify('Timers Stopped', 'Ticker or Transaction timer stopped')


notification_timer = threading.Timer(60 * 60 * 12, send_status_notification)  # Every 12 hours, send notification
integrity_timer = threading.Timer(60 * 5, check_timers)  # Every 5 minutes, check integrity


if __name__ == '__main__':
    start()
    notification_timer.start()
    integrity_timer.start()

    while True:
        time.sleep(1)
