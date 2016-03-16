from notifier import notify
from db import (insert_trade, update_trade_for_transactions, update_trade_for_ticker, get_statistics, update_stat,
                prepare_session)
from signals import Ticker, Trades, Transactions, OrderBook

import threading
import datetime
import logging
import time


logging.basicConfig(filename='collector.log', level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
lock = threading.Lock()

timer = 0  # python Timer is bad. this int will act like a timer
timer_reset = 1  # this is the reset threshold
stats_interval = 60 * 60 * 6  # Every 6 hours


class BTCCollector(object):
    """
    BTC Dataset Collector Class
    """
    latest_orders = None
    last_ticker_transaction_ids = []
    ticker_trigger = 3  # Trigger ticker manually on every 3 trade

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

        if len(cls.last_ticker_transaction_ids) > cls.ticker_trigger:
            Ticker().fetch()

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


def send_status_notification():
    stats = get_statistics()

    if stats:
        message = 'Total: %s | Null Containing: %s | Last Update: %s' % (stats.inserted_rows,
                                                                         stats.null_containing_rows,
                                                                         stats.updated_at.strftime('%H:%M'))
        notify('Collector Statistics', message)


def start():
    Ticker.subscribe(BTCCollector.ticker_callback)
    Trades.subscribe(BTCCollector.trades_callback)
    Transactions.subscribe(BTCCollector.transactions_callback)
    OrderBook.subscribe(BTCCollector.orders_callback)


if __name__ == '__main__':
    prepare_session()
    start()

    timer_reset = Transactions().update_interval * Ticker().update_interval * stats_interval

    while True:
        # Implement a basic timer with integers since python timer does not work properly
        time.sleep(1)
        timer += 1

        if timer % Transactions().update_interval == 0:
            Transactions().fetch()

        if timer % Ticker().update_interval == 0:
            Ticker().fetch()

        if timer % stats_interval == 0:
            send_status_notification()

        if timer % timer_reset == 0:
            timer = 0
