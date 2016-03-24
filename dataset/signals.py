import pusherclient
import json
import logging
import requests
import notifier


logging.basicConfig(filename='collector.log', level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Signal(object):
    """
    Base Signal Class
    """
    __metaclass__ = Singleton

    def __init__(self):
        self.callbacks = []

    @classmethod
    def subscribe(cls, callback):
        instance = cls()
        instance.callbacks.append(callback)

    @classmethod
    def update(cls, message):
        logging.debug('Received message: %s' % cls.__name__)
        raise NotImplementedError('You need to implement `update(cls, message=None) for class %s' % cls)

    @classmethod
    def publish(cls, message):
        instance = cls()
        [cb(message) for cb in instance.callbacks]


class JSONSignal(Signal):
    """
    JSON Signal
    """
    @classmethod
    def update(cls, message):
        processed_message = json.loads(message)
        cls.publish(processed_message)


class ObjectSignal(Signal):
    """
    Object Signal
        - refreses `x` seconds
    """
    def __init__(self):
        super(ObjectSignal, self).__init__()
        self.source = None
        self.update_interval = None
        self.failure_retry_seconds = 2

    def fetch(self, retry_count=0):
        try:
            response = requests.get(self.source)
        except requests.ConnectionError as exc:
            logging.error('Cannot connect - Connection error %s' % exc.message)
            if retry_count >= 0:
                return self.fetch(retry_count=retry_count - 1)
        else:
            try:
                response.raise_for_status()
            except requests.HTTPError:
                if retry_count >= 0:
                    return self.fetch(retry_count=retry_count - 1)
                else:
                    return notifier.notify('Bad Response: HTTP %s' % response.status_code, response.content)
            else:
                self.update(response.json())

    @classmethod
    def subscribe(cls, callback):
        super(ObjectSignal, cls).subscribe(callback)
        #  cls().start_timer()   -> this is obsolete now since python timer is not working

    def update(self, message):
        self.publish(message)

    # def start_timer(self, retry=False):
    #     interval = self.failure_retry_seconds if retry else self.update_interval
    #     threading.Timer(interval, self.fetch).start()


class OrderBook(JSONSignal):
    """
    Order Book Signal
    """
    pass


class Trades(JSONSignal):
    """
    Trade Signal
    """
    pass


class Ticker(ObjectSignal):
    """
    Live Ticker object, signal
        - refreshes every 20 seconds
    """
    def __init__(self):
        super(Ticker, self).__init__()

        self.source = 'https://www.bitstamp.net/api/ticker/'
        self.update_interval = 20

        self.last_check_timestamp = None
        self.daily_high = None
        self.daily_low = None
        self.daily_vwap = None
        self.daily_volume = None

    @classmethod
    def update(cls, message):
        instance = cls()
        instance.daily_high = float(message['high'])
        instance.daily_low = float(message['low'])
        instance.daily_vwap = float(message['vwap'])
        instance.daily_volume = float(message['volume'])
        instance.last_check_timestamp = int(message['timestamp'])

        cls.publish(instance)


class Transactions(ObjectSignal):
    """
    Transactions signal,
        - refreshes every 1 min
    """
    def __init__(self):
        super(Transactions, self).__init__()
        self.source = 'https://www.bitstamp.net/api/transactions/?time=hour'
        self.update_interval = 60


class TransactionsBackup(ObjectSignal):
    """
    Transactions Signal for Backup (Daily)
    """
    def __init__(self):
        super(Transactions, self).__init__()
        self.source = 'https://www.bitstamp.net/api/transactions/?time=day'
        self.update_interval = 60 * 60 * 3  # Every 3 hours


def connection_handler(data):
    global pusher

    order_book = pusher.subscribe('order_book')
    order_book.bind('data', OrderBook.update)

    live_trades = pusher.subscribe('live_trades')
    live_trades.bind('trade', Trades.update)


pusher = pusherclient.Pusher('de504dc5763aeef9ff52', log_level=logging.WARNING)
pusher.connection.bind('pusher:connection_established', connection_handler)
pusher.connect()
