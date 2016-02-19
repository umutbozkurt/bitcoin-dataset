from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ValueSequence
from cassandra import AlreadyExists


import datetime
import logging


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
log.addHandler(handler)

KEYSPACE = 'btc_short_dataset'
session = None


def prepare_session():
    """
        - Create keyspace if not exists
        - Create table if not exists
        - Open a session
    """
    cluster = Cluster()
    global session

    try:
        session = cluster.connect()
        create_keyspace(session)
    except AlreadyExists:
        session = cluster.connect(KEYSPACE)

    try:
        create_transactions_table(session)
    except AlreadyExists:
        log.info('db exists, passing')

    return session


def create_keyspace(session):
    log.info('creating keyspace')

    session.execute("""
        CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}
    """ % KEYSPACE)

    return session


def create_transactions_table(session):
    log.info('creating table')

    session.execute("""
        CREATE TABLE transactions(
            id int PRIMARY KEY,
            ts timestamp,
            price float,
            amount float,
            sell boolean,
            asks frozen <list<list<float>>>,
            bids frozen <list<list<float>>>,
            daily_high float,
            daily_low float,
            daily_vwap float,
            daily_volume float
        )
    """)


def create_timestamp():
    return int((datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds())


def insert_trade(tid, price, amount, asks, bids):
    """
    Insert trade to db with incoming trade data
    """
    log.info('Inserting Trade - %s' % tid)

    db_asks = []
    for ask in asks:
        ask_price, ask_amount = ask
        db_asks.append([float(ask_price), float(ask_amount)])

    db_bids = []
    for bid in bids:
        bid_price, bid_amount = bid
        db_bids.append([float(bid_price), float(bid_amount)])

    session.execute("""
        INSERT INTO transactions(id, ts, price, amount, asks, bids)
        VALUES(%s, %s, %s, %s, %s, %s)
    """, (tid, create_timestamp(), price, amount, db_asks, db_bids))


def update_trade_for_transactions(transactions):
    """
    Update trade's `sell` field from incoming transactions
    """
    log.info('Updating %s Transactions' % len(transactions))

    update_trade_statement = session.prepare("""
        UPDATE transactions
        SET sell = ?
        WHERE id = ?
    """)

    batch = BatchStatement()

    for transaction in transactions:
        tid, sell = transaction['tid'], transaction['type']
        sell = bool(sell)
        batch.add(update_trade_statement, (sell, tid))

    session.execute(batch)


def update_trade_for_ticker(ticker, transaction_ids):
    """
    Add daily values to trades
    """
    transaction_count = len(transaction_ids)
    log.info('Updating Daily Values for %s object(s)' % transaction_count)

    query_body = """
      UPDATE transactions
          SET daily_high = %s,
              daily_low = %s,
              daily_vwap = %s,
              daily_volume = %s
    """

    if transaction_count > 1:
        session.execute(
            query_body + ' WHERE id IN %s',
            (ticker.daily_high, ticker.daily_low, ticker.daily_vwap,
             ticker.daily_volume, ValueSequence(transaction_ids))
        )
    elif transaction_count == 1:
        session.execute(
            query_body + ' WHERE id = %s',
            (ticker.daily_high, ticker.daily_low, ticker.daily_vwap, ticker.daily_volume, transaction_ids[0])
        )
    else:
        pass


session = session or prepare_session()
