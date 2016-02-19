from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from cassandra.query import ValueSequence, BatchStatement
from cassandra import AlreadyExists

import datetime
import operator
import logging


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
log.addHandler(handler)

KEYSPACE = 'btc_short_dataset'
session = None


def get_field_set(items, getter=operator.attrgetter('id'), cast=int):
    """
    Get ids via getter and collect them in a set
    """
    return {cast(getter(item)) for item in items}


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
        session = create_keyspace(session)
    except AlreadyExists:
        log.info('Keyspace Exists, Passing')

    session = cluster.connect(KEYSPACE)

    try:
        create_transactions_table(session)
    except AlreadyExists:
        log.info('DB Exists, Passing')

    return session


def create_keyspace(session):
    log.info('Creating Keyspace `%s`' % KEYSPACE)

    session.execute("""
        CREATE KEYSPACE %s WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 2}
    """ % KEYSPACE)

    return session


def create_transactions_table(session):
    log.info('Creating Table `transactions`')

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
        INSERT INTO transactions(id, price, amount, asks, bids)
        VALUES(%s, %s, %s, %s, %s)
    """, parameters=(tid, price, amount, db_asks, db_bids))


def update_trade_for_transactions(transactions):
    """
    Update trade's `sell` field from incoming transactions
        - Cannot batch query with `IF EXISTS`
    """
    transaction_ids = get_field_set(transactions, getter=operator.itemgetter('tid'))

    result = session.execute("""
        SELECT id
        FROM transactions
        WHERE id IN %s
    """, parameters=[ValueSequence(transaction_ids)])

    transaction_ids = get_field_set(result)
    log.info('Updating `sell` `ts` fields for %s Transactions' % len(transaction_ids))

    update_trade_statement = session.prepare("""
        UPDATE transactions
        SET sell = ?, ts = ?
        WHERE id = ?
    """)

    batch = BatchStatement(retry_policy=RetryPolicy.RETRY)

    for transaction in transactions:
        tid, sell, ts = transaction['tid'], bool(transaction['type']), int(transaction['date'])
        if tid in transaction_ids:
            batch.add(update_trade_statement, parameters=(sell, datetime.datetime.fromtimestamp(ts), tid))

    session.execute(batch)


def update_trade_for_ticker(ticker, transaction_ids):
    """
    Add daily values to trades
    """
    transaction_count = len(transaction_ids)
    log.info('Updating `daily_*` fields for %s Transactions' % transaction_count)

    session.execute("""
          UPDATE transactions
          SET daily_high = %s,
              daily_low = %s,
              daily_vwap = %s,
              daily_volume = %s
          WHERE id IN %s
        """, parameters=(
            ticker.daily_high, ticker.daily_low, ticker.daily_vwap, ticker.daily_volume, ValueSequence(transaction_ids)
        )
    )


session = session or prepare_session()
