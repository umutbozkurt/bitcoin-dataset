from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from cassandra.query import ValueSequence, BatchStatement
from cassandra import AlreadyExists

import datetime
import operator
import logging


logging.basicConfig(filename='collector.log',
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

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
        logging.info('Keyspace Exists, Passing')

    session = cluster.connect(KEYSPACE)

    try:
        create_transactions_table(session)
    except AlreadyExists:
        logging.info('Transactions Table Exists, Passing')

    try:
        create_stats_table(session)
    except AlreadyExists:
        logging.info('Stats Table Exists, Passing')

    return session


def create_keyspace(session):
    logging.info('Creating Keyspace `%s`' % KEYSPACE)

    session.execute("""
        CREATE KEYSPACE %s WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 2}
    """ % KEYSPACE)

    return session


def create_transactions_table(session):
    logging.info('Creating Table `transactions`')

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


def create_stats_table(session):
    logging.info('Creating Table `stats`')

    session.execute("""
        CREATE TABLE stats(
            t_name ascii PRIMARY KEY,
            inserted_rows counter,
            null_containing_rows counter
        )
    """)


def insert_trade(tid, price, amount, asks, bids):
    """
    Insert trade to db with incoming trade data
    """
    logging.info('Inserting Trade - %s' % tid)

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


def update_stat(inserted_rows, null_rows):
    """
    Insert statistics table
    """
    logging.info('Inserting Statistics')

    session.execute("""
        UPDATE stats
        SET inserted_rows = inserted_rows + %s, null_containing_rows = null_containing_rows + %s
        WHERE t_name = 'transactions'
    """, parameters=(int(inserted_rows), int(null_rows)))


def get_statistics():
    results = session.execute("""
        SELECT inserted_rows, null_containing_rows, WRITETIME(null_containing_rows) FROM stats
    """)

    return results[-1]


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
    logging.info('Updating `sell` `ts` fields for %s Transactions' % len(transaction_ids))

    update_trade_statement = session.prepare("""
        UPDATE transactions
        SET sell = ?, ts = ?
        WHERE id = ?
    """)

    batch = BatchStatement(retry_policy=RetryPolicy.RETRY)

    for transaction in transactions:
        tid, sell, ts = transaction['tid'], bool(transaction['type']), int(transaction['date'])
        if tid in transaction_ids:
            batch.add(update_trade_statement, parameters=(sell, datetime.datetime.utcfromtimestamp(ts), tid))

    session.execute(batch)


def update_trade_for_ticker(ticker, transaction_ids):
    """
    Add daily values to trades
    """
    transaction_count = len(transaction_ids)
    logging.info('Updating `daily_*` fields for %s Transactions' % transaction_count)

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
