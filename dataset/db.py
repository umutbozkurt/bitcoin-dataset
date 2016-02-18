from cassandra.cluster import Cluster
from cassandra import AlreadyExists

import logging

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
log.addHandler(handler)


KEYSPACE = 'btc_short_dataset'
session = None


def prepare_session():
    cluster = Cluster()

    try:
        session = cluster.connect()
        create_keyspace(session)
    except AlreadyExists:
        session = cluster.connect(KEYSPACE)

    try:
        create_transactions_table(session)
    except AlreadyExists:
        pass

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
            asks frozen <list<tuple<float, float>>>,
            bids frozen <list<tuple<float, float>>>,
            daily_high float,
            daily_low float,
            daily_vwap float,
            daily_volume float
        )
    """)


session = session or prepare_session()
