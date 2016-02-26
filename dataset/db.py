import records
import operator
import logging

from sqlalchemy.exc import ProgrammingError

logging.basicConfig(filename='collector.log',
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


db = records.Database()


def get_field_set(items, getter=operator.attrgetter('id'), cast=int):
    """
    Get ids via getter and collect them in a set
    """
    return {cast(getter(item)) for item in items}


def prepare_session():
    """
        - Open a session
        - Create table if not exists
    """
    try:
        create_transactions_table()
    except ProgrammingError:
        logging.info('Transactions Table Exists, Passing')

    try:
        create_stats_table()
    except ProgrammingError:
        logging.info('Stats Table Exists, Passing')


def create_transactions_table():
    logging.info('Creating Table `transactions`')

    db.query("""
        CREATE TABLE transactions(
            id int PRIMARY KEY,
            ts timestamp,
            price float,
            amount float,
            sell boolean,
            asks float[][],
            bids float[][],
            daily_high float,
            daily_low float,
            daily_vwap float,
            daily_volume float
        )
    """)


def create_stats_table():
    logging.info('Creating Table `stats`')

    db.query("""
        CREATE TABLE stats(
            t_name varchar(20) PRIMARY KEY,
            inserted_rows int ,
            null_containing_rows int,
            updated_at timestamp with time zone
        )
    """)

    db.query("""
        INSERT INTO stats(t_name, inserted_rows, null_containing_rows, updated_at)
        VALUES('transactions', 0, 0, to_timestamp(extract(epoch from now())))
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

    db.query("""
        INSERT INTO transactions(id, price, amount, asks, bids)
        VALUES(:id, :price, :amount, :asks, :bids)
    """, id=tid, price=price, amount=amount, asks=db_asks, bids=db_bids)


def update_stat(inserted_rows, null_rows):
    """
    Insert statistics table
    """
    logging.info('Inserting Statistics')

    db.query("""
        UPDATE stats
        SET
        inserted_rows = inserted_rows + :inserted_rows,
        null_containing_rows = null_containing_rows + :null_rows,
        updated_at = to_timestamp(extract(epoch from now()))
        WHERE t_name = 'transactions'
    """, inserted_rows=int(inserted_rows), null_rows=int(null_rows))


def get_statistics():
    results = db.query("""
        SELECT inserted_rows, null_containing_rows, updated_at FROM stats
    """)

    try:
        return results[-1]
    except IndexError:
        return None


def update_trade_for_transactions(transactions):
    """
    Update trade's `sell` field from incoming transactions
        - Cannot batch query with `IF EXISTS`
    """
    logging.info('Updating `sell` `ts` fields for %s Transactions' % len(transactions))

    for transaction in transactions:
        tid, sell, ts = transaction['tid'], bool(transaction['type']), int(transaction['date'])
        db.query("""
            UPDATE transactions
            SET sell = :sell, ts = to_timestamp(:ts)
            WHERE id = :id
        """, sell=sell, ts=ts, id=tid)


def update_trade_for_ticker(ticker, transaction_ids):
    """
    Add daily values to trades
    """
    transaction_count = len(transaction_ids)
    logging.info('Updating `daily_*` fields for %s Transactions' % transaction_count)

    if transaction_count > 0:
        db.query(
            """UPDATE transactions
            SET daily_high = :high,
              daily_low = :low,
              daily_vwap = :vwap,
              daily_volume = :volume
            WHERE id IN :id_list""",
            high=ticker.daily_high,
            low=ticker.daily_low,
            vwap=ticker.daily_vwap,
            volume=ticker.daily_volume,
            id_list=tuple(transaction_ids)
        )

