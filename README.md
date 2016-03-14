# Bitcoin Dataset & Collector

Collector uses [Bitsamp](https://bitstamp.net) as a datasource, gathers market data.

###Collecting:

- Timestamp
- Price 
- Amount
- type (sell or buy)
- Top 20 bids
- Top 20 asks
- Daily High
- Daily Low
- Daily Volume
- Daily Volume Weighted Average Price

for each trade.

# Setup & Requirements

- Clone repository
- Create a virtualenv, preferably
- Install requirements
  - Install Postgres
  - Install Python packages `pip install --upgrade -r requirements.txt`
- Create a `secret.env` for secret keys and configuration
  - `DATABASE_URL`, should look like this `postgres://<USER>:<PASSWORD>@127.0.0.1:5432/<DB_NAME>`
  - [AWS Environment variables](http://boto3.readthedocs.org/en/latest/guide/configuration.html#environment-variables)

# Running & Logs

Just run `python dataset/collector.py`

Logs will be available in same directory as `colector.log`

# Dataset

Dataset will be available in June (after collecting 4-5 months worth data)

# Technical Details

Collected data is stored in Postgres. When finished collecting, data will be migrated to InfluxDB for nice time-series query capabilities

# Contributing

- Adding a new datasource / digital currency worth collecting
- Fixing docs / bugs etc.

# Licence

<a href="http://www.wtfpl.net/"><img src="http://www.wtfpl.net/wp-content/uploads/2012/12/wtfpl-badge-1.png" width="88" height="31" alt="WTFPL" /></a>

Check [LICENSE](https://github.com/umutbozkurt/bitcoin-dataset/blob/master/LICENSE)
