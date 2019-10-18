import json
import logging
import time

import click
import pendulum
import pandas as pd

from db import SqliteDatabase
from utils import date_range, get_data

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

API_URL = 'https://api.bitfinex.com/v2'


def get_symbols():
    """
    curl https://api-pub.bitfinex.com/v2/tickers?symbols=ALL

    Transforms from tBTCUSD to btcusd which is due to the original code in this repo.
    """
    url = 'https://api-pub.bitfinex.com/v2/tickers?symbols=ALL'
    data = get_data(url)
    df = pd.DataFrame(data)
    pair_df = df[df[0].str.contains('t\w\w\w\w\w\w')].copy()
    pair_df[0] = pair_df[0].apply(lambda x: x[1:].lower())
    return pair_df[0].tolist()



def get_candles(symbol, start_date, end_date, candle_size='5m', limit=5000, get_earliest=False):
    """
    Return symbol candles between two dates.
    https://docs.bitfinex.com/v2/reference#rest-public-candles
    """
    if get_earliest:
        url = f'{API_URL}/candles/trade:{candle_size}:t{symbol.upper()}/hist' \
              f'?start={start_date}&end={end_date}&limit={limit}&sort=1'
        data = get_data(url)
        # reverse data
        data = data[::-1]
    else:
        url = f'{API_URL}/candles/trade:{candle_size}:t{symbol.upper()}/hist' \
              f'?start={start_date}&end={end_date}&limit={limit}'
        data = get_data(url)

    return data


@click.command()
@click.argument('db_path', default='bitfinex.sqlite3',
                type=click.Path(resolve_path=True))
# candle size should be in minutes
@click.option('--candle_size', default='5m')
@click.option('--debug', is_flag=True, help='Set debug mode')
def main(db_path, candle_size, debug):
    candle_size_int = int(candle_size[:-1])
    if debug:
        logger.setLevel(logging.DEBUG)

    db = SqliteDatabase(path=db_path, candle_size=candle_size)

    symbols = get_symbols()
    logging.info(f'Found {len(symbols)} symbols')
    for i, symbol in enumerate(symbols, 1):
        # get start date for symbol
        # this is either the last entry from the db
        # or the trading start date (from json file)
        latest_candle_date = db.get_latest_candle_date(symbol)
        if latest_candle_date is None:
            logging.debug('No previous entries in db. Starting from scratch')
            start_date = 0
            logging.info(f'{i}/{len(symbols)} | {symbol} | Processing from beginning')
        else:
            logging.debug('Found previous db entries. Resuming from latest')
            start_date = latest_candle_date
            logging.info(f'{i}/{len(symbols)} | {symbol} | Processing from {pd.to_datetime(start_date, unit="ms", utc=True)}')

        while True:
            # bitfinex is supposed to return 5000 datapoints but often returns fewer
            # probably due to not all bars having trades
            now = int(pd.Timestamp.utcnow().timestamp() * 1000)
            if start_date == 0:
                end_date = now
            else:
                # number of datapoints x candle size x s/min x ms/s x extra factor
                end_date = start_date + 5000 * candle_size_int * 60 * 1000 * 100

            # request won't work with an end date after the current time
            if end_date > now:
                end_date = now

            fmt_start = pd.to_datetime(start_date, unit='ms', utc=True).strftime('%D %H:%M')
            fmt_end = pd.to_datetime(end_date, unit='ms', utc=True).strftime('%D %H:%M')
            logging.debug(f'{fmt_start} -> {fmt_end}')
            # returns (max) 5000 candles, one for each bar
            candles = get_candles(symbol, start_date, end_date, get_earliest=True, candle_size=candle_size)
            # import ipdb; ipdb.set_trace()

            # df = pd.DataFrame(candles)
            # time_diffs = df[0].astype('int').diff().value_counts()
            # if len(time_diffs) > 1:
            #     logging.debug('WARNING: more than one time difference:')
            #     logging.debug(time_diffs)

            # end when we don't see any new data
            last_start_date = start_date
            start_date = candles[0][0]

            if start_date == last_start_date:
                logging.debug('Reached latest data, ending')
                time.sleep(1)
                break

            # seems like this modifies the original 'candles' to insert the ticker
            logging.debug(f'Fetched {len(candles)} candles')
            if candles:
                db.insert_candles(symbol, candles)


            # prevent from api rate-limiting -- 60 per minute claimed, but seems to be a little slower
            time.sleep(1)

    db.close()


if __name__ == '__main__':
    main()
