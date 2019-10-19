# loads candle data saved by bitfinex-ohlc-import
import os

import pandas as pd
import sqlite3

def load_data(pair='btcusd', candle_size='5m', get_timediffs=False, path="/home/nate/github/bitfinex_ohlc_import/", filename="bitfinex.sqlite3"):
    conn = sqlite3.connect(os.path.join(path, filename))
    df = pd.read_sql_query("select * from candles_{} where symbol='{}';".format(candle_size, pair), conn)
    df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)


    if get_timediffs:
        # gets iloc of last timediff that's not the normal timediff
        # diffs = df[df['timediff'] > pd.Timedelta('5T')]
        #latest_iloc = df.index.get_loc(diffs.index[-1])
        df['timediff'] = df['time'].diff()

    # need to forward-fill data because low volume times are missing data
    # resample, forward filling prices and 0-filling volume
    df.set_index('time', inplace=True)
    df = df.resample('5T').ffill()
    df['new_time'] = df.index
    upsampled_idxs = df[df['new_time'] != df.index].index
    df.loc[upsampled_idxs, 'volume'] = 0
    df.loc[upsampled_idxs, 'open'] = df.loc[upsampled_idxs, 'close']
    df.loc[upsampled_idxs, 'high'] = df.loc[upsampled_idxs, 'close']
    df.loc[upsampled_idxs, 'low'] = df.loc[upsampled_idxs, 'close']
    df.drop(columns='new_time', inplace=True)
    for c in df.columns[1:]:  # first column is symbol
        df[c] = df[c].astype('float')

    df.dropna(inplace=True)

    df.drop(columns='symbol', inplace=True)

    conn.close()
    return df


def resample_data(df, timeunit='1H'):
    """
    Resamples OHLCV data into OHLCV data with bars the size of timeunit.  
    timeunit should be a pandas time unit string.
    """
    new_df = pd.DataFrame(
                        {'open': df['open'].resample(timeunit).first(),
                        'high': df['high'].resample(timeunit).max(),
                        'low': df['low'].resample(timeunit).min(),
                        'close': df['close'].resample(timeunit).last(),
                        'volume': df['volume'].resample(timeunit).sum()}
                        )
    
    return new_df



def check_for_gaps(df, unit='T'):
    """
    Checks to see if any gaps in the data using units.  T is minutes.
    """
    timediffs = df['time'].diff()
    # last_non_unit_diff = timediffs.


if __name__ == "__main__":
    df = load_data()
