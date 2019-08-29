import sqlite3
import pendulum


class SqliteDatabase:
    def __init__(self, path, candle_size):
        self.path = path
        self.candle_size = candle_size
        self.con = sqlite3.connect(path)
        self.create()

    def close(self):
        self.con.close()

    def create(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS candles_{} (
                symbol TEXT NOT NULL,
                time TEXT NOT NULL,
                open TEXT NOT NULL,
                close TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                volume TEXT NOT NULL,
                PRIMARY KEY (symbol, time)
            );
        """.format(self.candle_size))

    def insert_candles(self, symbol, candles):
        def candle_generator():
            # prepend `symbol` before each candle data
            for candle in candles:
                candle.insert(0, symbol)
                yield candle

        with self.con:
            self.con.executemany("""
                INSERT OR IGNORE INTO candles_{}(
                    symbol, time, open, close, high, low, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.format(self.candle_size), candle_generator())

    def get_latest_candle_date(self, symbol):
        """
        Get the time of the most recent candle for a symbol
        """
        r = self.con.execute('select max(time) from candles_{} where symbol=?'.format(self.candle_size),
                             (symbol,))
        result = r.fetchone()[0]
        if result is None:
            return None
        else:
            return int(result)
