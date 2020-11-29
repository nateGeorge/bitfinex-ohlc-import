import json
import sqlite3

# get bitcoin price data
db = sqlite3.connect('C:/Users/words/.bitfinex_data/bitfinex.sqlite3')
cur = db.cursor()
res = cur.execute('select * from candles_1d where symbol="btcusd";')
data = res.fetchall()

# get column names
res = cur.execute('pragma table_info(candles_1d)')
column_names = [r[1] for r in res.fetchall()]


# convert numbers to int (time) and float (others)
formatted_data = []
for d in data:
    formatted_data.append([d[0]] + [int(d[1])] + [float(di) for di in d[2:]])


# convert data to a dictionary
data_list = []
for i, d in enumerate(formatted_data):
    data_list.append({c: di for c, di in zip(column_names, d)})

# write data as json to file
with open('bitcoin_price.json', 'w') as f:
    f.write(json.dumps(data_list, indent=True))

db.close()