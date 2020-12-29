import requests as req
import pandas as pd

url = 'https://api.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?start=0&limit=5000&sort=1'

data = req.get(url).json()

df = pd.DataFrame(data)

# seems to return 5k, at least for earliest and latest data
print(df.shape[0] == 5000)

df[0] = pd.to_datetime(df[0], unit='ms', utc=True)

# the different number of time differences are all over the place...1m up to over an hour
df[0].diff().value_counts()



url = 'https://api.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?start=0&limit=5000'

data = req.get(url).json()

df = pd.DataFrame(data)

# seems to return 5k, at least for earliest and latest data
print(df.shape == 5000)

df[0] = pd.to_datetime(df[0], unit='ms', utc=True)

df.sort_values(by=0, inplace=True)

# the different number of time differences are all over the place...1m up to 4m
df[0].diff().value_counts()


# I'm thinking there are periods with no trades, so we should forward-fill the data.
