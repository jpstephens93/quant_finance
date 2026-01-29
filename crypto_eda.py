from kraken.futures import Market as fMarket
from kraken.spot import Market as sMarket
from datetime import datetime
import requests
import pandas as pd


class Kraken:

    granularity_map = {
        "1d": 1440,
        "1h": 60,
        "30m": 30,
        "15m": 15,
        '1m': 1
    }

    futures_api_url = "https://futures.kraken.com/derivatives/api/v3"

    def get_active_futures(self) -> list:
        """
        Returns a list of active perpetual futures symbols.
        """
        url = self.futures_api_url + "/tickers"
        req = requests.get(url).json()
        tickers = pd.DataFrame(req['tickers'])
        perpetuals = tickers[tickers['tag'] == 'perpetual']
        return [x for x in perpetuals['symbol'].values if x[:2] == 'PF']

    def get_futures_ohlcv_df(self, symbol: str, granularity: str, start_date: str) -> pd.DataFrame:
        """
        Returns a formatted pandas DataFrame of timeseries data for a given symbol.
        """
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.today().timestamp())

        df = pd.DataFrame()
        while start_ts < end_ts:
            try:
                data = pd.DataFrame(
                    fMarket().get_ohlc(
                        tick_type='trade', symbol=symbol, resolution=granularity, from_=start_ts, to=end_ts
                    )['candles']
                )
            except Exception:
                # if time period too large
                data = pd.DataFrame(
                    fMarket().get_ohlc(
                        tick_type='trade', symbol=symbol, resolution=granularity, from_=start_ts
                    )['candles']
                )

            start_ts = int(max(data['time']) / 1000)
            data = self.format_timeseries(data, unit='ms')

            if len(data) == 1:
                break

            df = pd.concat([df, data], ignore_index=True).drop_duplicates('time').reset_index(drop=True)

        assert len(df) == len(df['time'].unique())

        return df

    def get_spot_ohlvc(self, symbol: str, granularity: str, start_date: str) -> pd.DataFrame:
        """
        Returns a formatted pandas DataFrame of timeseries data for a given symbol.
        """
        adj_granularity = self.granularity_map[granularity]
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())

        spot_data = sMarket().get_ohlc(pair=symbol, interval=adj_granularity, since=start_ts)
        spot_df = pd.DataFrame(spot_data[list(spot_data)[0]])[[0, 1, 2, 3, 4, 6]]
        spot_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']

        spot_df = format_timeseries(spot_df, unit='s')

        return spot_df


def get_avg_bid_price(fut_symbol: str):
    return sum([x[0] for x in fMarket().get_orderbook(fut_symbol)['orderBook']['bids'][:10]]) / 10


def get_avg_ask_price(fut_symbol: str):
    return sum([x[0] for x in fMarket().get_orderbook(fut_symbol)['orderBook']['asks'][:10]]) / 10


def get_market_bid_price(fut_symbol: str):
    ob = fMarket().get_orderbook(fut_symbol)['orderBook']
    return ob['bids'][0][0]


def get_market_ask_price(fut_symbol: str):
    ob = fMarket().get_orderbook(fut_symbol)['orderBook']
    return ob['asks'][0][0]


def get_mid_price(fut_symbol: str):
    ob = fMarket().get_orderbook(fut_symbol)['orderBook']
    curr_bid, curr_ask = ob['bids'][0][0], ob['asks'][0][0]
    return sum([curr_bid, curr_ask]) / 2


def format_timeseries(df, unit):
    """
    Formats the columns in a dataframe retrieved from Kraken market data API.
    """
    df['time'] = pd.to_datetime(df['time'], unit=unit)
    for col in df.columns:
        if col != 'time':
            df[col] = pd.to_numeric(df[col], downcast='float')
    return df
