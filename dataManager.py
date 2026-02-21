import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo

class ServiceManager:
    def __init__(self):
        # No longer storing full DataFrames as instance variables to save memory.
        pass

    def analyze_stockdata(self, symbol):
        todayn = datetime.now().strftime('%d')
        yesterdayn = (datetime.now() - timedelta(days=1)).strftime('%d')

        data5m = self.GetStockdata_Byinterval(symbol, "5m", indicatorList="macd")
        df_merged = data5m[data5m['nday'] == todayn].tail(20).copy()
        del data5m

        data15m = self.GetStockdata_Byinterval(symbol, "15m", indicatorList="macd")
        data30m = self.GetStockdata_Byinterval(symbol, "30m", indicatorList="macd")
        data1h  = self.GetStockdata_Byinterval(symbol, "1h",  indicatorList="macd")

        data15m = self.calculate_Buy_Sell_Values(data15m, data30m, 65)
        df_merged = pd.concat(
            [df_merged, data15m[data15m['nday'] == todayn].tail(12)],
            ignore_index=True
        )
        del data15m

        data30m = self.calculate_Buy_Sell_Values(data30m, data1h, 125)
        df_merged = pd.concat(
            [df_merged, data30m[data30m['nday'] == todayn].tail(8)],
            ignore_index=True
        )
        del data30m

        df_merged = pd.concat(
            [df_merged, data1h[data1h['nday'] == todayn].tail(4)],
            ignore_index=True
        )
        del data1h

        data4h = self.GetStockdata_Byinterval(symbol, "4h", indicatorList="macd").tail(3)
        df_temp = data4h[(data4h['nday'] == todayn) | (data4h['nday'] == yesterdayn)]
        if len(df_temp) == 0:
            df_temp = data4h
        df_merged = pd.concat([df_merged, df_temp], ignore_index=True)
        del data4h, df_temp

        return df_merged

    def GetStockdata_Byinterval(self, symbol, interval="1d", indicatorList="macd"):
        stPeriod = int((datetime.now() - timedelta(days=4)).timestamp())
        endPeriod = datetime.now()
        minutes_to_subtract = endPeriod.minute % 5
        endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0)

        df = self.download_stock_data(symbol, stPeriod, endPeriod.timestamp(), interval)
        if df is None:
            print("Failed to fetch data. Please check your internet connection.")
            return

        if interval == "5m":
            valid_minutes = {"00","05","10","15","20","25","30","35","40","45","50","55"}
            df = df[(df['unixtime'] <= endPeriod.timestamp()) & (df['minute'].isin(valid_minutes))]

        elif interval == "15m":
            minutes_to_subtract = endPeriod.minute % 15
            ep = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
            df = df[(df['unixtime'] <= ep) & (df['minute'].isin({"00","15","30","45"}))]

        elif interval == "30m":
            minutes_to_subtract = endPeriod.minute % 30
            ep = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
            df = df[(df['unixtime'] <= ep) & (df['minute'].isin({"00","30"}))]

        elif interval == "1h":
            df = df.resample('1h', origin='05:00:00-04:00').agg({
                'unixtime': 'first',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            }).dropna()
            ep = endPeriod.replace(minute=0, second=0, microsecond=0).timestamp()
            df = df[df['unixtime'] <= ep]
            df['unixtime'] = pd.to_numeric(df['unixtime'])
            # Compute datetime series once
            dt_ny = pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            df['rec_dt'] = dt_ny.dt.date
            df['nmonth'] = dt_ny.dt.strftime('%m').astype('category')
            df['nday']   = dt_ny.dt.strftime('%d').astype('category')
            df['hour']   = dt_ny.dt.strftime('%H').astype('category')
            df['minute'] = dt_ny.dt.strftime('%M').astype('category')
            del dt_ny

        elif interval == "4h":
            df = df[df['minute'].isin({"00"})]
            hours_to_subtract = endPeriod.hour % 4
            ep = endPeriod.replace(hour=endPeriod.hour - hours_to_subtract, minute=0, second=0, microsecond=0).timestamp() - 1
            df = df[df['unixtime'] <= ep]
            df = df.resample('4h', origin='05:00:00-04:00', closed='right', label='right').agg({
                'unixtime': 'first',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            }).dropna()
            df['unixtime'] = pd.to_numeric(df['unixtime'])
            dt_ny = pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            df['rec_dt'] = dt_ny.dt.date
            df['nmonth'] = dt_ny.dt.strftime('%m').astype('category')
            df['nday']   = dt_ny.dt.strftime('%d').astype('category')
            df['hour']   = dt_ny.dt.strftime('%H').astype('category')
            df['minute'] = dt_ny.dt.strftime('%M').astype('category')
            del dt_ny
            df['hour'] = df['hour'].cat.rename_categories(lambda x: "17" if x == "18" else x)

        # Compute indicators on df in-place (no separate copy)
        if "macd" in indicatorList:
            df = self.calculate_macd(df)
        if "rsi" in indicatorList:
            df = self.calculate_rsi(df)

        # Select only needed columns
        base_cols = ['unixtime', 'nmonth', 'nday', 'hour', 'minute']
        indicator_cols = []
        if "macd" in indicatorList:
            indicator_cols += ['macd', 'msignal', 'histogram']
        if "rsi" in indicatorList:
            indicator_cols += ['rsi', 'rsignal']

        final_cols = [c for c in base_cols + indicator_cols + ['open', 'close', 'high', 'low'] if c in df.columns]
        df_sel = df[final_cols].copy()
        del df

        df_sel['interval'] = pd.Categorical([interval] * len(df_sel))
        df_sel['symbol']   = pd.Categorical([symbol.replace("%3DF", "")] * len(df_sel))

        return df_sel

    def download_stock_data(self, symbol, startPeriod, endPeriod, interval="1d"):
        """
        Fetch stock data from Yahoo Finance API.
        interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        """
        if interval in ("4h", "1h"):
            interval = "30m"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            'period1': int(startPeriod),
            'period2': int(endPeriod),
            'interval': interval,
            'includePrePost': 'true'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            result = data['chart']['result'][0]
            timestamps = result['timestamp']
            quotes = result['indicators']['quote'][0]

            df = pd.DataFrame({
                'unixtime': np.array(timestamps, dtype=np.int32),
                'open':  pd.array(quotes['open'],  dtype='float32'),
                'high':  pd.array(quotes['high'],  dtype='float32'),
                'low':   pd.array(quotes['low'],   dtype='float32'),
                'close': pd.array(quotes['close'], dtype='float32'),
            })
            df.dropna(inplace=True)

            # Build timestamp index once from unixtime
            ts = pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            df.index = ts
            df.index.name = 'timestamp'

            df['rec_dt'] = ts.dt.date.values
            df['nmonth'] = ts.dt.strftime('%m').astype('category')
            df['nday']   = ts.dt.strftime('%d').astype('category')
            df['hour']   = ts.dt.strftime('%H').astype('category')
            df['minute'] = ts.dt.strftime('%M').astype('category')

            return df

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except KeyError as e:
            print(f"Error parsing data: {e}")
            return None

    def calculate_macd(self, df, fast=12, slow=26, signal=9):
        """Calculate MACD, Signal, and Histogram. Operates in-place."""
        close = df['close']
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()

        df['macd']      = macd_line.round(2).astype('float32')
        df['msignal']   = signal_line.round(2).astype('float32')
        df['histogram'] = (macd_line - signal_line).round(2).astype('float32')

        return df

    def calculate_Buy_Sell_Values(self, dfcur, dfhtf, lookupmins):
        dfcur = dfcur.copy()
        dfcur['buyval']    = np.float32(0)
        dfcur['sellval']   = np.float32(0)
        dfcur['stoploss']  = np.float32(0)
        dfcur['crossover'] = 'Neutral'

        lookupts = int((datetime.now() - timedelta(minutes=lookupmins)).timestamp())
        dfcur = dfcur[dfcur['unixtime'].astype(int) >= lookupts].copy()

        if not dfcur.empty:
            dfhtf = dfhtf[dfhtf['unixtime'].astype(int) >= lookupts]
            dfcur['ninemaval']     = dfcur['close'].rolling(window=9).mean().round(2)
            dfcur['histogram_prev'] = dfcur['histogram'].shift(1)

            last_dfrec = dfcur.iloc[[-1]]
            if not last_dfrec.empty and not dfhtf.empty:
                last_dfhtf_rec = dfhtf.iloc[[-1]]
                hist_cur  = last_dfrec['histogram'].iloc[0]
                hist_prev = last_dfrec['histogram_prev'].iloc[0]
                change_values = False
                crossoverval = 'Neutral'

                if hist_cur >= 0 and hist_prev <= 0 and hist_prev != hist_cur:
                    change_values = True
                    crossoverval = 'Bullish'
                elif hist_cur <= 0 and hist_prev >= 0 and hist_prev != hist_cur:
                    change_values = True
                    crossoverval = 'Bearish'

                if change_values:
                    idx = last_dfrec.index[0]
                    dfcur.loc[idx, 'crossover'] = crossoverval
                    dfcur.loc[idx, 'buyval']    = last_dfrec['ninemaval'].iloc[0]
                    dfcur.loc[idx, 'sellval']   = last_dfhtf_rec['close'].iloc[0]
                    dfcur.loc[idx, 'stoploss']  = last_dfhtf_rec['open'].iloc[0]

            dfcur.drop(columns=['histogram_prev', 'ninemaval'], inplace=True)

        return dfcur

    def identify_candlestick_patterns(self, data):
        """Identifies common candlestick patterns in the data."""
        if len(data) < 3:
            return data

        data['pattern']  = 'NA'
        data['pattern2c'] = 'NA'
        data['pattern3c'] = 'NA'

        opens  = data['open'].to_numpy(dtype='float32')
        highs  = data['high'].to_numpy(dtype='float32')
        lows   = data['low'].to_numpy(dtype='float32')
        closes = data['close'].to_numpy(dtype='float32')
        idx    = data.index

        for i in range(3, len(data)):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            o_prev, h_prev, l_prev, c_prev = opens[i-1], highs[i-1], lows[i-1], closes[i-1]

            body        = abs(c - o)
            price_range = h - l

            # Single-bar patterns
            if price_range > 0:
                ratio = body / price_range
                if ratio < 0.1:
                    data.loc[idx[i], 'pattern'] = 'Dj'
                elif ratio > 0.95:
                    data.loc[idx[i], 'pattern'] = 'UM' if c > o else 'EM'

            # Two-bar patterns
            if c > o and l > l_prev and c > o_prev and price_range > 0 and body / price_range > 0.95:
                data.loc[idx[i], 'pattern2c'] = 'UE'
            elif c < o and h < h_prev and c < o_prev and price_range > 0 and body / price_range > 0.95:
                data.loc[idx[i], 'pattern2c'] = 'EE'

            # Three-bar patterns
            o_p2, c_p2 = opens[i-2], closes[i-2]
            o_p3, c_p3 = opens[i-3], closes[i-3]

            is_three_white = c_p3 < o_p3 and c_p2 > o_p2 and c_prev > o_prev
            is_higher_highs = c_p2 > c_p3 and c_prev > c_p2
            is_strike_down  = c < o and o > c_prev and c < o_p3
            if is_three_white and is_higher_highs and is_strike_down:
                data.loc[idx[i], 'pattern3c'] = 'Ul3LS'

            is_three_black = c_p3 > o_p3 and c_p2 < o_p2 and c_prev < o_prev
            is_lower_lows  = c_p2 < c_p3 and c_prev < c_p2
            is_strike_up   = c > o and o < c_prev and c > o_p3
            if is_three_black and is_lower_lows and is_strike_up:
                data.loc[idx[i], 'pattern3c'] = 'Ea3LS'

        return data

    def calculate_bollinger_bands(self, df, period=20, std_dev=2):
        mid    = df['close'].rolling(window=period).mean()
        stddev = df['close'].rolling(window=period).std()
        df['midbnd'] = mid.round(2).astype('float32')
        df['ubnd']   = (mid + std_dev * stddev).round(2).astype('float32')
        df['lbnd']   = (mid - std_dev * stddev).round(2).astype('float32')
        return df

    def calculate_rsi(self, df, period=14):
        """Calculate RSI and Signal. Operates in-place (no full copy)."""
        price_change = df['close'].diff()
        gain = price_change.clip(lower=0)
        loss = (-price_change).clip(lower=0)

        avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

        rs  = avg_gain / avg_loss
        rsi = (100 - (100 / (1 + rs))).round(2).astype('float32')
        rsignal = rsi.ewm(span=period).mean().round(2).astype('float32')

        rsi_prev     = rsi.shift(1)
        rsignal_prev = rsignal.shift(1)

        bullish = (rsi > rsignal) & (rsi_prev < rsignal_prev)
        bearish = (rsi < rsignal) & (rsi_prev > rsignal_prev)

        df['rsi']      = rsi
        df['rsignal']  = rsignal
        df['crossover'] = np.where(bullish, "Bullish", np.where(bearish, "Bearish", "Neutral"))

        return df
