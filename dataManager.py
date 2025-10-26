import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo

class ServiceManager:
    def __init__(self):
                
        self.data5m = None
        self.data15m = None
        self.data30m = None
        self.data1h = None
        self.data4h = None

    def analyze_stockdata(self, symbol):
        todayn = int(datetime.now().strftime('%d'))-1
        todayn = str(todayn)
        df_merged = { }
        self.data5m = self.GetStockdata_Byinterval(symbol, "5m")
        df_merged = self.data5m[(self.data5m['nday'] == todayn) ].copy().tail(20)

        self.data15m = self.GetStockdata_Byinterval(symbol, "15m")
        self.data30m = self.GetStockdata_Byinterval(symbol, "30m")
        self.data1h = self.GetStockdata_Byinterval(symbol, "1h")

        df_signal = self.calculate_bollinger_bands(self.data15m, period=20, std_dev=2)
        df_signal['buyval'], df_signal['sellval'], df_signal['stoploss']= 0, 0, 0
        self.data15m = self.calculate_Buy_Sell_Values(df_signal)
        df_temp = self.data15m[(self.data15m['nday'] == todayn) ].copy().tail(12)
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)

        df_signal = self.calculate_bollinger_bands(self.data30m, period=20, std_dev=2)
        df_signal['buyval'], df_signal['sellval'], df_signal['stoploss']= 0, 0, 0
        self.data30m = self.calculate_Buy_Sell_Values(df_signal)
        df_temp = self.data30m[(self.data30m['nday'] == todayn) ].copy().tail(8)   
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)

        df_temp = self.data1h[(self.data1h['nday'] == todayn) ].copy().tail(4)
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)
        self.data4h = self.GetStockdata_Byinterval(symbol, "4h").tail(3)
        yesterdayn = (datetime.now() - timedelta(days=1)).strftime('%d')        
        df_temp = self.data4h[(self.data4h['nday'] == todayn) | (self.data4h['nday'] == yesterdayn)].copy()
        if (len(df_temp) == 0):
            df_temp = self.data4h.copy()
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)
        
        return df_merged

    def GetStockdata_Byinterval(self, symbol, interval="1d"):
        df_signal = { }
                
        stPeriod = int((datetime.now()- timedelta(days=5)).timestamp()) 
        endPeriod = datetime.now()
        minutes_to_subtract = endPeriod.minute % 5
        endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0)
        df = self.download_stock_data(symbol, stPeriod, endPeriod.timestamp(), interval)
        if df is None:
            print("Failed to fetch data. Please check your internet connection.")
            return
        if (interval=="5m"):
            df = df[(df['unixtime'] <= endPeriod.timestamp()) & (df['minute'].isin(["00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55"])) ]
        elif (interval=="15m"):
            minutes_to_subtract = endPeriod.minute % 15
            endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
            df = df[(df['unixtime'] <= endPeriod) & (df['minute'].isin(["00", "15", "30", "45"])) ]
        elif (interval=="30m"):
            minutes_to_subtract = endPeriod.minute % 30
            endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
            df = df[(df['unixtime'] <= endPeriod) & (df['minute'].isin(["00", "30"])) ]
        elif (interval=="1h"):
            df = df.resample('1h', origin='05:00:00-04:00').agg({
                'unixtime': 'first',                
                'open': 'first',   # First open in 1-hour period
                'high': 'max',     # Highest price in 1-hour period  
                'low': 'min',      # Lowest price in 1-hour period
                'close': 'last'   # Last close in 1-hour period
            }).dropna()
            endPeriod = endPeriod.replace(minute=0, second=0, microsecond=0).timestamp() - 1
            df = df[ (df['unixtime'] <= endPeriod) ]
            df['unixtime'] = pd.to_numeric(df['unixtime'])
            df['rec_dt']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.date
            df['nmonth']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%m')
            df['nday']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%d')
            df['hour']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%H')
            df['minute']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%M')
        elif (interval=="4h"):
            df = df[ (df['minute'].isin(["00"])) ]
            hours_to_subtract = (endPeriod.hour % 4)
            endPeriod = endPeriod.replace(hour=endPeriod.hour - hours_to_subtract, minute=0, second=0, microsecond=0).timestamp() - 1
            df = df[ ( df['unixtime'] <= endPeriod) ]
            df = df.resample('4h', origin='05:00:00-04:00').agg({
                'unixtime': 'first',                
                'open': 'first',   # First open in 4-hour period
                'high': 'max',     # Highest price in 4-hour period  
                'low': 'min',      # Lowest price in 4-hour period
                'close': 'last'   # Last close in 4-hour period
            }).dropna()
            df['unixtime'] = pd.to_numeric(df['unixtime'])
            df['rec_dt']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.date
            df['nmonth']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%m')
            df['nday']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%d')
            df['hour']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%H')
            df['minute']= pd.to_datetime(df['unixtime'], unit='s').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%M')
            df['hour'] = np.where(df['hour']=="18", "17", df['hour'])

        df_signal = df.copy()
        df_signal = self.calculate_macd(df_signal)
        df_with_rsi = self.calculate_rsi(df_signal, period=14)
        df_final = self.calculate_crossover(df_with_rsi)
        
        df_sel_cols = df_final.loc[:, ['unixtime', 'rec_dt', 'nmonth', 'nday', 'hour', 'minute', 'rsi', 'rsignal', 'macd', 'msignal', 'crossover','open','close','high','low']]
        df_sel_cols['interval'] = interval
        df_sel_cols['symbol'] = symbol.replace("%3DF","")
            
        return df_sel_cols

    def download_stock_data(self, symbol, startPeriod, endPeriod, interval="1d"):
        """
        Fetch SPY data from Yahoo Finance API
        
        Parameters:
        period: str - Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        interval: str - Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        """        
        if (interval == "4h" or interval == "1h"):
            interval = "30m"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/" + symbol
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
            
            # Extract price data
            result = data['chart']['result'][0]
            timestamps = result['timestamp']
            quotes = result['indicators']['quote'][0]
            
            # Create DataFrame
            df = pd.DataFrame({
                'unixtime': timestamps,
                'timestamp': [datetime.fromtimestamp(ts, ZoneInfo("America/New_York")) for ts in timestamps],
                'open': quotes['open'],
                'high': quotes['high'],
                'low': quotes['low'],
                'close': quotes['close'],
                # 'volume': quotes['volume'],            
            })
            # Clean data (remove NaN values)
            df = df.dropna()

            df['rec_dt']= df['timestamp'].dt.date
            df['nmonth']= df['timestamp'].dt.strftime('%m')
            df['nday']= df['timestamp'].dt.strftime('%d')
            df['hour']= df['timestamp'].dt.strftime('%H')
            df['minute']= df['timestamp'].dt.strftime('%M')
            df['close'] = round(df['close'], 2)
            df['open'] = round(df['open'], 2)
            df['high'] = round(df['high'], 2)
            df['low'] = round(df['low'], 2)
            df.set_index('timestamp', inplace=True)
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except KeyError as e:
            print(f"Error parsing data: {e}")
            return None

    def calculate_rsi(self, df, period=14):
        """
        Calculate RSI (Relative Strength Index)
        
        Parameters:
        df: DataFrame with 'close' column
        period: RSI calculation period (default 14)
        
        Returns:
        DataFrame with RSI column added
        """
        df = df.copy()        
        # Calculate price changes
        df['price_change'] = df['close'].diff()        
        # Separate gains and losses
        df['gain'] = df['price_change'].where(df['price_change'] > 0, 0)
        df['loss'] = -df['price_change'].where(df['price_change'] < 0, 0)        
        # Calculate average gain and average loss using Wilder's smoothing
        df['avg_gain'] = df['gain'].ewm(alpha=1/period, adjust=False).mean()
        df['avg_loss'] = df['loss'].ewm(alpha=1/period, adjust=False).mean()        
        # Calculate Relative Strength (RS)
        df['rs'] = df['avg_gain'] / df['avg_loss']        
        # Calculate RSI
        df['rsi'] = round(100 - (100 / (1 + df['rs'])),2)        
        # Calculate Signal
        df['rsignal'] = round(df['rsi'].ewm(span=period).mean(),2)
        
        return df

    def calculate_macd(self, df, fast=12, slow=26, signal=9):
        """
        Calculate MACD line and Signal line
        
        Parameters:
        - df: DataFrame with 'Close' prices
        - fast: Fast EMA period (default 12)
        - slow: Slow EMA period (default 26)
        - signal: Signal line EMA period (default 9)
        
        Returns:
        - DataFrame with MACD, Signal, and Histogram columns
        """
        # Calculate EMAs
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
                
        # Add to dataframe
        df['macd'] = macd_line
        df['msignal'] = signal_line
        df['histogram'] = macd_line - signal_line
        
        return df
        
    def calculate_crossover(self, df):
        df = df.copy()        
        # Calculate crossover signals
        df['rsi_prev'] = df['rsi'].shift(1)
        df['rsignal_prev'] = df['rsignal'].shift(1)
        # Bullish crossover: RSI crosses above Signal
        df['bullish_crossover'] = ( (df['rsi'] > df['rsignal']) & (df['rsi_prev'] < df['rsignal_prev']) & (df['histogram'] > 0)  )
        # Bearish crossover: RSI crosses below Signal
        df['bearish_crossover'] =  ( (df['rsi'] < df['rsignal']) &  (df['rsi_prev'] > df['rsignal_prev']) & (df['histogram'] < 0) )
        df['crossover'] = np.where(df['bullish_crossover'], "Bullish", np.where(df['bearish_crossover'], "Bearish", "Neutral"))

        return df

    def calculate_Buy_Sell_Values(self, df):
        todayn = int(datetime.now().strftime('%d')) - 1
        todayn = str(todayn)
        for i in range(1, len(df)):
            if ( df['nday'].iloc[i] == todayn ):
                if (df['crossover'].iloc[i] == "Bullish" ):
                    sel_rechr = df['hour'].iloc[i]
                    sel_recinmin = df['minute'].iloc[i]
                    if (df['interval'].iloc[i]=="15m"):
                        if (sel_recinmin == "00" or sel_recinmin == "15" ):
                            sel_recinmin = "30"
                            sel_rechr = str( int (df['hour'].iloc[i] ) - 1)
                        elif (sel_recinmin == "30" or sel_recinmin == "45"):
                            sel_recinmin = "00"
                        df_matched_recs = self.data30m[ (self.data30m['nday'] == df['nday'].iloc[i]) & (self.data30m['hour'] == sel_rechr) & (self.data30m['minute'] == sel_recinmin) ]
                    elif (df['interval'].iloc[i]=="30m"):
                        sel_rechr = str( int (df['hour'].iloc[i] ) - 1)
                        df_matched_recs = self.data1h[ (self.data1h['nday'] == df['nday'].iloc[i]) & (self.data1h['hour'] == sel_rechr) ]
                    
                    if (len(df_matched_recs) > 0):
                        if ( float( df_matched_recs['macd'].iloc[0]) > float( df_matched_recs['msignal'].iloc[0])):
                            df['buyval'].iloc[i] = df['midbnd'].iloc[i]
                            df['sellval'].iloc[i] = df['ubnd'].iloc[i]
                            df['stoploss'].iloc[i] = df['lbnd'].iloc[i]
                elif (df['crossover'].iloc[i] == "Bearish" ):
                    sel_rechr = df['hour'].iloc[i]
                    sel_recinmin = df['minute'].iloc[i]
                    if (df['interval'].iloc[i]=="15m"):
                        if (sel_recinmin == "00" or sel_recinmin == "15" ):
                            sel_recinmin = "30"
                            sel_rechr = str( int (df['hour'].iloc[i] ) - 1)
                        elif (sel_recinmin == "30" or sel_recinmin == "45"):
                            sel_recinmin = "00"
                        df_matched_recs = self.data30m[ (self.data30m['nday'] == df['nday'].iloc[i]) & (self.data30m['hour'] == sel_rechr) & (self.data30m['minute'] == sel_recinmin) ]
                    elif (df['interval'].iloc[i]=="30m"):
                        sel_rechr = str( int (df['hour'].iloc[i] ) - 1)
                        df_matched_recs = self.data1h[ (self.data1h['nday'] == df['nday'].iloc[i]) & (self.data1h['hour'] == sel_rechr) ]
                    
                    if (len(df_matched_recs) > 0 ):
                        if ( float( df_matched_recs['macd'].iloc[0]) < float( df_matched_recs['msignal'].iloc[0])):
                            df['buyval'].iloc[i] = df['midbnd'].iloc[i]
                            df['sellval'].iloc[i] = df['lbnd'].iloc[i]
                            df['stoploss'].iloc[i] = df['ubnd'].iloc[i]

        df.drop(['midbnd', 'ubnd', 'lbnd'], axis=1, inplace=True)
        return df

    def calculate_bollinger_bands(self, df, period=20, std_dev=2):
        
        # Calculate Middle Band (SMA)
        df['midbnd'] = df['close'].rolling(window=period).mean()
        df['stddev'] = df['close'].rolling(window=period).std()
        df['ubnd'] = round(df['midbnd'] + (std_dev * df['stddev']), 2)
        df['lbnd'] = round(df['midbnd'] - (std_dev * df['stddev']), 2)
        df['midbnd'] = round(df['midbnd'], 2)
        return df

    def identify_candlestick_patterns(self, data):
        """Identifies common candlestick patterns in the data."""
        
        if len(data) < 3:
            return data
        
        todayn = datetime.now().strftime('%d')
        for i in range(1, len(data)):
            if ( data['nday'].iloc[i] == todayn ):
                o, h, l, c = data['open'].iloc[i], data['high'].iloc[i], data['low'].iloc[i], data['close'].iloc[i]
                o_prev, h_prev, l_prev, c_prev = data['open'].iloc[i-1], data['high'].iloc[i-1], data['low'].iloc[i-1], data['close'].iloc[i-1]
                o_prev_2, c_prev_2 = data['open'].iloc[i-2], data['close'].iloc[i-3]
                
                body = abs(c - o)
                price_range = h - l
            
                # --- Single-bar patterns ---            
                # Doji (small body)
                if price_range > 0 and body / price_range < 0.1:
                    data['pattern'].iloc[i] = 'Dj'
                # Marubozu (strong momentum)
                if price_range > 0 and body / price_range > 0.95:
                    if c > o:
                        data['pattern'].iloc[i] = 'UM'
                    else:
                        data['pattern'].iloc[i] = 'EM'

                # --- Two-bar patterns ---
                data['pattern2c'].iloc[i] = 'NA'
                # Bullish Engulfing
                if c > o and l > l_prev and c > o_prev and body / price_range > 0.95:
                    data['pattern2c'].iloc[i] = 'UE'
                # Bearish Engulfing
                if c < o and h < h_prev and c < o_prev and body / price_range > 0.95:
                    data['pattern2c'].iloc[i] = 'EE'

                # --- Three-bar patterns ---
            
        return data
