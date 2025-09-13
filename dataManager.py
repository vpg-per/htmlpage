import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo

class ServiceManager:
    def __init__(self):
        self._message = []
        self.token = "6746979446:AAFk8lDekzXRkHQG5MUJVdpx1P0orOpWW1g"
        self.chat_id = "802449612"

    def fetch_stock_data(self, symbol, startPeriod, endPeriod, interval="1d"):
        """
        Fetch SPY data from Yahoo Finance API
        
        Parameters:
        period: str - Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        interval: str - Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        """
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
                'timestamp': [datetime.fromtimestamp(ts) for ts in timestamps],
                'open': quotes['open'],
                'high': quotes['high'],
                'low': quotes['low'],
                'close': quotes['close'],
                'volume': quotes['volume'],            
            })
            # first_timestamp_tz = df['timestamp'].iloc[0].tz
            # Clean data (remove NaN values)
            df = df.dropna()

            timezone_name = datetime.now().astimezone().tzname()
            if ("UTC" in timezone_name):
                df['rec_dt']= df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            else:
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
        df['signal'] = round(df['rsi'].ewm(span=period).mean(),2)
        
        return df

    def identify_crossovers(self, df):
        """
        Identify RSI crossover points
        
        Returns:
        DataFrame with crossover signals
        """
        df = df.copy()
        
        # Calculate crossover signals
        df['rsi_prev'] = df['rsi'].shift(1)
        df['signal_prev'] = df['signal'].shift(1)
        
        # Bullish crossover: RSI crosses above Signal
        df['bullish_crossover'] = (
            (df['rsi'] > df['signal']) & 
            (df['rsi_prev'] <= df['signal_prev'])
        )
        
        # Bearish crossover: RSI crosses below Signal
        df['bearish_crossover'] = (
            (df['rsi'] < df['signal']) & 
            (df['rsi_prev'] >= df['signal_prev'])
        )
        
        return df

    def calculate_rsi_signal(self, symbol, df, interval="5m"):
        df_signal = { }

        if (interval=="5m"):
            df_signal = df.copy()
        elif (interval=="15m"):
            df_signal = df[df['minute'].isin(["00", "15", "30", "45"])]
        elif (interval=="30m"):
            df_signal = df[df['minute'].isin(["00", "30"])]
        elif (interval=="1h"):
            df_signal = df[df['minute'].isin(["00"])]
        elif (interval=="4h"):
            df_signal = df[(df['hour'].isin(["00", "04", "08", "12", "16", "20", "24"]) & (df['minute'].isin(["00"])))]
        
        df_with_rsi = self.calculate_rsi(df_signal, period=14)    
        df_final = self.identify_crossovers(df_with_rsi)
        df_sel_cols = df_final.loc[:, ['rec_dt', 'nmonth', 'nday', 'hour', 'minute', 'rsi', 'signal', 'bullish_crossover', 'bearish_crossover','open','close','high','low']]
        df_sel_cols['interval'] = interval
        df_sel_cols['symbol'] = symbol.replace("%3DF","")
        #df_filtered_rows = de_sel_cols = df_sel_cols[(df_sel_cols['bullish_crossover']==True) | (df_sel_cols['bearish_crossover']==True)]
        
        if ((interval == "15m") | (interval == "30m" )):
            self.check_forcrossover(df_sel_cols)

        return df_sel_cols.tail(1)

    def check_forcrossover(self, df):
        # This alert is initiated for 15 or 30 minute time frame only

        for date, row in df.tail(1).iterrows():
            if (( row['bullish_crossover'] == True) | ( row['bearish_crossover'] == True)):
                if (( row['bullish_crossover'] == True) ):
                    message = (f"RSI buy signal for {row['symbol']} stock at {row['hour']}:{row['minute']}UTC, open:{row['open']}, close: {row['close']}, low: {row['low']}, high: {row['high']}, interval: {row['interval']};")
                    self._message.append( message )
                elif ( ( row['bearish_crossover'] == True)):
                    message = (f"RSI sell signal for {row['symbol']} stock at {row['hour']}:{row['minute']}UTC, open:{row['open']}, close: {row['close']}, low: {row['low']}, high: {row['high']}, interval: {row['interval']};")
                    self._message.append( message )

        return

    def send_chart_alert(self, s_message):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={s_message}"
        return requests.get(url).json()

    def get_message(self):
        return self._message







