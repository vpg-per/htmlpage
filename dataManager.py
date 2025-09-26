import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo
import os
import psycopg2

class ServiceManager:
    def __init__(self):
        
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        
        self._message = []
        self.token = os.getenv("TELE_TOKEN")
        self.chat_id = os.getenv("TELE_CHAT_ID")

    def fetch_stock_data(self, symbol, startPeriod, endPeriod, interval="1d"):
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

    def GetStockdata_Byinterval(self, symbol, interval="1d"):
        df_signal = { }
                
        stPeriod = int((datetime.now()- timedelta(days=5)).timestamp()) 
        endPeriod = datetime.now()
        minutes_to_subtract = endPeriod.minute % 5
        endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0)
        df = self.fetch_stock_data(symbol, stPeriod, endPeriod.timestamp(), interval)
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
        df_with_rsi = self.calculate_rsi(df_signal, period=14)    
        df_final = self.identify_crossovers(df_with_rsi)
        df_sel_cols = df_final.loc[:, ['unixtime', 'rec_dt', 'nmonth', 'nday', 'hour', 'minute', 'rsi', 'signal', 'crossover','open','close','high','low']]
        df_sel_cols['interval'] = interval
        df_sel_cols['symbol'] = symbol.replace("%3DF","")
                
        if ((interval == "15m") | (interval == "30m" ) ):
            self.check_forcrossover(df_sel_cols)
        
        return df_sel_cols.tail(1)

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
        df['bullish_crossover'] = ( (df['rsi'] > df['signal']) & (df['rsi_prev'] <= df['signal_prev'])  )
        # Bearish crossover: RSI crosses below Signal
        df['bearish_crossover'] = ( (df['rsi'] < df['signal']) &  (df['rsi_prev'] >= df['signal_prev']) )
        df['crossover'] = np.where(df['bullish_crossover'], "Bullish", np.where(df['bearish_crossover'], "Bearish", "Neutral"))

        return df

    def calculate_rsi_signal(self, symbol):
        df_merged = { }
        df_merged = self.GetStockdata_Byinterval(symbol, "5m")
        df_temp = self.GetStockdata_Byinterval(symbol, "15m")
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)
        df_temp = self.GetStockdata_Byinterval(symbol, "30m")
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)
        df_temp = self.GetStockdata_Byinterval(symbol, "1h")
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)
        df_temp = self.GetStockdata_Byinterval(symbol, "4h")
        df_merged=  pd.concat([df_merged, df_temp], ignore_index=True)

        return df_merged

    def check_forcrossover(self, df):
        # This alert is initiated for 15 or 30 minute time frame only

        for date, row in df.tail(1).iterrows():
            if (( row['crossover'] == "Bullish") | ( row['crossover'] == "Bearish")):
                if (self.isExistsinDB(row) == False):
                    tsignal = "buy" if (row['crossover'] == "Bullish") else "sell"
                    message = (f"{row['symbol']} {tsignal} signal on {row['interval']} analysis at {row['hour']}:{row['minute']}, o:{row['open']}, c: {row['close']};")
                    self._message.append( message )
                    self.AddRecordtoDB(row)

        return

    def send_chart_alert(self, s_message):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={s_message}"
        return requests.get(url).json()

    def get_message(self):
        return self._message
    
    def set_message(self, new_message):
        self._message = new_message

    def isExistsinDB(self, row):
        retval=False
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            dtlookupval = f"{row['nmonth']}-{row['nday']} {row['hour']}:{row['minute']}"
            with psycopg2.connect(conn_string) as conn:
                # Open a cursor to perform database operations
                with conn.cursor() as cur:
                    cur.execute("Select \"triggerTime\", \"interval\", \"crossover\" from rsicrossover where \"triggerTime\"=%s and \"interval\"=%s and \"stocksymbol\"=%s and \"NotificationSent\"=True; ", (dtlookupval, row['interval'], row['symbol'],))
                    if (cur.rowcount > 0 ):
                        retval = True
                cur.close()
            conn.close()
            return retval
            
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")

    def AddRecordtoDB(self, row):
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            dttimeval = f"{row['nmonth']}-{row['nday']} {row['hour']}:{row['minute']}"
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO rsicrossover (\"triggerTime\", \"interval\", \"crossover\", \"stocksymbol\", \"Open\", \"Close\", \"Low\", \"High\", \"NotificationSent\", \"rsiVal\", \"signal\") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                        (dttimeval, row['interval'], row['crossover'], row['symbol'], row['open'], row['close'], row['low'], row['high'], "TRUE", row['rsi'], row['signal'])
                    )
        
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")
        return

    def DelOldRecordsFromDB(self):
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            nowdt = datetime.now().date()- timedelta(days=1)
            dttimeval = f"%{nowdt.strftime('%m')}-{nowdt.strftime('%d')}%"
            delete_sql = "DELETE FROM rsicrossover WHERE \"triggerTime\" like %s;"
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(delete_sql, (dttimeval,))
        
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")
        return



