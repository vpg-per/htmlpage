import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from time import gmtime
from flask import Flask,json,render_template
app = Flask(__name__)

def fetch_stock_data(symbol="SPY", period="1y", interval="1d"):
    """
    Fetch SPY data from Yahoo Finance API
    
    Parameters:
    period: str - Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    interval: str - Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/" + symbol
    
    params = {
        'period1': int((datetime.now() - timedelta(days=5)).timestamp()),
        'period2': int(datetime.now().timestamp()),
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
        
        # Clean data (remove NaN values)
        df = df.dropna()
        df['rec_dt']= df['timestamp'].dt.date
        df['nmonth']= df['timestamp'].dt.strftime('%m')
        df['nday']= df['timestamp'].dt.strftime('%d')
        df['hour']= df['timestamp'].dt.strftime('%H')
        df['minute']= df['timestamp'].dt.strftime('%M')
        df.set_index('timestamp', inplace=True)
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error parsing data: {e}")
        return None

def calculate_rsi(df, period=14):
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

def identify_crossovers(df):
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

def calculate_rsi_signal(df, interval="5m"):
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
    
    df_with_rsi = calculate_rsi(df_signal, period=14)    
    df_final = identify_crossovers(df_with_rsi)
    df_sel_cols = df_final.loc[:, ['rec_dt', 'nmonth', 'nday', 'hour', 'minute', 'close', 'rsi', 'signal', 'bullish_crossover', 'bearish_crossover']]
    #df_filtered_rows = de_sel_cols = df_sel_cols[(df_sel_cols['bullish_crossover']==True) | (df_sel_cols['bearish_crossover']==True)]
    
    #if (interval=="15m"):
    #    return df_sel_cols
    #else:
    return df_sel_cols.tail(1)

def calculate_stock_signal(symbol="SPY", period="1y", interval="1d"):
    df = fetch_stock_data(symbol, period="5d", interval="5m")
    
    if df is None:
        print("Failed to fetch data. Please check your internet connection.")
        return
    
    TOKEN = "6746979446:AAFk8lDekzXRkHQG5MUJVdpx1P0orOpWW1g"
    chat_id = "802449612"
    message = ""

    interval="5m"
    alldatafs = {}
    df_5m = calculate_rsi_signal(df, interval)
    df_5m['interval'] = interval
    df_alltfs = df_5m.copy()
    
    interval="15m"
    df_15m = calculate_rsi_signal(df)
    df_15m['interval'] = interval
    if (df_15m['bullish_crossover'].iloc[-1]):
        message = "RSI buy crossover triggered at " + df_15m['hour'].iloc[-1] + "-" + df_15m['minute'].iloc[-1] + ", stock symbol: " + symbol + ", interval: " + interval
    elif (df_15m['bearish_crossover'].iloc[-1]):
        message = "****RSI sell crossover triggered at " + df_15m['hour'].iloc[-1] + "-" + df_15m['minute'].iloc[-1] + ", stock symbol: " + symbol + ", interval: " + interval
    df_alltfs = pd.concat([df_alltfs, df_15m], ignore_index=False)

    if (message != ""):
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        print(requests.get(url).json())
    
    interval="30m"
    df_30m = calculate_rsi_signal(df)
    df_30m['interval'] = interval
    df_alltfs = pd.concat([df_alltfs, df_30m], ignore_index=False)

    interval="1h"
    df_1h = calculate_rsi_signal(df)
    df_1h['interval'] = interval
    df_alltfs = pd.concat([df_alltfs, df_1h], ignore_index=False)

    interval="4h"
    df_4h = calculate_rsi_signal(df)
    df_4h['interval']=interval
    df_alltfs = pd.concat([df_alltfs, df_4h], ignore_index=False)
    
    return df_alltfs

@app.route('/returnPattern')
def ReturnPattern():
#def main():
    stocksymbols = ['QQQ']
    df_allsymbols = {}
    for ss in stocksymbols:  
        df_stock = calculate_stock_signal(ss, period="5d", interval="15m")
        df_stock['symbol'] = ss
        df_len = len(df_allsymbols)
        if df_len == 0:
            df_allsymbols = df_stock.copy()
        else:
            df_allsymbols = pd.concat([df_allsymbols, df_stock], ignore_index=False)
    #print(df_allsymbols)
    local_timezone = datetime.utcnow().astimezone().tzinfo

    print(local_timezone)
    
    return df_allsymbols.to_json(orient='records', index=False)

@app.route("/")
def index():
    return render_template('./index.html')

if __name__ == "__main__":
    # Run the analysis
    #spy_data = main()
    app.run(host='0.0.0.0', port=80) 
