import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from flask import Flask
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
            'volume': quotes['volume']
        })
        
        # Clean data (remove NaN values)
        df = df.dropna()
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
    df['rsi'] = 100 - (100 / (1 + df['rs']))
    
    # Calculate Signal
    df['signal'] = calculate_ema(df['rsi'], period=14)
    
    return df

def calculate_ema(data, period):
    return data.ewm(span=period).mean()

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

@app.route("/")
def home():
    print("Fetching SPY data from Yahoo Finance...")
    
    # Fetch data
    symbol="SPY"
    df = fetch_stock_data(symbol, period="5d", interval="15m")
    
    if df is None:
        print("Failed to fetch data. Please check your internet connection.")
        return
    
    print(f"Successfully fetched {len(df)} days of data")
    
    # Calculate RSI
    df_with_rsi = calculate_rsi(df, period=14)    
    df_final = identify_crossovers(df_with_rsi)
    df_reversed = df_final[::-1]
                    
    ret_val = "<table border='1' cellspacing='1' cellpadding='1'><tr><td>Time</td><td>" + symbol + " Symbol</td><td>Flag</td>"    
    for date, row in df_reversed.iterrows():
        flag_val = "" 
        if (row['bullish_crossover']==True): 
            flag_val = "Bullish"
        elif (row['bearish_crossover']==True): 
            flag_val = "Bearish"
        if (flag_val != ""):
            ret_val += "<tr><td>" + date.strftime('%m-%d %H:%M') + "</td><td>" + f"{row['close']:.2f}" + "</td><td>" + flag_val + "</td></tr>"
    ret_val += "</table>"
    
    return ret_val

if __name__ == "__main__":
    # Run the analysis
    # spy_data = main()
    app.run(host='0.0.0.0', port=80) 