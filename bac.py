import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from flask import Flask
app = Flask(__name__)

def fetch_spy_data(period="1y", interval="1d"):
    """
    Fetch SPY data from Yahoo Finance API
    
    Parameters:
    period: str - Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    interval: str - Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    """
    #url = f"https://query1.finance.yahoo.com/v8/finance/chart/SPY"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF"
    
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

def calculate_ema(data, period):
    """Calculate Exponential Moving Average"""
    return data.ewm(span=period).mean()

def calculate_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """
    Calculate MACD (Moving Average Convergence Divergence)
    
    Parameters:
    df: DataFrame with 'close' column
    fast_period: Fast EMA period (default 12)
    slow_period: Slow EMA period (default 26)
    signal_period: Signal line EMA period (default 9)
    
    Returns:
    DataFrame with MACD, Signal, and Histogram columns
    """
    df = df.copy()
    
    # Calculate EMAs
    df['ema_fast'] = calculate_ema(df['close'], fast_period)
    df['ema_slow'] = calculate_ema(df['close'], slow_period)
    
    # Calculate MACD line
    df['macd'] = df['ema_fast'] - df['ema_slow']
    
    # Calculate Signal line
    df['signal'] = calculate_ema(df['macd'], signal_period)
    
    # Calculate Histogram
    df['histogram'] = df['macd'] - df['signal']
    
    return df

def identify_crossovers(df):
    """
    Identify MACD crossover points
    
    Returns:
    DataFrame with crossover signals
    """
    df = df.copy()
    
    # Calculate crossover signals
    df['macd_prev'] = df['macd'].shift(1)
    df['signal_prev'] = df['signal'].shift(1)
    
    # Bullish crossover: MACD crosses above Signal
    df['bullish_crossover'] = (
        (df['macd'] > df['signal']) & 
        (df['macd_prev'] <= df['signal_prev'])
    )
    
    # Bearish crossover: MACD crosses below Signal
    df['bearish_crossover'] = (
        (df['macd'] < df['signal']) & 
        (df['macd_prev'] >= df['signal_prev'])
    )
    
    return df

def print_crossover_summary_1(df):
    """Print summary of crossover signals"""
    print("MACD CROSSOVER ANALYSIS FOR SPY")
    
    
    for date, row in df.iterrows():
        if (row['bullish_crossover']):
            print(f"{date.strftime('%m-%d %H:%M')}\t${row['close']:.2f}" + "\tBullish")
        elif (row['bearish_crossover']):
            print(f"{date.strftime('%m-%d %H:%M')}\t${row['close']:.2f}" + "\t--Bearish")

def plot_macd_analysis(df):
    """Plot price and MACD with crossover signals"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), 
                                   gridspec_kw={'height_ratios': [2, 1]})
    
    # Plot price
    ax1.plot(df.index, df['close'], label='SPY Close', linewidth=1.5, color='black')
    
    # Mark crossover points on price chart
    bullish_dates = df[df['bullish_crossover']].index
    bearish_dates = df[df['bearish_crossover']].index
    
    ax1.scatter(bullish_dates, df.loc[bullish_dates, 'close'], 
               color='green', marker='^', s=100, label='Bullish Crossover', zorder=5)
    ax1.scatter(bearish_dates, df.loc[bearish_dates, 'close'], 
               color='red', marker='v', s=100, label='Bearish Crossover', zorder=5)
    
    ax1.set_title('SPY Price with MACD Crossover Signals', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Price ($)', fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot MACD
    ax2.plot(df.index, df['macd'], label='MACD', color='blue', linewidth=1.5)
    ax2.plot(df.index, df['signal'], label='Signal', color='red', linewidth=1.5)
    ax2.bar(df.index, df['histogram'], label='Histogram', alpha=0.3, color='gray')
    
    # Mark crossover points on MACD chart
    ax2.scatter(bullish_dates, df.loc[bullish_dates, 'macd'], 
               color='green', marker='^', s=100, zorder=5)
    ax2.scatter(bearish_dates, df.loc[bearish_dates, 'macd'], 
               color='red', marker='v', s=100, zorder=5)
    
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax2.set_title('MACD Indicator', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Date', fontsize=12)
    ax2.set_ylabel('MACD', fontsize=12)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

def print_crossover_summary(df):
    """Print summary of crossover signals"""
    bullish_crossovers = df[df['bullish_crossover']]
    bearish_crossovers = df[df['bearish_crossover']]
    
    print("\n" + "="*60)
    print("MACD CROSSOVER ANALYSIS FOR SPY")
    print("="*60)
    
    print(f"\nData Period: {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")
    print(f"Total Trading Days: {len(df)}")
    
    print(f"\n📈 BULLISH CROSSOVERS: {len(bullish_crossovers)}")
    if len(bullish_crossovers) > 0:
        print("Date\t\tPrice\t\tMACD\t\tSignal")
        print("-" * 60)
        for date, row in bullish_crossovers.tail(10).iterrows():
            print(f"{date.strftime('%m-%d %H:%M')}\t${row['close']:.2f}\t\t{row['macd']:.4f}\t\t{row['signal']:.4f}")
    
    print(f"\n📉 BEARISH CROSSOVERS: {len(bearish_crossovers)}")
    if len(bearish_crossovers) > 0:
        print("Date\t\tPrice\t\tMACD\t\tSignal")
        print("-" * 60)
        for date, row in bearish_crossovers.tail(10).iterrows():
            print(f"{date.strftime('%m-%d %H:%M')}\t${row['close']:.2f}\t\t{row['macd']:.4f}\t\t{row['signal']:.4f}")
    
    # Recent status
    latest = df.iloc[-1]
    if latest['macd'] > latest['signal']:
        trend = "BULLISH (MACD above Signal)"
    else:
        trend = "BEARISH (MACD below Signal)"
    
    print(f"\n📊 CURRENT STATUS ({df.index[-1].strftime('%m-%d %H:%M')}):")
    print(f"Price: ${latest['close']:.2f}")
    print(f"MACD: {latest['macd']:.4f}")
    print(f"Signal: {latest['signal']:.4f}")
    print(f"Trend: {trend}")


@app.route("/")
def home():
    #def main():
    print("Fetching SPY data from Yahoo Finance...")
    
    # Fetch data
    df = fetch_spy_data(period="5d", interval="15m")
    
    if df is None:
        print("Failed to fetch data. Please check your internet connection.")
        return
    
    print(f"Successfully fetched {len(df)} days of data")
    
    # Calculate MACD
    df_with_macd = calculate_macd(df)
    
    # Identify crossovers
    df_final = identify_crossovers(df_with_macd)
    df_reversed = df_final[::-1]
    # Print summary
    #print_crossover_summary(df_final)
    #print_crossover_summary_1(df_final)
    
    ret_val = "<div>This is test data to be shown on the web page</div><table border='1' cellspacing='1' cellpadding='1'>"
    
    for date, row in df_reversed.iterrows():
        flag_val = "Bullish" if row['bullish_crossover'] else "Bearish"
        if (row['bullish_crossover']):
            ret_val += "<tr><td>" + date.strftime('%m-%d %H:%M') + "</td><td>" + f"{row['close']:.2f}" + "</td><td>Bullish</td></tr>"
        elif (row['bearish_crossover']):
            ret_val += "<tr><td>" + date.strftime('%m-%d %H:%M') + "</td><td>" + f"{row['close']:.2f}" + "</td><td>Bearish</td></tr>"
    ret_val += "</table>"
    
    return ret_val

if __name__ == "__main__":
    # Run the analysis
    # spy_data = main()
    app.run(host='0.0.0.0', port=80) 