from flask import Flask,json,render_template
import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np
import yfinance as yf
import requests

app = Flask(__name__) 

pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Initialise the data 
fast_MA = 5
slow_MA = 9
initial_wealth = '1000'
#STK = input("Enter share name : ")
stocksymbols = ['SPY','QQQ','IWM','DIA','SMH']

from datetime import date, timedelta
today = date.today() + timedelta(days=1)
end_date = today.strftime("%Y-%m-%d")
prevdayrange = today- timedelta(days=4)
start_date = prevdayrange.strftime("%Y-%m-%d")   

all_df = pd.DataFrame(columns=['Symbol', 'History'])

@app.route('/') 
def ReturnHome():
    if len(all_df) <= 0:
        DownloadStockData()
    return render_template('data.html')

@app.route('/returnStrategy') 
def ReturnStrategy():
    if len(all_df) <= 0:
        DownloadStockData() 
    data = getStrategySummary()
    response = app.response_class(response=json.dumps(data),status=200,mimetype='application/json')
    return response

def getStrategySummary():
    obj = StockDataCustomStrategy("5m")
    df1 = obj.bar_with_Strategy()
    return df1.to_json(orient='records', index=False)
    
@app.route('/returnPattern')
def ReturnPattern():
    if len(all_df) <= 0:
        DownloadStockData()
    obj1 = StockDataPattern("5m")
    df1 = obj1.bar_with_pattern()
    data = df1.to_json(orient='records', index=False)
    response = app.response_class(response=json.dumps(data),status=200,mimetype='application/json')
    return response

def DownloadStockData():
    for x in stocksymbols:    
        sdf = yf.download(tickers=x, period='1d', interval='5m', start=start_date, end = end_date)
        sdf.reset_index(inplace=True) 
        sdf['Datetime'] = pd.to_datetime(sdf['Datetime']).dt.strftime('%H:%M')
        sdf[['Open','High','Low','Close','Adj Close']] = sdf[['Open','High','Low','Close','Adj Close']].round(2)
        all_df.loc[len(all_df)] = [x, sdf]
    
class StockDataCustomStrategy:
  
    # The init method or constructor
    def __init__(self, interval): 
        # Instance Variable
        self.interval = interval

    def getStockData(self, stock, dwl_df):
        dwl_df['fast_MA'] = dwl_df['Close'].ewm(span=fast_MA, adjust=False).mean()
        dwl_df['slow_MA'] = dwl_df['Close'].ewm(span=slow_MA, adjust=False).mean()
        dwl_df[['fast_MA','slow_MA']] = dwl_df[['fast_MA','slow_MA']].round(2)
        dwl_df['intersectionpoint'] = np.where(dwl_df['fast_MA'] > dwl_df['slow_MA'], 1.0, 0.0)
        dwl_df['Crossover'] = dwl_df['intersectionpoint'].diff()
        filtered_col_df = dwl_df[['Datetime','Open','High','Low','Close','Crossover']].copy()
        rslt_df = filtered_col_df[filtered_col_df['Crossover'] != 0.0] 
        rslt_df.insert(0,'Symbol',stock) 
        return rslt_df.tail(4)

    def bar_with_Strategy(self):
        appended_data = pd.DataFrame()
        for index, row in all_df.iterrows():
            data = self.getStockData(row['Symbol'], row['History'])
            appended_data = appended_data._append(data,ignore_index=True)
        appended_data = appended_data.sort_values(by='Datetime',ascending=[False])
        return appended_data

class StockDataPattern:
    # The init method or constructor
    def __init__(self, interval): 
        # Instance Variable
        self.interval = interval
        
    def getStockData(self, stock, dwl_5m):
        new = [[stock,dwl_5m['Datetime'].iloc[-1],dwl_5m['High'].max(),dwl_5m['High'].min(),dwl_5m['High'].iloc[-1],dwl_5m['Low'].iloc[-1]]]
        rslt_df = pd.DataFrame(new, columns=['Symbol','Datetime','Day_High','Day_Low','LastRow_High','LastRow_Low'])
        return rslt_df
        
    def bar_with_pattern(self):
        appended_data = pd.DataFrame()
        for index, row in all_df.iterrows():
            data = self.getStockData(row['Symbol'], row['History'])
            appended_data = appended_data._append(data,ignore_index=True)
        appended_data = appended_data.sort_values(by='Datetime',ascending=[False])
        return appended_data



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)  
