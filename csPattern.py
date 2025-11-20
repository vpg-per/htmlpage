import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo
from dataManager import ServiceManager
from itertools import islice
import io

class csPattern:
    def __init__(self):
        self.objMgr = ServiceManager()
        self.data5m = None
        self.data15m = None
        self.data30m = None
        self.openorderon5m = None
        self.closeorderon5m = None

    def analyze_stockcandles(self, symbol):
        todayn = datetime.now().strftime('%d')   

        self.data5m = self.invoke_downloadcall(symbol, "5m")
        self.data5m = self.identify_candlebreakout_pattern(self.data5m)
        self.data5m['ninemaval'] = round(self.data5m['close'].rolling(window=9).mean(), 2)

        self.data15m = self.invoke_downloadcall(symbol, "15m")
        self.data15m = self.identify_candlebreakout_pattern(self.data15m)
        self.data30m = self.invoke_downloadcall(symbol, "30m")
        self.data30m = self.identify_candlebreakout_pattern(self.data30m)

        #self.ResettoSampleData()   
        loadedFromDB = True
        if (self.openorderon5m is None):
            self.parse_stockdataintervalforOpen()
            loadedFromDB = False

        if (self.openorderon5m is not None and loadedFromDB):
            self.parse_stockdataintervalforClose()
        
        return

    def parse_stockdataintervalforOpen(self):
        df = self.data30m.copy()
        sub_df15m, sub_df5m = None, None
                
        if df is not None:
            last_but_one_30mrow = df.iloc[-2].copy()
            last_30mrow = df.iloc[-1].copy()
            if ( last_30mrow['cspattern'] != last_but_one_30mrow['cspattern'] and last_30mrow['cspattern'] != "Neutral" ):
                sub_df15m = self.data15m[ self.data15m['unixtime'].astype(int) >= int(last_but_one_30mrow['unixtime'] ) ]
                if ( len(sub_df15m) > 1):
                    for i in range(1, len(sub_df15m)):
                        last_but_one_15mrow = sub_df15m.iloc[i-1].copy()
                        last_15mrow = sub_df15m.iloc[i].copy()
                        if (last_15mrow['cspattern'] != last_but_one_15mrow['cspattern'] and last_15mrow['cspattern'] != "Neutral" and 
                            last_15mrow['cspattern'] == last_30mrow['cspattern']):
                            sub_df5m = self.data5m[ (self.data5m['unixtime'].astype(int) >= int(last_but_one_15mrow['unixtime'])) ]
                            if ( len(sub_df5m) > 1):
                                for j in range(len(sub_df5m)):
                                    last_5mrow = sub_df5m.iloc[j].copy()
                                    if (last_5mrow['cspattern'] == last_15mrow['cspattern']):
                                        stoploss = last_15mrow['low']
                                        profittarget = last_15mrow['close']
                                        if ( last_15mrow['cspattern'] == "Bearish"):
                                            stoploss = last_15mrow['high']
                                        self.openorderon5m = {"symbol": last_5mrow['symbol'], "stockprice": float(last_5mrow['open']), "cspattern": last_5mrow['cspattern'],
                                            "unixtime": int(last_5mrow['unixtime']), 'stoploss': float(stoploss), 'profittarget': float(profittarget),
                                            'hour': int(last_5mrow['hour']), 'minute': int(last_5mrow['minute']), "updatedTriggerTime" : int(last_5mrow['unixtime'])}
                                        break
                        if (self.openorderon5m is not None):
                            break

        return

    def parse_stockdataintervalforClose(self):
        
        sub_df15m = self.data15m[ (self.data15m['unixtime'].astype(int) >= int(self.openorderon5m['updatedTriggerTime'])) ].copy()
        if ( len(sub_df15m) <= 1):
            sub_df15m = self.data15m.tail(2).copy()
        if (len(sub_df15m) > 1):
            last_but_one_15mrow = sub_df15m.iloc[-2].copy()
            last_15mrow = sub_df15m.iloc[-1].copy()
            dfdata_5m = self.data5m[ (self.data5m['unixtime'].astype(int) > int(self.openorderon5m['updatedTriggerTime'])) ]
            if (len(dfdata_5m) <= 1):
                dfdata_5m = self.data5m.tail(2).copy()
        
            if (len(dfdata_5m) > 1):
                last_but_one_5mrow = dfdata_5m.iloc[-2].copy()
                last_5mrow = dfdata_5m.iloc[-1].copy()
                if (last_5mrow['cspattern'] == last_but_one_5mrow['cspattern'] and last_5mrow['cspattern'] == self.openorderon5m['cspattern']):
                    self.openorderon5m['hour'] = int(last_5mrow['hour'])
                    self.openorderon5m['minute'] = int(last_5mrow['minute'])
                    self.openorderon5m['updatedTriggerTime'] = int(last_5mrow['unixtime'])
                    self.openorderon5m['cstwopattern'] = last_5mrow['cstwopattern']
                    self.openorderon5m['csfvgpattern'] = last_5mrow['csfvgpattern']
                    if (self.openorderon5m['cspattern'] == "Bullish"):
                        self.openorderon5m['profittarget'] = float(last_15mrow['high'])
                        self.openorderon5m['stoploss'] = float(last_15mrow['low'])
                    elif (self.openorderon5m['cspattern'] == "Bearish"):
                        self.openorderon5m['profittarget'] = float(last_15mrow['low'])
                        self.openorderon5m['stoploss'] = float(last_15mrow['high'])
                else:
                    self.closeorderon5m = {"symbol": last_5mrow['symbol'], "stockprice": float(last_5mrow['open']), "cspattern": last_5mrow['cspattern'],
                        "unixtime": int(last_5mrow['unixtime']), 'stoploss': "0", 'profittarget': "0", 'hour': int(last_5mrow['hour']), 'minute': int(last_5mrow['minute'])}
                    
        return
    
    def identify_candlebreakout_pattern(self, df, engulfFlag=True, fvgFlag=True):
        df['cspattern'] = "Neutral"
        if (engulfFlag):
            df['cstwopattern'] = "NA"
        if (fvgFlag):
            df['csfvgpattern'] = "NA"
        for i in range(1, len(df)):    
            o, h, l, c = df['open'].iloc[i], df['high'].iloc[i], df['low'].iloc[i], df['close'].iloc[i]
            p1_open, p1_high, p1_low, p1_close = df['open'].iloc[i-1], df['high'].iloc[i-1], df['low'].iloc[i-1], df['close'].iloc[i-1]       
            signal = df['cspattern'].iloc[i-1]
            
            # Candlestick sentiment change
            if c > p1_high:
                signal = "Bullish"
            elif c < p1_low:
                signal = "Bearish"
            df.loc[df.index[i], 'cspattern'] = signal

            # Bullish Engulfing
            if (engulfFlag):
                if c > o and p1_close < p1_open and c > p1_open and o < p1_close:
                    df.loc[df.index[i], 'cstwopattern'] = "UlEngulf"
                # Bearish Engulfing
                elif c < o and p1_close > p1_open and c < p1_open and o > p1_close:
                    df.loc[df.index[i], 'cstwopattern'] = "EaEngulf"
            
            if (i > 1 and fvgFlag):  
                p2_high, p2_low = df['high'].iloc[i-2], df['low'].iloc[i-2]
                if p2_low > h:
                    df.loc[df.index[i], 'csfvgpattern'] = "BearishFVG"
                if p2_high < l:
                    df.loc[df.index[i], 'csfvgpattern'] = "BullishFVG"
        
        return df

    def invoke_downloadcall(self, symbol, interval="15m", rsiFlag=True):
                
        stPeriod = int((datetime.now()- timedelta(days=2)).timestamp()) 
        endPeriod = datetime.now()
        minutes_to_subtract = endPeriod.minute % 5
        endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0)
        df = self.objMgr.download_stock_data(symbol, stPeriod, endPeriod.timestamp(), interval)

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

        df['symbol']=symbol
        df['interval']=interval
        if (rsiFlag):
            df = self.objMgr.calculate_rsi(df, period=14)
            df = df.loc[:, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval']]
        else:
            df = df.loc[:, ['unixtime', 'nmonth', 'nday', 'hour', 'minute', 'open','close','high','low', 'symbol', 'interval']]

        return df

    def ResettoSampleData(self):

        print(self.data30m.loc[self.data30m['unixtime'].astype(int) <= 1763566200, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        print(self.data15m.loc[self.data15m['unixtime'].astype(int) <= 1763566200, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        print(self.data5m.loc[self.data5m['unixtime'].astype(int) <= 1763566200, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern', 'ninemaval']].tail(42).to_string(index=False))

        data_30m = """
        1763560800     11   19   09     00 44.78    53.21 6652.75 6642.50 6654.25 6641.25 ES%3DF      30m   Bearish           NA           NA
        1763562600     11   19   09     30 66.79    55.02 6642.50 6691.75 6693.75 6635.75 ES%3DF      30m   Bullish           NA           NA
        """
        self.data30m = self.sampledata_toDF(data_30m, False)

        data_15m = """
        1763560800     11   19   09     00 40.75    54.33 6652.75 6643.50 6654.25 6643.00 ES%3DF      15m   Bearish           NA           NA
        1763561700     11   19   09     15 40.01    52.42 6643.75 6642.50 6647.25 6641.25 ES%3DF      15m   Bearish           NA   BearishFVG
        1763562600     11   19   09     30 58.35    53.21 6642.50 6665.00 6668.25 6635.75 ES%3DF      15m   Bullish           NA           NA
        1763563500     11   19   09     45 70.07    55.46 6665.00 6691.75 6693.75 6661.75 ES%3DF      15m   Bullish           NA   BullishFVG
        """
        # 1763564400     11   19   10     00 72.91    57.78 6692.00 6701.00 6703.00 6689.25 ES%3DF      15m   Bullish           NA   BullishFVG
        self.data15m = self.sampledata_toDF(data_15m, False)

        data_5m = """
        1763562300     11   19   09     25 29.99    36.55 6646.00 6642.50 6646.75 6641.25 ES%3DF       5m   Bearish    6648.25
        1763562600     11   19   09     30 43.61    37.49 6642.50 6649.00 6651.00 6635.75 ES%3DF       5m   Bullish    6647.47
        1763562900     11   19   09     35 54.88    39.81 6649.00 6656.75 6658.75 6648.00 ES%3DF       5m   Bullish    6647.72
        """
        # 1763563200     11   19   09     40 63.29    42.94 6656.75 6665.00 6668.25 6651.75 ES%3DF       5m   Bullish    6649.08
        # 1763563500     11   19   09     45 67.45    46.21 6665.00 6670.25 6673.00 6661.75 ES%3DF       5m   Bearish    6651.22
        self.data5m = self.sampledata_toDF(data_5m, True)
        return

    def sampledata_toDF(self, data, is5m):
        column_names = [
            'unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal', 'open', 'close', 'high', 'low',
            'symbol', 'interval', 'cspattern', 'cstwopattern', 'csfvgpattern'
        ]
        if (is5m):
            column_names = [
                'unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal', 'open', 'close', 'high', 'low',
                'symbol', 'interval', 'cspattern', 'ninemaval'
            ]

        df = pd.read_csv(io.StringIO(data),
        sep=r'\s+',              # Regular expression to match one or more whitespace characters
        header=None,             # No header row in the input data
        names=column_names,      # Assign the defined column names
        engine='python'          # 'python' engine is required for complex regex separators
        )
        return df
        