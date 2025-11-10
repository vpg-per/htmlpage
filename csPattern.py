import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo
from dataManager import ServiceManager
from itertools import islice

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

        self.data5m = self.invoke_downloadcall(symbol, "5m", False)
        self.data5m = self.identify_candlebreakout_pattern(self.data5m, False, False)
        self.data5m['ninemaval'] = round(self.data5m['close'].rolling(window=9).mean(), 2)

        self.data15m = self.invoke_downloadcall(symbol, "15m")
        self.data15m = self.identify_candlebreakout_pattern(self.data15m)
        self.data30m = self.invoke_downloadcall(symbol, "30m")
        self.data30m = self.identify_candlebreakout_pattern(self.data30m)

        if (self.openorderon5m is None):
            self.parse_stockdataintervalforOpen()
            print(self.openorderon5m)
        
        if (self.openorderon5m is not None):
            self.parse_stockdataintervalforClose()
            print(self.openorderon5m)
        
        return

    def parse_stockdataintervalforOpen(self):
        
        df = self.data30m.copy()
        sub_df15m, sub_df5m = None, None
        sm_temp15m, sm_temp5m, sm_temp15m_c = None, None, None
        if df is not None:
            for i in range(1, len(df)):
                if ( df['cspattern'].iloc[i] != df['cspattern'].iloc[i-1] and df['cspattern'].iloc[i] != "Neutral" ):
                    sub_df15m = self.data15m[((self.data15m['hour'].astype(int) * 60 + self.data15m['minute'].astype(int)) > (int(df['hour'].iloc[i]) * 60 + int(df['minute'].iloc[i]))) & (self.data15m['nday'] == df['nday'].iloc[i]) ]

                    for j in range( len(sub_df15m[(sub_df15m['hour'].astype(int) < 15)])):
                        sub_df5m = self.data5m[   ((self.data5m['hour'].astype(int) * 60 + self.data5m['minute'].astype(int)) > (int(sub_df15m['hour'].iloc[j]) * 60 + int(sub_df15m['minute'].iloc[j]))) & (self.data5m['nday'] == sub_df15m['nday'].iloc[j]) & (self.data5m['hour'].astype(int) < 15) ].copy()
                        if (len(sub_df5m) > 0 and int(sub_df5m['hour'].iloc[0]) < 15):
                            self.openorderon5m = {"symbol": sub_df5m['symbol'].iloc[0], "stockprice": float(sub_df5m['ninemaval'].iloc[0]), "cspattern": sub_df5m['cspattern'].iloc[0],
                                                    "unixtime": int(sub_df5m['unixtime'].iloc[0]), 'stoploss': float(sub_df15m['open'].iloc[j]), 'profittarget': float(sub_df15m['close'].iloc[j]),
                                                    'hour': int(sub_df5m['hour'].iloc[0]), 'minute': int(sub_df5m['minute'].iloc[0])}
                        break                    
                    sub_df15m, sub_df5m, sm_temp15m_c = None, None, None
        
        return

    def parse_stockdataintervalforClose(self):
        df = self.data15m[(self.data15m['nday'] == "07")].copy()
        df = df[ df['hour'].astype(int) * 60 + df['minute'].astype(int) > int(self.openorderon5m['hour']) * 60 + int(self.openorderon5m['minute'])].copy()
        if df is not None:
            matched_data15m = df.head(1)
            if (matched_data15m['cspattern'].iloc[0] == self.openorderon5m['cspattern'] and int(matched_data15m['hour'].iloc[0]) < 16):
                self.openorderon5m['hour'] = int(matched_data15m['hour'].iloc[0])
                self.openorderon5m['minute'] = int(matched_data15m['minute'].iloc[0])
                if (float(matched_data15m['close'].iloc[0]) > float(self.openorderon5m['profittarget'])):
                    self.openorderon5m['profittarget'] = float(matched_data15m['high'].iloc[0])
                    self.openorderon5m['stoploss'] = float(matched_data15m['open'].iloc[0])
            else:
                self.closeorderon5m = {"symbol": matched_data15m['symbol'].iloc[0], "stockprice": float(matched_data15m['open'].iloc[0]), "cspattern": self.openorderon5m['cspattern'],
                                        "unixtime": int(matched_data15m['unixtime'].iloc[0]), 'stoploss': float(matched_data15m['open'].iloc[0]), 'profittarget': float(matched_data15m['close'].iloc[0]),
                                        'hour': int(matched_data15m['hour'].iloc[0]), 'minute': int(matched_data15m['minute'].iloc[0])}

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
                
        stPeriod = int((datetime.now()- timedelta(days=3)).timestamp()) 
        endPeriod = datetime.now()
        minutes_to_subtract = endPeriod.minute % 15
        endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
        df = self.objMgr.download_stock_data(symbol, stPeriod, endPeriod, interval)
        if (interval=="30m"):
            df = df[(df['unixtime'] <= endPeriod) & (df['minute'].isin(["00", "30"])) ]
        df['symbol']=symbol
        df['interval']=interval
        if (rsiFlag):
            df = self.objMgr.calculate_rsi(df, period=14)
            df = df.loc[:, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval']]
        else:
            df = df.loc[:, ['unixtime', 'nmonth', 'nday', 'hour', 'minute', 'open','close','high','low', 'symbol', 'interval']]

        return df


        