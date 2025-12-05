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
        self.data1h = None
        self.data4h = None
        self.openorderon5m = None
        self.closeorderon5m = None
        self.htfPattern = None

    def analyze_stockcandlesLTF(self, symbol):
        todayn = datetime.now().strftime('%d')   

        self.data5m = self.objMgr.GetStockdata_Byinterval(symbol, "5m", False)
        self.data5m = self.identify_candlebreakout_pattern(self.data5m)
        self.data5m['fivemaval'] = round(self.data5m['close'].rolling(window=5).mean(), 2)
        self.data15m = self.objMgr.GetStockdata_Byinterval(symbol, "15m", False)
        self.data15m = self.identify_candlebreakout_pattern(self.data15m)
        self.data30m = self.objMgr.GetStockdata_Byinterval(symbol, "30m", False)
        self.data30m = self.identify_candlebreakout_pattern(self.data30m)

        self.ResettoSampleData()   
        loadedFromDB = True
        if (self.openorderon5m is None):
            self.parse_stockdataintervalforOpen()
            loadedFromDB = False

        if (self.openorderon5m is not None and loadedFromDB ):
            self.parse_stockdataintervalforClose()
        
        return

    def analyze_stockcandlesHTF(self, symbol):
        todayn = datetime.now().strftime('%d')   

        self.data1h = self.objMgr.GetStockdata_Byinterval(symbol, "1h", False)
        self.data1h = self.identify_candlebreakout_pattern(self.data1h)
        self.data4h = self.objMgr.GetStockdata_Byinterval(symbol, "4h", False)
        self.data4h = self.identify_candlebreakout_pattern(self.data4h)
        ret = self.parse_forMktStructure()
        
        return ret

    def parse_forMktStructure(self):
        mspattern = None
        last_but_one_4hrow = self.data4h.iloc[-2].copy()
        last_4hrow = self.data4h.iloc[-1].copy()
        last_but_one_1hrow = self.data1h.iloc[-2].copy()
        last_1hrow = self.data1h.iloc[-1].copy()
        
        if ( last_4hrow['cspattern'] == last_1hrow['cspattern'] and last_1hrow['cspattern'] != last_but_one_1hrow['cspattern'] ):
            mspattern = {"symbol":last_1hrow['symbol'], "4h-1h":last_1hrow['cspattern'], "hour":last_1hrow['hour'], "2cspattern":last_1hrow['cstwopattern'], "fvg":last_1hrow['csfvgpattern']}
        
        return mspattern

    def parse_stockdataintervalforOpen(self):
        lookupts = int((datetime.now()- timedelta(minutes=48)).timestamp())
        df = self.data30m.copy()
        sub_df15m, sub_df5m, last_but_one_15mrow, last_15mrow = None, None, None, None
        found_crossover = False
                
        if df is not None:
            last_but_one_30mrow = df.iloc[-2].copy()
            last_30mrow = df.iloc[-1].copy()
            print(df.tail(20).to_string(index=False))
            #if ( last_30mrow['unixtime'] > lookupts and last_30mrow['cspattern'] != last_but_one_30mrow['cspattern'] and last_30mrow['cspattern'] != "Neutral" ):
            if ( last_30mrow['cspattern'] != last_but_one_30mrow['cspattern'] and last_30mrow['cspattern'] != "Neutral" ):
                sub_df15m = self.data15m[ self.data15m['unixtime'].astype(int) >= int(last_but_one_30mrow['unixtime'] ) ]
                print(sub_df15m.to_string(index=False))
                if ( len(sub_df15m) > 1):
                    for i in range(1, len(sub_df15m)):
                        if (sub_df15m.iloc[i]['cspattern'] != sub_df15m.iloc[i-1]['cspattern'] and sub_df15m.iloc[i]['cspattern'] != "Neutral" and 
                            sub_df15m.iloc[i]['cspattern'] == last_30mrow['cspattern']):
                            last_but_one_15mrow = sub_df15m.iloc[i-1].copy()
                            last_15mrow = sub_df15m.iloc[i].copy()
                            found_crossover = True
                    if (found_crossover == True and last_15mrow is not None and last_but_one_15mrow is not None):
                        print(last_but_one_15mrow.to_string(index=False))
                        print(last_15mrow.to_string(index=False ))
                        sub_df5m = self.data5m[ (self.data5m['unixtime'].astype(int) >= int(last_15mrow['unixtime'])) ]
                        print(sub_df5m.to_string(index=False))
                        if ( len(sub_df5m) > 1):
                            for j in range(len(sub_df5m)):
                                last_5mrow = sub_df5m.iloc[j].copy()
                                if (last_5mrow['cspattern'] == last_15mrow['cspattern']):
                                    stoploss = last_15mrow['low']
                                    profittarget = last_15mrow['close']
                                    if ( last_15mrow['cspattern'] == "Bearish"):
                                        stoploss = last_15mrow['high']
                                    
                                    isAlertValid = False
                                    if (last_5mrow['cspattern'] == "Bullish"):
                                        isAlertValid = float(last_5mrow['fivemaval']) > float(stoploss) and float(last_5mrow['fivemaval']) < float(profittarget)
                                    elif (last_5mrow['cspattern'] == "Bearish"):
                                        isAlertValid = float(last_5mrow['fivemaval']) < float(stoploss) and float(last_5mrow['fivemaval']) > float(profittarget)
                                    if (isAlertValid):
                                        self.openorderon5m = {"symbol": last_5mrow['symbol'], "stockprice": float(last_5mrow['fivemaval']), "cspattern": last_5mrow['cspattern'],
                                            "unixtime": int(last_5mrow['unixtime']), 'stoploss': float(stoploss), 'profittarget': float(profittarget),
                                            'hour': int(last_5mrow['hour']), 'minute': int(last_5mrow['minute']), "updatedTriggerTime" : int(last_5mrow['unixtime'])}
                                        break
                        print(self.openorderon5m)

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
            df['cstwopattern'] = "na"
        if (fvgFlag):
            df['csfvgpattern'] = "na"
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
                    df.loc[df.index[i], 'csfvgpattern'] = "EaFVG"
                if p2_high < l:
                    df.loc[df.index[i], 'csfvgpattern'] = "UlFVG"
        
        return df

    # def invoke_downloadcall(self, symbol, interval="15m", rsiFlag=True):
                
    #     stPeriod = int((datetime.now()- timedelta(days=2)).timestamp()) 
    #     endPeriod = datetime.now()
    #     minutes_to_subtract = endPeriod.minute % 5
    #     endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0)
    #     df = self.objMgr.download_stock_data(symbol, stPeriod, endPeriod.timestamp(), interval)

    #     if (interval=="5m"):
    #         df = df[(df['unixtime'] <= endPeriod.timestamp()) & (df['minute'].isin(["00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55"])) ]
    #     elif (interval=="15m"):
    #         minutes_to_subtract = endPeriod.minute % 15
    #         endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
    #         df = df[(df['unixtime'] <= endPeriod) & (df['minute'].isin(["00", "15", "30", "45"])) ]
    #     elif (interval=="30m"):
    #         minutes_to_subtract = endPeriod.minute % 30
    #         endPeriod = endPeriod.replace(minute=endPeriod.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() - 1
    #         df = df[(df['unixtime'] <= endPeriod) & (df['minute'].isin(["00", "30"])) ]

    #     df['symbol']=symbol
    #     df['interval']=interval
    #     df = self.objMgr.calculate_rsi(df, period=14)
    #     df = df.loc[:, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval']]
        
    #     return df

    def ResettoSampleData(self):

        #print(self.data30m.loc[self.data30m['unixtime'].astype(int) <= 1764937800, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        #print(self.data15m.loc[self.data15m['unixtime'].astype(int) <= 1764937800, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        #print(self.data5m.loc[self.data5m['unixtime'].astype(int) <= 1764937800, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern', 'fivemaval']].tail(42).to_string(index=False))
        # print(self.data30m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(30).to_string(index=False))
        # print(self.data15m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(40).to_string(index=False))
        # print(self.data5m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern', 'fivemaval']].tail(42).to_string(index=False))
        data_30m = """
        1764934200     12   05   06     30 61.07    57.02 686.02 685.98 686.12 685.90    SPY      30m   Bullish           na           na
        1764936000     12   05   07     00 59.98    57.41 685.95 685.87 686.16 685.57    SPY      30m   Bearish           na           na
        """
        self.data30m = self.sampledata_toDF(data_30m, False)

        data_15m = """
        1764935100     12   05   06     45 63.36    61.87 686.01 685.98 686.02 685.91    SPY      15m   Bullish           na           na
        1764936000     12   05   07     00 63.47    62.09 685.95 685.99 686.16 685.90    SPY      15m   Bullish           na           na
        1764936900     12   05   07     15 61.05    61.95 685.96 685.87 686.08 685.57    SPY      15m   Bearish           na           na
        """
        self.data15m = self.sampledata_toDF(data_15m, False)

        data_5m = """
        1764936600     12   05   07     10 57.70    60.67 686.05 685.99 686.08 685.96    SPY       5m   Bearish           na           na     686.02
        1764936900     12   05   07     15 52.68    59.60 685.96 685.89 686.08 685.86    SPY       5m   Bearish           na           na     686.01
        1764937200     12   05   07     20 47.36    57.97 685.89 685.77 685.91 685.57    SPY       5m   Bearish           na        EaFVG     685.96
        1764937500     12   05   07     25 51.74    57.14 685.71 685.87 685.90 685.64    SPY       5m   Bearish           na           na     685.91
        """
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
                'symbol', 'interval', 'cspattern', 'cstwopattern', 'csfvgpattern', 'fivemaval'
            ]

        df = pd.read_csv(io.StringIO(data),
        sep=r'\s+',              # Regular expression to match one or more whitespace characters
        header=None,             # No header row in the input data
        names=column_names,      # Assign the defined column names
        engine='python'          # 'python' engine is required for complex regex separators
        )
        return df
        
