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

        self.data5m = self.objMgr.GetStockdata_Byinterval(symbol, "5m", indicatorList = "rsi,macd")
        self.data5m = self.identify_candlebreakout_pattern(self.data5m)
        self.data5m['ema5'] = round(self.data5m['close'].ewm(span=5, adjust=False).mean(), 2)
        self.data15m = self.objMgr.GetStockdata_Byinterval(symbol, "15m", indicatorList = "rsi,macd")
        self.data15m = self.identify_candlebreakout_pattern(self.data15m)
        self.data30m = self.objMgr.GetStockdata_Byinterval(symbol, "30m", indicatorList = "rsi,macd")
        self.data30m = self.identify_candlebreakout_pattern(self.data30m)
        
        # self.ResettoSampleData()  
        loadedFromDB = True
        utc_now = datetime.now(timezone.utc)
        if (self.openorderon5m is None and utc_now.hour <= 20):
            self.parse_stockdataintervalforOpen()
            loadedFromDB = False

        if (self.openorderon5m is not None and loadedFromDB ):
            self.parse_stockdataintervalforClose()
        
        return

    def analyze_stockcandlesHTF(self, symbol):   

        self.data1h = self.objMgr.GetStockdata_Byinterval(symbol, "1h", indicatorList = "rsi")
        self.data1h = self.identify_candlebreakout_pattern(self.data1h)
        self.data4h = self.objMgr.GetStockdata_Byinterval(symbol, "4h", indicatorList = "rsi")
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
        self.Structure_15m()
        self.Structure_5m()
        last_5mrow = self.data5m.iloc[-1].copy()
        last_15mrow = self.data15m.iloc[-1].copy()

        stoploss, profittarget = 0, 0
        if (last_5mrow['macdpattern'] == "Bullish"):
            stoploss = last_15mrow['low']
            profittarget = last_15mrow['close']
        elif ( last_5mrow['macdpattern'] == "Bearish"):
            profittarget = last_15mrow['low']
            stoploss = last_15mrow['high']
        if (stoploss > 0 or profittarget > 0):
            self.openorderon5m = {"symbol": last_5mrow['symbol'], "stockprice": float(last_5mrow['ema5']), "cspattern": last_5mrow['macdpattern'], "cstwopattern": last_5mrow['cstwopattern'], "csfvgpattern": last_5mrow['csfvgpattern'],
                "unixtime": int(last_5mrow['unixtime']), 'stoploss': float(stoploss), 'profittarget': float(profittarget),
                'hour': int(last_5mrow['hour']), 'minute': int(last_5mrow['minute']), "updatedTriggerTime" : int(last_5mrow['unixtime'])}

        return

    def parse_stockdataintervalforClose(self):
        self.Structure_15m()
        self.Structure_5m()
        last_5mrow = self.data5m.iloc[-1].copy()
        last_15mrow = self.data15m.iloc[-1].copy()

        if (self.openorderon5m['cspattern'] == last_5mrow['macdpattern'] or last_5mrow['macdpattern'] == "Neutral"):
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
             self.closeorderon5m = {"symbol": last_5mrow['symbol'], "stockprice": float(last_5mrow['open']), "cspattern": last_5mrow['macdpattern'],
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

    def Structure_30m(self):
        mapattern = self.Structure_usingInputRows(self.data30m.iloc[-1], self.data30m.iloc[-2], self.data30m.iloc[-3])
        self.data30m['macdpattern'] = 'na'
        self.data30m.at[self.data30m.index[-1], 'macdpattern'] = mapattern
        return 
        
    def Structure_15m(self):
        mapattern = self.Structure_usingInputRows(self.data15m.iloc[-1], self.data15m.iloc[-2], self.data15m.iloc[-3])
        self.data15m['macdpattern'] = 'na'
        self.data15m.at[self.data15m.index[-1], 'macdpattern'] = mapattern
        return
        
    def Structure_5m(self):
        mapattern = self.Structure_usingInputRows(self.data5m.iloc[-1], self.data5m.iloc[-2], self.data5m.iloc[-3])
        self.data5m['macdpattern'] = 'na'
        self.data5m.at[self.data5m.index[-1], 'macdpattern'] = mapattern
        return
    
    def Structure_usingInputRows(self, last_row, last_but_one_row, last_but_second_row):
        retPattern = "Neutral"
        
        # normalize values to floats for comparisons
        m = float(last_row.get('macd', 0))
        s = float(last_row.get('macdsignal', 0))
        h = float(last_row.get('histogram', 0))
        h1 = float(last_but_one_row.get('histogram', 0))
        h2 = float(last_but_second_row.get('histogram', 0))

        # Bullish conditions (any of these indicates bullish momentum)
        cond_macd_above = (m > s and m > 0)
        cond_macd_above_with_pos_hist = (m > s and h > 0)
        cond_hist_positive_and_rising = (h > 0 and h > h1 > h2)
        cond_macd_positive_hist_rising = (m > 0 and s > 0 and h > h1 > h2)

        # Bearish conditions (any of these indicates bearish momentum)
        cond_macd_below = (m < s and m < 0)
        cond_macd_below_with_neg_hist = (m < s and h < 0)
        cond_hist_negative_and_falling = (h < 0 and h < h1 < h2)
        cond_macd_negative_hist_falling = (m < 0 and s < 0 and h < h1 < h2)
        cond_hist_falling_while_macd_pos = (m > 0 and s > 0 and h < h1 and h1 > h2)
        # Histogram positive but decreasing (dark green -> light green)
        cond_hist_positive_and_falling = (m > 0 and h > 0 and h < h1 and h1 > h2)
        # Histogram negative but increasing (dark red -> light red)
        cond_hist_negative_and_rising = (m < 0 and h < 0 and h > h1 and h1 > h2)
        # Prioritize positive->decreasing histogram as a potential early bearish flip
        if cond_hist_positive_and_falling:
            retPattern = "Bearish"
        # Prioritize negative->increasing histogram as a potential early bullish flip
        elif cond_hist_negative_and_rising:
            retPattern = "Bullish"
        elif cond_macd_above or cond_macd_above_with_pos_hist or cond_hist_positive_and_rising or cond_macd_positive_hist_rising:
            retPattern = "Bullish"
        elif (cond_macd_below or cond_macd_below_with_neg_hist or cond_hist_negative_and_falling or cond_macd_negative_hist_falling or cond_hist_falling_while_macd_pos):
            retPattern = "Bearish"

        return retPattern

    def ResettoSampleData(self):
        
        # print(self.data30m.loc[self.data30m['unixtime'].astype(int) <= 1766005200, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        # print(self.data15m.loc[self.data15m['unixtime'].astype(int) <= 1766008000, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(20).to_string(index=False))
        # print(self.data5m.loc[self.data5m['unixtime'].astype(int) <= 1766008000, ['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern', 'ema5']].tail(42).to_string(index=False))
        # print(self.data30m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(30).to_string(index=False))
        # print(self.data15m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern']].tail(40).to_string(index=False))
        # print(self.data5m[['unixtime', 'nmonth', 'nday', 'hour', 'minute','rsi', 'rsignal','open','close','high','low', 'symbol', 'interval','cspattern', 'cstwopattern', 'csfvgpattern', 'ema5']].tail(42).to_string(index=False))
        data_30m = """
        1766001600     12   17   15     00 26.10    31.96 673.12 672.52 673.32 672.24      30m    SPY   Bullish           na           na
        1766003400     12   17   15     30 22.72    30.73 672.53 671.30 673.00 671.20      30m    SPY   Bearish           na           na
        """
        self.data30m = self.sampledata_toDF(data_30m, False)

        data_15m = """
        1766002500     12   17   15     15 30.06    29.77 672.76 672.52 673.24 672.24    SPY      15m   Bullish           na           na
        1766003400     12   17   15     30 33.26    30.24 672.53 672.79 672.90 671.95    SPY      15m   Bullish           na           na
        1766004300     12   17   15     45 26.16    29.69 672.79 671.30 673.00 671.20    SPY      15m   Bearish           na           na
        """
        self.data15m = self.sampledata_toDF(data_15m, False)

        data_5m = """
        1766003100     12   17   15     25 42.04    44.01 672.95 672.52 672.95 672.24    SPY       5m   Bearish           na           na     672.75
        1766003400     12   17   15     30 41.90    43.73 672.53 672.51 672.78 671.95    SPY       5m   Bearish           na        EaFVG     672.73
        1766003700     12   17   15     35 38.06    42.97 672.50 672.23 672.56 672.17    SPY       5m   Bearish           na           na     672.62
        1766004000     12   17   15     40 48.27    43.68 672.24 672.79 672.90 672.18    SPY       5m   Bullish           na           na     672.60
        1766004300     12   17   15     45 51.06    44.66 672.79 672.97 672.98 672.48    SPY       5m   Bullish           na           na     672.60
        1766004600     12   17   15     50 36.97    43.64 672.95 671.79 673.00 671.61    SPY       5m   Bearish           na           na     672.46
        1766004900     12   17   15     55 32.91    42.21 671.79 671.30 671.79 671.20    SPY       5m   Bearish           na        EaFVG     672.22
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
                'symbol', 'interval', 'cspattern', 'cstwopattern', 'csfvgpattern', 'ema5'
            ]

        df = pd.read_csv(io.StringIO(data),
        sep=r'\s+',              # Regular expression to match one or more whitespace characters
        header=None,             # No header row in the input data
        names=column_names,      # Assign the defined column names
        engine='python'          # 'python' engine is required for complex regex separators
        )
        return df
        
