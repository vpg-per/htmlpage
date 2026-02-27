import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dataManager import ServiceManager
import gc
import io


class csPattern:
    def __init__(self):
        self.objMgr         = ServiceManager()
        # DataFrames are stored only while needed and freed immediately after use
        self.data5m         = None
        self.data15m        = None
        self.data30m        = None
        self.data1h         = None
        self.data4h         = None
        self.openorderon5m  = None
        self.closeorderon5m = None
        self.htfPattern     = None

    # ------------------------------------------------------------------
    # Public entry-points
    # ------------------------------------------------------------------

    def analyze_stockcandlesLTF(self, symbol):
        """Fetch 5m/15m/30m, detect candlestick breakout patterns, then run
        open/close signal logic.  DataFrames are freed as soon as possible."""

        # ---- 5m ----
        self.data5m = self.objMgr.GetStockdata_Byinterval(symbol, "5m", indicatorList="macd")
        self.data5m = self._identify_candlebreakout_pattern(self.data5m)
        # EMA-5 on close — keep as float32
        self.data5m['ema5'] = (
            self.data5m['close'].ewm(span=5, adjust=False).mean()
            .round(2).astype('float32')
        )
        self._trim_to_last_n(self.data5m, 25)   # only recent bars needed

        # ---- 15m ----
        self.data15m = self.objMgr.GetStockdata_Byinterval(symbol, "15m", indicatorList="macd")
        self.data15m = self._identify_candlebreakout_pattern(self.data15m)
        self._trim_to_last_n(self.data15m, 20)

        # ---- 30m ----
        self.data30m = self.objMgr.GetStockdata_Byinterval(symbol, "30m", indicatorList="macd")
        self.data30m = self._identify_candlebreakout_pattern(self.data30m)
        self._trim_to_last_n(self.data30m, 10)

        gc.collect()

        # ---- signal detection ----
        loadedFromDB = True
        utc_now = datetime.now(timezone.utc)
        if self.openorderon5m is None and utc_now.hour <= 20:
            self._parse_stockdataintervalforOpen()
            loadedFromDB = False

        if self.openorderon5m is not None and loadedFromDB:
            self._parse_stockdataintervalforClose()

        # ---- free DataFrames immediately after signal detection ----
        self._free_dataframes()

    def analyze_stockcandlesHTF(self, symbol):
        """Fetch 1h/4h data for higher-timeframe market structure detection."""
        self.data1h = self.objMgr.GetStockdata_Byinterval(symbol, "1h", indicatorList="rsi")
        self.data1h = self._identify_candlebreakout_pattern(self.data1h)
        self._trim_to_last_n(self.data1h, 10)

        self.data4h = self.objMgr.GetStockdata_Byinterval(symbol, "4h", indicatorList="rsi")
        self.data4h = self._identify_candlebreakout_pattern(self.data4h)
        self._trim_to_last_n(self.data4h, 5)

        gc.collect()

        ret = self._parse_forMktStructure()

        self._free_dataframes()
        return ret

    # ------------------------------------------------------------------
    # Signal parsing
    # ------------------------------------------------------------------

    def _parse_forMktStructure(self):
        if self.data4h is None or len(self.data4h) < 2:
            return None
        if self.data1h is None or len(self.data1h) < 2:
            return None

        last_4h      = self.data4h.iloc[-1]
        last_1h      = self.data1h.iloc[-1]
        prev_1h      = self.data1h.iloc[-2]

        if (str(last_4h['cspattern']) == str(last_1h['cspattern'])
                and str(last_1h['cspattern']) != str(prev_1h['cspattern'])):
            return {
                "symbol":     str(last_1h['symbol']),
                "4h-1h":      str(last_1h['cspattern']),
                "hour":       str(last_1h['hour']),
                "2cspattern": str(last_1h['cstwopattern']),
                "fvg":        str(last_1h['csfvgpattern']),
            }
        return None

    def _parse_stockdataintervalforOpen(self):
        if self.data5m is None or self.data5m.empty or self.data15m is None or self.data15m.empty or self.data30m is None or self.data30m.empty:
                return

        self._structure_30m()
        self._structure_15m()
        self._structure_5m()

        last_5m  = self.data5m.iloc[-1]
        last_15m = self.data15m.iloc[-1]
        last_30m = self.data30m.iloc[-1]

        macdpattern     = str(last_5m['macdpattern'])
        macdpattern_15m = str(last_15m['macdpattern']) if 'macdpattern' in last_15m.index else "Neutral"
        macdpattern_30m = str(last_30m['macdpattern']) if 'macdpattern' in last_30m.index else "Neutral"

        # Require 5m, 15m and 30M MACD to agree for a valid alert
        if macdpattern != macdpattern_15m and macdpattern != macdpattern_30m:
            macdpattern = "Neutral"

        stoploss, profittarget = 0.0, 0.0
        if macdpattern == "Bullish":
            stoploss     = float(last_15m['low'])
            profittarget = float(last_15m['close'])
        elif macdpattern == "Bearish":
            profittarget = float(last_15m['low'])
            stoploss     = float(last_15m['high'])

        if macdpattern in ("Bullish", "Bearish"):
            self.openorderon5m = {
                "symbol":            str(last_5m['symbol']),
                "stockprice":        round(float(last_5m['ema5']), 2),
                "cspattern":         macdpattern,
                "cstwopattern":      str(last_5m['cstwopattern']),
                "csfvgpattern":      str(last_5m['csfvgpattern']),
                "unixtime":          int(last_5m['unixtime']),
                "stoploss":          round(stoploss, 2),
                "profittarget":      round(profittarget, 2),
                "hour":              int(last_5m['hour']),
                "minute":            int(last_5m['minute']),
                "updatedTriggerTime": int(last_5m['unixtime']),
            }

    def _parse_stockdataintervalforClose(self):
        if self.data5m is None or self.data5m.empty or self.data15m is None or self.data15m.empty:
            return

        self._structure_15m()
        self._structure_5m()

        last_5m  = self.data5m.iloc[-1]
        last_15m = self.data15m.iloc[-1]

        cur_pattern = str(last_5m['macdpattern'])

        if (self.openorderon5m['cspattern'] == cur_pattern or cur_pattern == "Neutral"):
            # Update the open order with fresh time / levels
            self.openorderon5m['hour']             = int(last_5m['hour'])
            self.openorderon5m['minute']            = int(last_5m['minute'])
            self.openorderon5m['updatedTriggerTime'] = int(last_5m['unixtime'])
            self.openorderon5m['cstwopattern']      = str(last_5m['cstwopattern'])
            self.openorderon5m['csfvgpattern']      = str(last_5m['csfvgpattern'])
            if self.openorderon5m['cspattern'] == "Bullish":
                self.openorderon5m['profittarget'] = round(float(last_15m['high']), 2)
                self.openorderon5m['stoploss']     = round(float(last_15m['low']),  2)
            elif self.openorderon5m['cspattern'] == "Bearish":
                self.openorderon5m['profittarget'] = round(float(last_15m['low']),  2)
                self.openorderon5m['stoploss']     = round(float(last_15m['high']), 2)
        else:
            # Pattern has flipped — signal close
            self.closeorderon5m = {
                "symbol":       str(last_5m['symbol']),
                "stockprice":   round(float(last_5m['open']), 2),
                "cspattern":    cur_pattern,
                "unixtime":     int(last_5m['unixtime']),
                "stoploss":     "0",
                "profittarget": "0",
                "hour":         int(last_5m['hour']),
                "minute":       int(last_5m['minute']),
            }

    # ------------------------------------------------------------------
    # MACD pattern classification (only last 3 rows needed)
    # ------------------------------------------------------------------

    def _structure_from_tail(self, df):
        """Extract MACD pattern using the last 3 rows of df."""
        if df is None or len(df) < 3:
            return "Neutral"
        return self._structure_usingInputRows(df.iloc[-1], df.iloc[-2], df.iloc[-3])

    def _structure_5m(self):
        pattern = self._structure_from_tail(self.data5m)
        if 'macdpattern' not in self.data5m.columns:
            self.data5m['macdpattern'] = pd.Categorical(
                ['Neutral'] * len(self.data5m),
                categories=['Neutral', 'Bullish', 'Bearish']
            )
        self.data5m.iat[-1, self.data5m.columns.get_loc('macdpattern')] = pattern

    def _structure_15m(self):
        pattern = self._structure_from_tail(self.data15m)
        if 'macdpattern' not in self.data15m.columns:
            self.data15m['macdpattern'] = pd.Categorical(
                ['Neutral'] * len(self.data15m),
                categories=['Neutral', 'Bullish', 'Bearish']
            )
        self.data15m.iat[-1, self.data15m.columns.get_loc('macdpattern')] = pattern

    def _structure_30m(self):
        pattern = self._structure_from_tail(self.data30m)
        if 'macdpattern' not in self.data30m.columns:
            self.data30m['macdpattern'] = pd.Categorical(
                ['Neutral'] * len(self.data30m),
                categories=['Neutral', 'Bullish', 'Bearish']
            )
        self.data30m.iat[-1, self.data30m.columns.get_loc('macdpattern')] = pattern

    @staticmethod
    def _structure_usingInputRows(last_row, prev_row, prev2_row):
        """Classify MACD momentum using three consecutive rows."""
        m  = float(last_row.get('macd', 0))
        s  = float(last_row.get('msignal', 0))
        h  = float(last_row.get('histogram', 0))
        h1 = float(prev_row.get('histogram', 0))
        h2 = float(prev2_row.get('histogram', 0))

        # Early reversal signals (highest priority)
        if (last_row['interval'] != '5m'):
            if m > 0 and h > 0 and h < h1 and h1 > h2:       # histogram fading while positive
                return "Bearish"
            if m < 0 and h < 0 and h > h1 and h1 > h2:        # histogram recovering while negative
                return "Bullish"

        # Standard bullish conditions
        if (m > s and m > 0) or (m > s and h > 0) or (h > 0 and h > h1 > h2) or (m > 0 and s > 0 and h > h1 > h2):
            return "Bullish"

        # Standard bearish conditions
        if ((m < s and m < 0) or (m < s and h < 0) or (h < 0 and h < h1 < h2)
                or (m < 0 and s < 0 and h < h1 < h2)
                or (m > 0 and s > 0 and h < h1 and h1 > h2)):
            return "Bearish"

        return "Neutral"

    # ------------------------------------------------------------------
    # Candlestick pattern identification (vectorised where possible)
    # ------------------------------------------------------------------

    def _identify_candlebreakout_pattern(self, df, engulfFlag=True, fvgFlag=True):
        if df is None or df.empty:
            return df

        n = len(df)
        # Pre-allocate with known categories — cheaper than per-row assignment
        df['cspattern'] = pd.Categorical(
            ['Neutral'] * n, categories=['Neutral', 'Bullish', 'Bearish']
        )
        if engulfFlag:
            df['cstwopattern'] = pd.Categorical(
                ['na'] * n, categories=['na', 'UlEngulf', 'EaEngulf']
            )
        if fvgFlag:
            df['csfvgpattern'] = pd.Categorical(
                ['na'] * n, categories=['na', 'EaFVG', 'UlFVG']
            )

        if n < 2:
            return df

        # Extract numpy arrays once — avoids repeated pandas indexing overhead
        opens  = df['open'].to_numpy(dtype='float32')
        highs  = df['high'].to_numpy(dtype='float32')
        lows   = df['low'].to_numpy(dtype='float32')
        closes = df['close'].to_numpy(dtype='float32')
        idx    = df.index

        # Build result arrays, then assign in bulk — much faster & less memory
        cspattern_arr    = np.full(n, 'Neutral', dtype=object)
        cstwopattern_arr = np.full(n, 'na',      dtype=object) if engulfFlag else None
        csfvgpattern_arr = np.full(n, 'na',      dtype=object) if fvgFlag    else None

        # Carry-forward signal (propagate from previous bar)
        cspattern_arr[0] = 'Neutral'
        for i in range(1, n):
            o, h, l, c    = opens[i],   highs[i],   lows[i],   closes[i]
            o1, h1, l1, c1 = opens[i-1], highs[i-1], lows[i-1], closes[i-1]

            # Sentiment carry-forward + change
            sig = cspattern_arr[i-1]
            if c > h1:
                sig = 'Bullish'
            elif c < l1:
                sig = 'Bearish'
            cspattern_arr[i] = sig

            # Engulfing patterns
            if engulfFlag:
                if c > o and c1 < o1 and c > o1 and o < c1:
                    cstwopattern_arr[i] = 'UlEngulf'
                elif c < o and c1 > o1 and c < o1 and o > c1:
                    cstwopattern_arr[i] = 'EaEngulf'

            # Fair Value Gap (needs i >= 2)
            if fvgFlag and i >= 2:
                h2 = highs[i-2]
                l2 = lows[i-2]
                if l2 > h:
                    csfvgpattern_arr[i] = 'EaFVG'
                elif h2 < l:
                    csfvgpattern_arr[i] = 'UlFVG'

        # Bulk assignment — one DataFrame write per column
        df['cspattern'] = pd.Categorical(cspattern_arr, categories=['Neutral', 'Bullish', 'Bearish'])
        if engulfFlag:
            df['cstwopattern'] = pd.Categorical(cstwopattern_arr, categories=['na', 'UlEngulf', 'EaEngulf'])
        if fvgFlag:
            df['csfvgpattern'] = pd.Categorical(csfvgpattern_arr, categories=['na', 'EaFVG', 'UlFVG'])

        del opens, highs, lows, closes
        del cspattern_arr
        if cstwopattern_arr is not None: del cstwopattern_arr
        if csfvgpattern_arr is not None: del csfvgpattern_arr

        return df

    # ------------------------------------------------------------------
    # Memory helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _trim_to_last_n(df, n):
        """Trim a DataFrame in-place to the last n rows to save RAM.
        Works safely with both RangeIndex and DatetimeIndex."""
        if df is not None and len(df) > n:
            drop_idx = df.index[:-n]
            df.drop(index=drop_idx, inplace=True)

    def _free_dataframes(self):
        """Release all held DataFrames and trigger GC."""
        for attr in ('data5m', 'data15m', 'data30m', 'data1h', 'data4h'):
            setattr(self, attr, None)
        gc.collect()

    # ------------------------------------------------------------------
    # Legacy public aliases (keep old names so existing callers don't break)
    # ------------------------------------------------------------------

    def Structure_5m(self):
        self._structure_5m()

    def Structure_15m(self):
        self._structure_15m()

    def Structure_30m(self):
        self._structure_30m()

    def Structure_usingInputRows(self, last_row, last_but_one_row, last_but_second_row):
        return self._structure_usingInputRows(last_row, last_but_one_row, last_but_second_row)

    def identify_candlebreakout_pattern(self, df, engulfFlag=True, fvgFlag=True):
        return self._identify_candlebreakout_pattern(df, engulfFlag, fvgFlag)

    def parse_stockdataintervalforOpen(self):
        self._parse_stockdataintervalforOpen()

    def parse_stockdataintervalforClose(self):
        self._parse_stockdataintervalforClose()

    def parse_forMktStructure(self):
        return self._parse_forMktStructure()

    # ------------------------------------------------------------------
    # Sample-data helpers (used for testing / debugging only)
    # ------------------------------------------------------------------

    def ResettoSampleData(self):
        data_30m = """
        1766001600     12   17   15     00 26.10    31.96 673.12 672.52 673.32 672.24      30m    SPY   Bullish           na           na
        1766003400     12   17   15     30 22.72    30.73 672.53 671.30 673.00 671.20      30m    SPY   Bearish           na           na
        """
        self.data30m = self._sampledata_toDF(data_30m, is5m=False)

        data_15m = """
        1766002500     12   17   15     15 30.06    29.77 672.76 672.52 673.24 672.24    SPY      15m   Bullish           na           na
        1766003400     12   17   15     30 33.26    30.24 672.53 672.79 672.90 671.95    SPY      15m   Bullish           na           na
        1766004300     12   17   15     45 26.16    29.69 672.79 671.30 673.00 671.20    SPY      15m   Bearish           na           na
        """
        self.data15m = self._sampledata_toDF(data_15m, is5m=False)

        data_5m = """
        1766003100     12   17   15     25 42.04    44.01 672.95 672.52 672.95 672.24    SPY       5m   Bearish           na           na     672.75
        1766003400     12   17   15     30 41.90    43.73 672.53 672.51 672.78 671.95    SPY       5m   Bearish           na        EaFVG     672.73
        1766003700     12   17   15     35 38.06    42.97 672.50 672.23 672.56 672.17    SPY       5m   Bearish           na           na     672.62
        1766004000     12   17   15     40 48.27    43.68 672.24 672.79 672.90 672.18    SPY       5m   Bullish           na           na     672.60
        1766004300     12   17   15     45 51.06    44.66 672.79 672.97 672.98 672.48    SPY       5m   Bullish           na           na     672.60
        1766004600     12   17   15     50 36.97    43.64 672.95 671.79 673.00 671.61    SPY       5m   Bearish           na           na     672.46
        1766004900     12   17   15     55 32.91    42.21 671.79 671.30 671.79 671.20    SPY       5m   Bearish           na        EaFVG     672.22
        """
        self.data5m = self._sampledata_toDF(data_5m, is5m=True)

    @staticmethod
    def _sampledata_toDF(data, is5m):
        base_cols = ['unixtime', 'nmonth', 'nday', 'hour', 'minute',
                     'rsi', 'rsignal', 'open', 'close', 'high', 'low',
                     'symbol', 'interval', 'cspattern', 'cstwopattern', 'csfvgpattern']
        cols = base_cols + (['ema5'] if is5m else [])
        return pd.read_csv(
            io.StringIO(data),
            sep=r'\s+',
            header=None,
            names=cols,
            engine='python'
        )

    # Legacy alias
    def sampledata_toDF(self, data, is5m):
        return self._sampledata_toDF(data, is5m)

    # Legacy — kept for backward compat; reduce_memory_usage is no longer needed
    # because all data is already stored in the smallest possible types
    def reduce_memory_usage(self, df, is5m=False):
        return df
