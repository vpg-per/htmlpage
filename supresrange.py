import numpy as np
import yfinance as yf
import pandas as pd
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use Agg backend for non-interactive plotting
from matplotlib.patches import Rectangle
from datetime import datetime, timedelta
import warnings
import io
import base64
warnings.filterwarnings('ignore')

class FifteenMinuteSupportResistance:
    def __init__(self, symbol, days_back=3):
        """
        Initialize for 15-minute day trading analysis
        
        Args:
            symbol (str): Stock ticker symbol
            days_back (int): Number of days of 15-min data to fetch
        """
        self.symbol = symbol
        if datetime.now().weekday() == 5 or datetime.now().weekday() == 6:
            self.days_back = 3
        elif datetime.now().weekday() == 0 or datetime.now().weekday() == 1:
            self.days_back = 3
        else:
            self.days_back = days_back
        self.data = None
        self.current_price = None
        
    def fetch_15min_data(self, include_premarket=True):
        """Fetch 15-minute stock data for recent days including pre-market"""
        try:
            ticker = yf.Ticker(self.symbol)
            
            # For 15-min data with extended hours
            if self.days_back <= 7:
                period = "7d"
            elif self.days_back <= 30:
                period = "1mo"
            else:
                period = "3mo"
            
            # Fetch data with extended hours (includes pre-market and after-hours)
            if include_premarket:
                self.data = ticker.history(period=period, interval="15m", 
                                         prepost=True, repair=True)
                print(f"Fetching extended hours data (includes pre-market)...")
            else:
                self.data = ticker.history(period=period, interval="15m")
                print(f"Fetching regular hours data only...")
            
            if self.data.empty:
                print(f"No 15-minute data available for {self.symbol}")
                return False
            
            # Keep only the requested number of days
            cutoff_date = datetime.now() - timedelta(days=self.days_back)
            # Handle timezone-aware datetime index
            if self.data.index.tz is not None:
                cutoff_date = cutoff_date.replace(tzinfo=self.data.index.tz)
            else:
                # If data has timezone info, convert cutoff_date
                if hasattr(self.data.index, 'tz') and self.data.index.tz is not None:
                    import pytz
                    cutoff_date = pytz.timezone('America/New_York').localize(cutoff_date)
            
            self.data = self.data[self.data.index >= cutoff_date]
            # Add session type classification
            self.data = self.classify_trading_sessions()
            self.current_price = self.data['Close'].iloc[-1]
            
            # Count different session types
            regular_bars = len(self.data[self.data['Session'] == 'Regular'])
            premarket_bars = len(self.data[self.data['Session'] == 'Pre-Market'])
            afterhours_bars = len(self.data[self.data['Session'] == 'After-Hours'])
            
            print(f"Fetched {len(self.data)} total 15-minute bars for {self.symbol}")
            print(f"  - Regular Hours: {regular_bars} bars")
            print(f"  - Pre-Market: {premarket_bars} bars") 
            print(f"  - After-Hours: {afterhours_bars} bars")
            print(f"Current Price: ${self.current_price:.2f}")
            return True
            
        except Exception as e:
            print(f"Error fetching 15-minute data: {e}")
            return False
    
    def classify_trading_sessions(self):
        """Classify each bar as Regular, Pre-Market, or After-Hours"""
        data_with_sessions = self.data.copy()
        
        # Convert to Eastern Time for US market hours
        if data_with_sessions.index.tz is None:
            import pytz
            data_with_sessions.index = pytz.timezone('America/New_York').localize(data_with_sessions.index)
        else:
            data_with_sessions.index = data_with_sessions.index.tz_convert('America/New_York')
        
        # Define market hours (Eastern Time)
        # Pre-market: 4:00 AM - 9:30 AM ET
        # Regular: 9:30 AM - 4:00 PM ET  
        # After-hours: 4:00 PM - 8:00 PM ET
        
        sessions = []
        for timestamp in data_with_sessions.index:
            hour = timestamp.hour
            minute = timestamp.minute
            time_in_minutes = hour * 60 + minute
            
            # Convert to minutes since midnight
            premarket_start = 4 * 60  # 4:00 AM
            regular_start = 9 * 60 + 30  # 9:30 AM
            regular_end = 16 * 60  # 4:00 PM
            afterhours_end = 20 * 60  # 8:00 PM
            
            if premarket_start <= time_in_minutes < regular_start:
                sessions.append('Pre-Market')
            elif regular_start <= time_in_minutes < regular_end:
                sessions.append('Regular')
            elif regular_end <= time_in_minutes < afterhours_end:
                sessions.append('After-Hours')
            else:
                sessions.append('Closed')  # Outside trading hours
        
        data_with_sessions['Session'] = sessions
        return data_with_sessions
    
    def session_levels(self):
        """Calculate key levels for current trading session"""
        if self.data is None:
            return None
            
        try:
            # Get today's session data - handle timezone issues
            today = datetime.now().date()
            
            # Convert index to date for comparison
            if hasattr(self.data.index, 'date'):
                data_dates = self.data.index.date
            else:
                data_dates = [d.date() for d in self.data.index]
            
            today_mask = [d == today for d in data_dates]
            today_data = self.data[today_mask]
            
            if today_data.empty:
                # Use most recent session if today's data not available
                latest_date = data_dates[-1] if data_dates else None
                if latest_date:
                    latest_mask = [d == latest_date for d in data_dates]
                    today_data = self.data[latest_mask]
            
            if not today_data.empty:
                session_open = today_data['Open'].iloc[0]
                session_high = today_data['High'].max()
                session_low = today_data['Low'].min()
                # Opening range (first 30 minutes - 2 bars of 15min data)
                opening_range = today_data.head(2)
                or_high = opening_range['High'].max()
                or_low = opening_range['Low'].min()
                
                return {
                    'session_open': session_open,
                    'session_high': session_high,
                    'session_low': session_low,
                    'opening_range_high': or_high,
                    'opening_range_low': or_low,
                    'opening_range_mid': (or_high + or_low) / 2
                }
        except:
            pass
            
        return None
    
    def previous_session_levels(self):
        """Get previous trading session's key levels"""
        if self.data is None:
            return None
            
        try:
            # Get unique trading dates - handle timezone issues
            if hasattr(self.data.index, 'date'):
                dates = sorted(list(set(self.data.index.date)))
            else:
                dates = sorted(list(set([d.date() for d in self.data.index])))
            
            if len(dates) >= 2:
                prev_date = dates[-2]  # Previous trading day
                
                # Create mask for previous date
                if hasattr(self.data.index, 'date'):
                    data_dates = self.data.index.date
                else:
                    data_dates = [d.date() for d in self.data.index]
                
                prev_mask = [d == prev_date for d in data_dates]
                prev_data = self.data[prev_mask]
                
                if not prev_data.empty:
                    return {
                        'prev_open': prev_data['Open'].iloc[0],
                        'prev_high': prev_data['High'].max(),
                        'prev_low': prev_data['Low'].min(),
                        'prev_close': prev_data['Close'].iloc[-1]
                    }
        except:
            pass
            
        return None
    
    def fifteen_min_pivot_points(self):
        """Calculate pivot points using previous session data"""
        prev_session = self.previous_session_levels()
        if prev_session is None:
            return None
            
        # Standard pivot point calculation
        pivot = (prev_session['prev_high'] + prev_session['prev_low'] + prev_session['prev_close']) / 3
        
        # Support and resistance levels
        s1 = 2 * pivot - prev_session['prev_high']
        s2 = pivot - (prev_session['prev_high'] - prev_session['prev_low'])
        s3 = prev_session['prev_low'] - 2 * (prev_session['prev_high'] - pivot)
        
        r1 = 2 * pivot - prev_session['prev_low']
        r2 = pivot + (prev_session['prev_high'] - prev_session['prev_low'])
        r3 = prev_session['prev_high'] + 2 * (pivot - prev_session['prev_low'])
        
        # Fibonacci pivots for additional levels
        diff = prev_session['prev_high'] - prev_session['prev_low']
        fib_s1 = pivot - 0.382 * diff
        fib_s2 = pivot - 0.618 * diff
        fib_r1 = pivot + 0.382 * diff
        fib_r2 = pivot + 0.618 * diff
        fib_s3 = pivot - 1.0 * diff
        fib_r3 = pivot + 1.0 * diff

        
        return {
            'pivot': pivot,
            'standard': {
                'support': [s1, s2, s3],
                'resistance': [r1, r2, r3]
            },
            'fibonacci': {
                'support': [fib_s1, fib_s2, fib_s3],
                'resistance': [fib_r1, fib_r2, fib_r3]
            },
            'previous_session': prev_session
        }
    
    def premarket_analysis(self):
        """Analyze pre-market activity, including gap analysis"""
        if self.data is None:
            return None

        prev_session = self.previous_session_levels()
        current_session = self.session_levels()

        if not prev_session or not current_session:
            return {'gap_analysis': {'gap_type': 'Not Available'}}

        prev_close = prev_session.get('prev_close')
        current_open = current_session.get('session_open')

        if not prev_close or not current_open:
            return {'gap_analysis': {'gap_type': 'Not Available'}}

        gap_amount = current_open - prev_close
        gap_percent = (gap_amount / prev_close) * 100

        if abs(gap_percent) < 0.1:
            gap_type = 'No Gap'
        elif gap_amount > 0:
            gap_type = 'Gap Up'
        else:
            gap_type = 'Gap Down'

        return {
            'gap_analysis': {
                'gap_amount': gap_amount,
                'gap_percent': gap_percent,
                'gap_type': gap_type
            }
        }

    def real_time_vwap(self):
        """Calculate VWAP for current session and previous sessions"""
        if self.data is None:
            return None
            
        vwap_levels = {}
        
        # Group by trading date - handle timezone issues
        if hasattr(self.data.index, 'date'):
            unique_dates = set(self.data.index.date)
            data_dates = self.data.index.date
        else:
            unique_dates = set([d.date() for d in self.data.index])
            data_dates = [d.date() for d in self.data.index]
        
        for date in sorted(unique_dates):
            # Create mask for this date
            date_mask = [d == date for d in data_dates]
            day_data = self.data[date_mask]
            
            if not day_data.empty and day_data['Volume'].sum() > 0:
                # Typical price for VWAP calculation
                typical_price = (day_data['High'] + day_data['Low'] + day_data['Close']) / 3
                
                # Calculate cumulative VWAP for the session
                cum_volume = day_data['Volume'].cumsum()
                cum_pv = (typical_price * day_data['Volume']).cumsum()
                session_vwap = cum_pv / cum_volume
                
                vwap_levels[str(date)] = {
                    'final_vwap': session_vwap.iloc[-1],
                    'vwap_series': session_vwap,
                    'is_current': date == datetime.now().date()
                }
        
        return vwap_levels
    
    def fifteen_min_swing_levels(self, swing_strength=2):
        """
        Identify swing highs and lows from 15-minute data
        
        Args:
            swing_strength (int): Number of bars on each side to confirm swing
        """
        if self.data is None:
            return None
            
        highs = self.data['High'].values
        lows = self.data['Low'].values
        closes = self.data['Close'].values
        
        # Find swing points with smaller lookback for 15-min data
        swing_highs_idx = argrelextrema(highs, np.greater, order=swing_strength)[0]
        swing_lows_idx = argrelextrema(lows, np.less, order=swing_strength)[0]
        # Also find swing points from closing prices
        swing_close_highs_idx = argrelextrema(closes, np.greater, order=swing_strength)[0]
        swing_close_lows_idx = argrelextrema(closes, np.less, order=swing_strength)[0]
        
        # Get recent swing levels (last 50 bars for relevance)
        recent_bars = min(50, len(self.data))
        recent_swing_highs = []
        recent_swing_lows = []
        
        for idx in swing_highs_idx:
            if idx >= len(self.data) - recent_bars:
                recent_swing_highs.append({
                    'price': highs[idx],
                    'time': self.data.index[idx],
                    'bars_ago': len(self.data) - 1 - idx
                })
        
        # Add swing highs from closing prices
        for idx in swing_close_highs_idx:
            if idx >= len(self.data) - recent_bars:
                recent_swing_highs.append({
                    'price': closes[idx],
                    'time': self.data.index[idx],
                    'bars_ago': len(self.data) - 1 - idx
                })

        for idx in swing_lows_idx:
            if idx >= len(self.data) - recent_bars:
                recent_swing_lows.append({
                    'price': lows[idx],
                    'time': self.data.index[idx],
                    'bars_ago': len(self.data) - 1 - idx
                })

        # Add swing lows from closing prices
        for idx in swing_close_lows_idx:
            if idx >= len(self.data) - recent_bars:
                recent_swing_lows.append({
                    'price': closes[idx],
                    'time': self.data.index[idx],
                    'bars_ago': len(self.data) - 1 - idx
                })
        
        # Sort by recency and strength
        recent_swing_highs.sort(key=lambda x: x['bars_ago'])
        recent_swing_lows.sort(key=lambda x: x['bars_ago'])
        
        # Cluster similar price levels
        def cluster_swing_levels(swings, tolerance=0.003):
            if not swings:
                return []
                
            clustered = []
            swings_sorted = sorted(swings, key=lambda x: x['price'])
            
            current_cluster = [swings_sorted[0]]
            
            for swing in swings_sorted[1:]:
                if abs(swing['price'] - current_cluster[-1]['price']) / current_cluster[-1]['price'] <= tolerance:
                    current_cluster.append(swing)
                else:
                    # Calculate cluster strength and representative price
                    cluster_price = np.mean([s['price'] for s in current_cluster])
                    cluster_strength = len(current_cluster)
                    most_recent = min(current_cluster, key=lambda x: x['bars_ago'])
                    
                    clustered.append({
                        'price': cluster_price,
                        'strength': cluster_strength,
                        'most_recent_time': most_recent['time'],
                        'bars_ago': most_recent['bars_ago']
                    })
                    current_cluster = [swing]
            
            # Add last cluster
            if current_cluster:
                cluster_price = np.mean([s['price'] for s in current_cluster])
                cluster_strength = len(current_cluster)
                most_recent = min(current_cluster, key=lambda x: x['bars_ago'])
                
                clustered.append({
                    'price': cluster_price,
                    'strength': cluster_strength,
                    'most_recent_time': most_recent['time'],
                    'bars_ago': most_recent['bars_ago']
                })
            
            return sorted(clustered, key=lambda x: (x['strength'], -x['bars_ago']), reverse=True)
        
        clustered_highs = cluster_swing_levels(recent_swing_highs)
        clustered_lows = cluster_swing_levels(recent_swing_lows)
        
        # Filter by current price
        current = self.current_price
        resistance_levels = [level for level in clustered_highs if level['price'] > current]
        support_levels = [level for level in clustered_lows if level['price'] < current]
        
        return {
            'resistance': resistance_levels[:5],
            'support': support_levels[:5],
            'all_swings': {
                'highs': recent_swing_highs,
                'lows': recent_swing_lows
            }
        }
    
    def scalping_moving_averages(self):
        """Calculate fast moving averages suitable for scalping"""
        if self.data is None:
            return None
            
        # Very short-term MAs for 15-min scalping
        periods = [8, 13, 21, 34, 55]  # Fibonacci-based periods
        
        mas = {}
        emas = {}
        
        for period in periods:
            if len(self.data) >= period:
                # Simple MA
                ma_value = self.data['Close'].rolling(period).mean().iloc[-1]
                mas[f'MA_{period}'] = ma_value
                
                # Exponential MA (more responsive)
                ema_value = self.data['Close'].ewm(span=period).mean().iloc[-1]
                emas[f'EMA_{period}'] = ema_value
        
        # Hull Moving Average for even faster signals
        if len(self.data) >= 16:
            def hull_ma(prices, period):
                wma1 = prices.rolling(period//2).apply(lambda x: np.average(x, weights=range(1, len(x)+1)))
                wma2 = prices.rolling(period).apply(lambda x: np.average(x, weights=range(1, len(x)+1)))
                diff = 2 * wma1 - wma2
                hull = diff.rolling(int(np.sqrt(period))).apply(lambda x: np.average(x, weights=range(1, len(x)+1)))
                return hull.iloc[-1]
            
            try:
                hull_9 = hull_ma(self.data['Close'], 9)
                hull_21 = hull_ma(self.data['Close'], 21)
                
                return {
                    'simple_mas': mas,
                    'exponential_mas': emas,
                    'hull_mas': {'HMA_9': hull_9, 'HMA_21': hull_21}
                }
            except:
                return {
                    'simple_mas': mas,
                    'exponential_mas': emas
                }
        
        return {
            'simple_mas': mas,
            'exponential_mas': emas
        }
    
    def volume_profile_15min(self, profile_bars=96):  # 24 hours of 15-min bars
        """Calculate volume profile for recent 15-minute data"""
        if self.data is None:
            return None
            
        # Use recent data for volume profile
        recent_data = self.data.tail(profile_bars)
        
        # Create price bins
        price_min = recent_data['Low'].min()
        price_max = recent_data['High'].max()
        num_bins = 20
        price_bins = np.linspace(price_min, price_max, num_bins)
        
        volume_profile = []
        
        for i in range(len(price_bins) - 1):
            bin_low = price_bins[i]
            bin_high = price_bins[i + 1]
            bin_mid = (bin_low + bin_high) / 2
            
            # Volume in this price range
            mask = (recent_data['Low'] <= bin_high) & (recent_data['High'] >= bin_low)
            volume_in_bin = recent_data.loc[mask, 'Volume'].sum()
            
            if volume_in_bin > 0:
                volume_profile.append({
                    'price': bin_mid,
                    'volume': volume_in_bin,
                    'price_range': (bin_low, bin_high)
                })
        
        # Sort by volume and get high volume nodes (HVN)
        volume_profile.sort(key=lambda x: x['volume'], reverse=True)
        
        return {
            'high_volume_nodes': volume_profile[:5],
            'point_of_control': volume_profile[0] if volume_profile else None
        }
    
    def calculate_all_15min_levels(self):
        """Calculate all 15-minute support and resistance levels"""
        if not self.fetch_15min_data(include_premarket=True):
            return None
            
        results = {
            'symbol': self.symbol,
            'current_price': self.current_price,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'timeframe': '15-minute',
            'session_levels': self.session_levels(),
            'premarket_analysis': self.premarket_analysis(),
            'pivot_points': self.fifteen_min_pivot_points(),
            'swing_levels': self.fifteen_min_swing_levels(),
            'vwap_levels': self.real_time_vwap(),
            'moving_averages': self.scalping_moving_averages(),
            'volume_profile': self.volume_profile_15min()
        }
        
        return results
    
    def get_scalping_summary(self):
        """Get optimized summary for scalping and day trading including pre-market"""
        results = self.calculate_all_15min_levels()
        if not results:
            return None
            
        summary = {
            'symbol': self.symbol,
            'current_price': self.current_price,
            'timeframe': '15-minute',
            'timestamp': results['timestamp'],
            'immediate_levels': {},
            'premarket_summary': {}
        }
        
        current = self.current_price
        
        # Session levels including pre-market
        if results['session_levels']:
            session = results['session_levels']
            if session.get('regular_high'):
                summary['immediate_levels']['regular_high'] = session['regular_high']
            if session.get('regular_low'):
                summary['immediate_levels']['regular_low'] = session['regular_low']
            if session.get('opening_range_high'):
                summary['immediate_levels']['opening_range_high'] = session['opening_range_high']
            if session.get('opening_range_low'):
                summary['immediate_levels']['opening_range_low'] = session['opening_range_low']
            
            # Pre-market levels
            if session.get('premarket'):
                pm = session['premarket']
                if pm.get('pm_high'):
                    summary['immediate_levels']['pm_high'] = pm['pm_high']
                if pm.get('pm_low'):
                    summary['immediate_levels']['pm_low'] = pm['pm_low']
                if pm.get('pm_vwap'):
                    summary['immediate_levels']['pm_vwap'] = pm['pm_vwap']
                
                # Pre-market summary
                summary['premarket_summary'] = {
                    'pm_range': pm.get('pm_high', 0) - pm.get('pm_low', 0) if pm.get('pm_high') and pm.get('pm_low') else 0,
                    'pm_volume': pm.get('pm_volume', 0),
                    'pm_change': pm.get('pm_close', 0) - pm.get('pm_open', 0) if pm.get('pm_close') and pm.get('pm_open') else 0
                }
        
        # Pre-market analysis
        if results['premarket_analysis']:
            pm_analysis = results['premarket_analysis']
            if pm_analysis.get('gap_analysis'):
                gap = pm_analysis['gap_analysis']
                summary['gap_info'] = {
                    'gap_amount': gap.get('gap_amount', 0),
                    'gap_percent': gap.get('gap_percent', 0),
                    'gap_type': gap.get('gap_type', 'No Gap')
                }
        
        # Pivot points
        if results['pivot_points']:
            pp = results['pivot_points']
            summary['immediate_levels']['pivot'] = pp['pivot']
            summary['immediate_levels']['r1'] = pp['standard']['resistance'][0]
            summary['immediate_levels']['r2'] = pp['standard']['resistance'][1]
            summary['immediate_levels']['r3'] = pp['standard']['resistance'][2]
            summary['immediate_levels']['s1'] = pp['standard']['support'][0]
            summary['immediate_levels']['s2'] = pp['standard']['support'][1]
            summary['immediate_levels']['s3'] = pp['standard']['support'][2]
        
        # Current VWAP
        if results['vwap_levels']:
            for date, vwap_data in results['vwap_levels'].items():
                if vwap_data['is_current']:
                    summary['immediate_levels']['vwap'] = vwap_data['final_vwap']
                    break
        
        # Nearest swing levels
        if results['swing_levels']:
            swings = results['swing_levels']
            if swings['resistance']:
                summary['immediate_levels']['nearest_resistance'] = swings['resistance'][0]['price']
            if swings['support']:
                summary['immediate_levels']['nearest_support'] = swings['support'][0]['price']
        
        return summary
    
    def plot_15min_chart(self, bars_to_show=96):  # 24 hours of 15-min bars
        """
        Plot 15-minute candlestick chart with all scalping levels
        
        Args:
            bars_to_show (int): Number of 15-minute bars to display
        """
        if self.data is None:
            print("No data available. Run calculate_all_15min_levels() first.")
            return
            
        # Get recent data for plotting
        recent_data = self.data.tail(bars_to_show).copy()
        
        # Create figure with subplots
        fig, ax1 = plt.subplots(1, 1, figsize=(12, 6))
        
        # Prepare candlestick data
        opens = recent_data['Open'].values
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
        closes = recent_data['Close'].values
        times = recent_data.index
        
        # Create candlestick chart
        self.plot_candlesticks(ax1, times, opens, highs, lows, closes)
        
        # Session levels with pre-market highlighting
        session_data = self.session_levels()
        if session_data:
            # Regular session levels
            # if session_data['session_high']:
            #     ax1.axhline(y=session_data['session_high'], color='red', linestyle='-', 
            #                linewidth=2.5, alpha=0.9, label='session High', zorder=5)
            # if session_data['session_low']:
            #     ax1.axhline(y=session_data['session_low'], color='green', linestyle='-', 
            #                linewidth=2.5, alpha=0.9, label='session Low', zorder=5)
            
            # Opening range
            if session_data['opening_range_high'] and session_data['opening_range_low']:
                or_high = session_data['opening_range_high']
                or_low = session_data['opening_range_low']
                ax1.axhline(y=or_high, color='orange', linestyle='--', 
                           linewidth=2, alpha=0.8, label='OR High', zorder=4)
                ax1.axhline(y=or_low, color='orange', linestyle='--', 
                           linewidth=2, alpha=0.8, label='OR Low', zorder=4)
                
                # Highlight opening range area
                ax1.fill_between(times, or_low, or_high, alpha=0.1, color='orange', zorder=1)
            
            # # Pre-market levels
            # if session_data['premarket']:
            #     pm = session_data['premarket']
            #     if pm.get('pm_high'):
            #         ax1.axhline(y=pm['pm_high'], color='magenta', linestyle='-.', 
            #                    linewidth=2, alpha=0.8, label='PM High', zorder=4)
            #     if pm.get('pm_low'):
            #         ax1.axhline(y=pm['pm_low'], color='cyan', linestyle='-.', 
            #                    linewidth=2, alpha=0.8, label='PM Low', zorder=4)
            #     if pm.get('pm_vwap'):
            #         ax1.axhline(y=pm['pm_vwap'], color='purple', linestyle=':', 
            #                    linewidth=1.5, alpha=0.7, label='PM VWAP', zorder=3)
        
        # Pivot points
        pivot_data = self.fifteen_min_pivot_points()
        if pivot_data:
            ax1.axhline(y=pivot_data['pivot'], color='blue', linestyle='-', 
                       linewidth=2.5, alpha=0.9, label='Pivot Point', zorder=5)
            
            # Standard pivot levels
            # colors_r = ['darkred', 'red', 'lightcoral']
            # colors_s = ['darkgreen', 'green', 'lightgreen']
            
            # for i, resistance in enumerate(pivot_data['standard']['resistance'][:3]):
            #     ax1.axhline(y=resistance, color=colors_r[i], linestyle=':', 
            #                linewidth=2, alpha=0.7, label=f'R{i+1}' if i == 0 else '', zorder=3)
                
            # for i, support in enumerate(pivot_data['standard']['support'][:3]):
            #     ax1.axhline(y=support, color=colors_s[i], linestyle=':', 
            #                linewidth=2, alpha=0.7, label=f'S{i+1}' if i == 0 else '', zorder=3)
            
            # Previous day levels
            prev = pivot_data['previous_session']
            if prev:
                ax1.axhline(y=prev['prev_high'], color='purple', linestyle='-.', 
                           linewidth=1.5, alpha=0.6, label='Prev High', zorder=2)
                ax1.axhline(y=prev['prev_low'], color='purple', linestyle='-.', 
                           linewidth=1.5, alpha=0.6, label='Prev Low', zorder=2)
                ax1.axhline(y=prev['prev_close'], color='gray', linestyle='-.', 
                           linewidth=1.5, alpha=0.6, label='Prev Close', zorder=2)
        
        # VWAP - dynamic line
        vwap_data = self.real_time_vwap()
        if vwap_data:
            current_date = datetime.now().date()
            for date_str, vwap_info in vwap_data.items():
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    if date_obj == current_date and len(vwap_info['vwap_series']) > 0:
                        # Get current session data
                        if hasattr(self.data.index, 'date'):
                            data_dates = self.data.index.date
                        else:
                            data_dates = [d.date() for d in self.data.index]
                        
                        current_mask = [d == current_date for d in data_dates]
                        session_data_for_vwap = self.data[current_mask]
                        
                        if not session_data_for_vwap.empty:
                            vwap_series = vwap_info['vwap_series']
                            # Match VWAP series length with session data
                            min_length = min(len(vwap_series), len(session_data_for_vwap))
                            vwap_times = session_data_for_vwap.index[:min_length]
                            vwap_values = vwap_series.iloc[:min_length]
                            
                            ax1.plot(vwap_times, vwap_values, color='purple', linewidth=3, 
                                   alpha=0.9, label='VWAP', zorder=6)
                        break
                except:
                    continue
        
        # Swing levels
        swing_data = self.fifteen_min_swing_levels()
        if swing_data:
            # Plot swing highs (resistance)
            for i, swing in enumerate(swing_data['resistance'][:3]):
                alpha = 0.7 - (i * 0.2)  # Fade older levels
                line_width = 2 - (i * 0.3)
                ax1.axhline(y=swing['price'], color='red', linestyle='--', 
                           alpha=alpha, linewidth=line_width, zorder=2)
                
                # Add strength indicator
                if swing['strength'] > 1:
                    ax1.text(times[-1], swing['price'], f" R-{swing['strength']}", 
                           color='red', alpha=0.8, fontsize=10, va='center')
            
            # Plot swing lows (support)
            for i, swing in enumerate(swing_data['support'][:3]):
                alpha = 0.7 - (i * 0.2)  # Fade older levels
                line_width = 2 - (i * 0.3)
                ax1.axhline(y=swing['price'], color='green', linestyle='--', 
                           alpha=alpha, linewidth=line_width, zorder=2)
                
                # Add strength indicator
                if swing['strength'] > 1:
                    ax1.text(times[-1], swing['price'], f" S-{swing['strength']}", 
                           color='green', alpha=0.8, fontsize=10, va='center')

        # Moving Averages
        ma_data = self.scalping_moving_averages()
        if ma_data and 'exponential_mas' in ma_data:
            ema_periods = [9, 20]
            ema_colors = [ '#3357FF', '#FF33A1', '#FF5733', '#33FF57']
            
            for i, period in enumerate(ema_periods):
                if len(recent_data) >= period:
                    ema_series = recent_data['Close'].ewm(span=period, adjust=False).mean()
                    ax1.plot(recent_data.index, ema_series, 
                             label=f'EMA {period}', 
                             color=ema_colors[i % len(ema_colors)], 
                             linewidth=1.5, alpha=0.7, zorder=4)


        # Candlestick patterns
        patterns = self.identify_candlestick_patterns(recent_data)
        if patterns:
            for p in patterns:
                y_pos = p['y_pos']
                color = 'green' if p['type'] == 'Bullish' else 'red' if p['type'] == 'Bearish' else 'black'
                va = 'bottom' if p['type'] == 'Bullish' else 'top'
                offset = (max(highs) - min(lows)) * 0.02  # 2% of price range
                y_pos = y_pos + offset if va == 'bottom' else y_pos - offset
                
                ax1.text(p['time'], y_pos, p['name'], color=color, fontsize=8, 
                         fontweight='bold', ha='center', va=va, zorder=10)
        
        # Fair Value Gaps (FVG)
        fvg_gaps = self.identify_fair_value_gaps(recent_data)
        if fvg_gaps:
            for gap in fvg_gaps:
                color = 'darkgreen' if gap['type'] == 'Bullish' else 'darkred'
                ax1.add_patch(Rectangle((gap['start_time'], gap['bottom']),
                                        (gap['end_time'] - gap['start_time'])*3,
                                        gap['top'] - gap['bottom'],
                                        facecolor=color, alpha=0.3,
                                        zorder=1))

        # Chart formatting
        ax1.set_title(f'{self.symbol} - 15-Minute Candlestick Chart with Support/Resistance\n'
                     f'Current Price: ${self.current_price:.2f} | '
                     f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
                     fontsize=14, pad=20)
        ax1.set_ylabel('Price ($)', fontsize=14)
        ax1.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=11)
        ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)

        ax1.set_xlabel('Time (15-minute intervals)', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        ax1.tick_params(axis='both', labelsize=10)

        plt.subplots_adjust(right=1.75, bottom=0.2)
        plt.tight_layout(pad=3.0)
        
        # Save plot to a memory buffer instead of showing it
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)  # Close the figure to free up memory
        return buf

    def plot_candlesticks(self, ax, times, opens, highs, lows, closes):
        """
        Plot candlestick chart on given axis
        
        Args:
            ax: Matplotlib axis
            times: Time index
            opens, highs, lows, closes: OHLC price arrays
        """
        # Calculate candle width (80% of time interval)
        if len(times) > 1:
            time_delta = times[1] - times[0]
            candle_width = time_delta * 0.8
        else:
            candle_width = timedelta(minutes=12)  # Default for 15-min chart
        
        for i in range(len(times)):
            time = times[i]
            open_price = opens[i]
            high_price = highs[i]
            low_price = lows[i]
            close_price = closes[i]
            
            # Determine candle color
            if close_price >= open_price:
                # Bullish candle (green)
                body_color = 'green'
                edge_color = 'darkgreen'
                alpha = 0.8
            else:
                # Bearish candle (red)
                body_color = 'red'
                edge_color = 'darkred'
                alpha = 0.8
            
            # Draw high-low line (wick)
            ax.plot([time, time], [low_price, high_price], 
                   color='black', linewidth=1, alpha=0.8, zorder=2)
            
            # Draw candle body
            body_height = abs(close_price - open_price)
            body_bottom = min(open_price, close_price)
            
            if body_height > 0:
                # Regular candle with body
                rectangle = Rectangle((time - candle_width/2, body_bottom), 
                                    candle_width, body_height,
                                    facecolor=body_color, 
                                    edgecolor=edge_color,
                                    alpha=alpha,
                                    linewidth=1,
                                    zorder=3)
                ax.add_patch(rectangle)
            else:
                # Doji candle (open == close)
                ax.plot([time - candle_width/2, time + candle_width/2], 
                       [close_price, close_price], 
                       color='black', linewidth=2, zorder=3)
        
        # Set axis limits
        ax.set_xlim(times[0] - candle_width, times[-1] + candle_width)
        
        # Add price labels on right side
        current_price = closes[-1]
        price_range = max(highs) - min(lows)
        
        # Add current price line and label
        ax.axhline(y=current_price, color='blue', linewidth=0.7, alpha=0.9, zorder=7)
        # ax.text(times[-1] + candle_width, current_price, 
        #        f' ${current_price:.2f}', 
        #        color='blue', fontsize=10, 
        #        va='center', ha='left', zorder=8,
        #        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.8))

    def identify_fair_value_gaps(self, data):
        """Identifies Fair Value Gaps (FVG) in the data."""
        gaps = []
        if len(data) < 3:
            return gaps

        for i in range(len(data) - 2):
            # Candles 1, 2, and 3
            c1_high = data['High'].iloc[i]
            c1_low = data['Low'].iloc[i]
            
            c3_high = data['High'].iloc[i+2]
            c3_low = data['Low'].iloc[i+2]

            # Bullish FVG (BISI - Buyside Imbalance Sellside Inefficiency)
            # Low of candle 1 is above the high of candle 3
            if c1_low > c3_high:
                gaps.append({
                    'type': 'Bullish',
                    'start_time': data.index[i+1],
                    'end_time': data.index[i+2],
                    'top': c1_low,
                    'bottom': c3_high
                })

            # Bearish FVG (SIBI - Sellside Imbalance Buyside Inefficiency)
            # High of candle 1 is below the low of candle 3
            if c1_high < c3_low:
                gaps.append({
                    'type': 'Bearish',
                    'start_time': data.index[i+1],
                    'end_time': data.index[i+2],
                    'top': c3_low,
                    'bottom': c1_high
                })
        return gaps

    def identify_candlestick_patterns(self, data):
        """Identifies common candlestick patterns in the data."""
        patterns = []
        if len(data) < 2:
            return patterns

        for i in range(1, len(data)):
            # Current bar
            o, h, l, c = data['Open'].iloc[i], data['High'].iloc[i], data['Low'].iloc[i], data['Close'].iloc[i]
            # Previous bar
            o_prev, c_prev = data['Open'].iloc[i-1], data['Close'].iloc[i-1]
            
            body = abs(c - o)
            price_range = h - l
            
            # --- Single-bar patterns ---
            
            # Doji (small body)
            # if price_range > 0 and body / price_range < 0.1:
            #     patterns.append({'name': 'Dji', 'type': 'Neutral', 'time': data.index[i], 'y_pos': h})

            # Hammer (bullish reversal) / Hanging Man (bearish reversal)
            lower_wick = min(o, c) - l
            upper_wick = h - max(o, c)
            
            # if price_range > 0 and body > 0 and lower_wick > body * 2 and upper_wick < body:
            #     # Hammer (check for preceding downtrend)
            #     if c_prev < o_prev:
            #         patterns.append({'name': 'Hammer', 'type': 'Bullish', 'time': data.index[i], 'y_pos': l})
            
            # if price_range > 0 and body > 0 and upper_wick > body * 2 and lower_wick < body:
            #     # Hanging Man (check for preceding uptrend)
            #     if c_prev > o_prev:
            #         patterns.append({'name': 'Hanging Man', 'type': 'Bearish', 'time': data.index[i], 'y_pos': h})

            # Marubozu (strong momentum)
            if price_range > 0 and body / price_range > 0.95:
                if c > o:
                    patterns.append({'name': 'UMbozu', 'type': 'Bullish', 'time': data.index[i], 'y_pos': l})
                else:
                    patterns.append({'name': 'EMbozu', 'type': 'Bearish', 'time': data.index[i], 'y_pos': h})

            # --- Two-bar patterns ---

            # Bullish Engulfing
            if c > o and c_prev < o_prev and c > o_prev and o < c_prev:
                patterns.append({'name': 'UE', 'type': 'Bullish', 'time': data.index[i], 'y_pos': l})

            # Bearish Engulfing
            if c < o and c_prev > o_prev and c < o_prev and o > c_prev:
                patterns.append({'name': 'EE', 'type': 'Bearish', 'time': data.index[i], 'y_pos': h})

        return patterns

# Scalping/Day Trading Example
def scalping_example():
    """Example optimized for scalping and short-term day trading"""
    
    # Initialize for 15-minute scalping (2 days of data)
    scalper = FifteenMinuteSupportResistance("SPY", days_back=2)
    
    # Get comprehensive analysis
    full_results = scalper.calculate_all_15min_levels()
    
    # Get scalping summary
    summary = scalper.get_scalping_summary()
    
    if summary and full_results:
        print(f"\n=== SCALPING SETUP: {summary['symbol']} (15-Min + Pre-Market) ===")
        print(f"Current Price: ${summary['current_price']:.2f}")
        print(f"Analysis Time: {summary['timestamp']}")
        
        # Pre-market summary
        if 'premarket_summary' in summary and summary['premarket_summary']:
            pm_sum = summary['premarket_summary']
            print(f"\n--- PRE-MARKET ANALYSIS ---")
            print(f"PM Range: ${pm_sum.get('pm_range', 0):.2f}")
            print(f"PM Volume: {pm_sum.get('pm_volume', 0):,}")
            print(f"PM Change: ${pm_sum.get('pm_change', 0):.2f}")
            
            # Gap analysis
            if 'gap_info' in summary:
                gap = summary['gap_info']
                gap_emoji = "⬆️" if gap['gap_amount'] > 0 else "⬇️" if gap['gap_amount'] < 0 else "➡️"
                print(f"Gap: {gap_emoji} ${gap['gap_amount']:.2f} ({gap['gap_percent']:.1f}%) - {gap['gap_type']}")
        
        print(f"\n--- IMMEDIATE SCALPING LEVELS ---")
        levels = summary['immediate_levels']
        current = summary['current_price']
        
        # Create sorted level list for trading
        level_list = []
        for name, price in levels.items():
            if price and not pd.isna(price):
                distance = abs(price - current)
                distance_pct = (distance / current) * 100
                level_list.append({
                    'name': name,
                    'price': price,
                    'distance_pct': distance_pct,
                    'type': 'RESISTANCE' if price > current else 'SUPPORT'
                })
        
        # Sort by distance from current price (nearest first)
        level_list.sort(key=lambda x: x['distance_pct'])
        
        print(f"\nNEAREST LEVELS (sorted by distance):")
        for level in level_list[:8]:  # Show top 8 nearest levels
            emoji = "🔴" if level['type'] == 'RESISTANCE' else "🟢"
            print(f"{level['name'].upper():18} ${level['price']:7.2f} {emoji} "
                  f"{level['type']:10} ({level['distance_pct']:.2f}% away)")
        
        # Scalping trade setup
        print(f"\n--- SCALPING TRADE SETUP ---")
        
        # Find immediate resistance and support
        immediate_resistance = [l for l in level_list if l['type'] == 'RESISTANCE'][:2]
        immediate_support = [l for l in level_list if l['type'] == 'SUPPORT'][:2]
        
        if immediate_resistance:
            target = immediate_resistance[0]
            print(f"🎯 LONG Target 1: ${target['price']:.2f} ({target['name']}) "
                  f"[+{((target['price']-current)/current)*100:.2f}%]")
            if len(immediate_resistance) > 1:
                target2 = immediate_resistance[1]
                print(f"🎯 LONG Target 2: ${target2['price']:.2f} ({target2['name']}) "
                      f"[+{((target2['price']-current)/current)*100:.2f}%]")
        
        if immediate_support:
            stop = immediate_support[0]
            print(f"🛑 Stop Loss: ${stop['price']:.2f} ({stop['name']}) "
                  f"[-{((current-stop['price'])/current)*100:.2f}%]")
        
        # Risk management for scalping
        if immediate_resistance and immediate_support:
            target_price = immediate_resistance[0]['price']
            stop_price = immediate_support[0]['price']
            
            profit_potential = target_price - current
            risk_amount = current - stop_price
            
            if risk_amount > 0:
                risk_reward = profit_potential / risk_amount
                print(f"\n--- SCALPING RISK MANAGEMENT ---")
                print(f"💰 Profit Potential: ${profit_potential:.2f}")
                print(f"⚠️  Risk Amount: ${risk_amount:.2f}")
                print(f"📊 Risk/Reward Ratio: 1:{risk_reward:.2f}")
                
                if risk_reward >= 1.5:
                    print("✅ GOOD risk/reward for scalping")
                elif risk_reward >= 1.0:
                    print("⚠️  ACCEPTABLE risk/reward")
                else:
                    print("❌ POOR risk/reward - consider waiting")
        
        print(f"\nGenerating 15-minute chart...")
        scalper.plot_15min_chart(bars_to_show=96)  # 24 hours

    return scalper

