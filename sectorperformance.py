"""
sector_performance.py
=====================
1. Fetches yesterday's close + current price for all 11 S&P 500 sector ETFs
   via the Yahoo Finance v8 chart API (same endpoint used by dataManager.py).
2. Calculates % change, sorts descending, keeps only Top-3 and Bottom-3.
3. Renders a compact single-side bar chart (all bars extend right from zero).
   Green = gain, Red = loss.

Run:
    python sector_performance.py
"""

import time
import requests
import numpy  as np
import pandas as pd
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from datetime import datetime, timedelta
import warnings
import io
import base64
import gc
warnings.filterwarnings('ignore')

# ── S&P 500 Select Sector SPDR ETFs ──────────────────────────────────────────

class SectorPerformance:
    def __init__(self):
        self.SECTORS = {
            "XLB":  "Materials",
            "XLC":  "Comm Svcs",
            "XLE":  "Energy",
            "XLF":  "Financials",
            "XLI":  "Industrials",
            "XLK":  "Technology",
            "XLP":  "Cons Staples",
            "XLRE": "Real Estate",
            "XLU":  "Utilities",
            "XLV":  "Health Care",
            "XLY":  "Cons Discret",
        }

        self.HEADERS = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept':          'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer':         'https://finance.yahoo.com/',
        }

# ── 1. Data retrieval ─────────────────────────────────────────────────────────

    def fetch_sector_data(self) -> pd.DataFrame:
        """
        Downloads intraday data to accurately parse out the most recent 
        completed 16:00 EST closing price as a baseline, comparing it against
        the absolute latest market print (Pre-market, Regular, or Post-market).

        Returns DataFrame with columns:
            symbol | sector | prev_close | curr_price | change | change_pct
        sorted descending by change_pct.
        """
        records = []
        current_weekday = datetime.now().weekday()
        
        if current_weekday == 0:     # Monday
            history_period = "4d"    # Covers Mon, Sun, Sat, Fri
        elif current_weekday in [5, 6]: # Weekend tracking
            history_period = "3d"    # Covers Weekend + Friday
        else:                        # Tuesday through Friday
            history_period = "2d"    # Covers Today + Yesterday

        for symbol, sector in self.SECTORS.items():
            try:
                ticker = yf.Ticker(symbol)
                # Fetch 5 days of 1-minute bars with extended market sessions included
                data = ticker.history(period=history_period, interval="1m", prepost=True)

                if data.empty or len(data) <= 1:
                    print(f"  {symbol}: not enough data")
                    continue

                # 1. Grab the absolute latest available price row (Pre, Reg, or Post)
                curr_row = data.iloc[-1]
                curr = round(float(curr_row['Close']), 2)
                latest_timestamp = data.index[-1]

                # 2. Filter for standard regular session hours (09:30 to 16:00 EST/EDT)
                # Note: yfinance localized timestamps reflect the exchange's timezone natively.
                reg_hours_data = data.between_time("09:29", "15:59")
                
                # Filter out regular hour sessions that match or come after the current tick time
                # (This ensures that during Next Day Pre-Market, it looks back at Yesterday's Regular Close)
                past_reg_data = reg_hours_data[reg_hours_data.index < latest_timestamp]

                if not past_reg_data.empty:
                    # The final tick of the last completed standard session is the official baseline
                    prev = round(float(past_reg_data['Close'].iloc[-1]), 2)
                else:
                    # Emergency fallback to first available price if historical regular data missing
                    prev = round(float(data['Close'].iloc[0]), 2)

                chg  = round(curr - prev, 2)
                pct  = round((chg / prev) * 100, 2)

                records.append({'symbol': symbol, 'sector': sector,
                                'prev_close': prev, 'curr_price': curr,
                                'change': chg, 'change_pct': pct})
                del data

            except requests.exceptions.HTTPError as e:
                print(f"  {symbol}: HTTP error – {e}")
            except Exception as e:
                print(f"  {symbol}: {e}")

            time.sleep(0.25)

        if not records:
            raise RuntimeError(
                "No data retrieved. Check internet connection and Yahoo Finance access."
            )

        df_all = (pd.DataFrame(records)
                    .sort_values('change_pct', ascending=False)
                    .reset_index(drop=True))

        del records
        gc.collect()
        print("\n── All sectors ───────────────────────────────────────")
        print(df_all[['symbol','prev_close','curr_price','change_pct']].to_string(index=False))

        return df_all

    # ── 3. Bar chart ──────────────────────────────────────────────────────────────

    def plot_sector_chart(self, df: pd.DataFrame, out_path: str = "sector_performance.png"):
        """
        Single-side horizontal bar chart.
        All bars extend right; length = abs(change_pct).
        Green = gain, Red = loss.
        """
        BG       = "#0d1117"
        CARD_BG  = "#161b22"
        GREEN    = "#3fb950"
        GREEN_BG = "#162a1e"
        RED      = "#f85149"
        RED_BG   = "#2a1616"
        TEXT_PRI = "#e6edf3"
        TEXT_SEC = "#8b949e"
        DIVIDER  = "#30363d"
        GRID     = "#21262d"
        MONO     = "DejaVu Sans Mono"

        n      = len(df)
        fig_h  = max(3, n * 0.2 + 1)  # scale height to row count
        fig, ax = plt.subplots(figsize=(6, fig_h), facecolor=BG)
        ax.set_facecolor(CARD_BG)

        # y_pos reversed so row 0 (best gainer) appears at top
        y_pos     = np.arange(n)[::-1]
        values    = df['change_pct'].tolist()
        absvals   = [abs(v) for v in values]
        colors    = [GREEN if v >= 0 else RED    for v in values]
        bg_colors = [GREEN_BG if v >= 0 else RED_BG for v in values]
        x_max     = max(absvals) * 1.55 if absvals else 1.0

        # background glow rows
        for y, bgc in zip(y_pos, bg_colors):
            ax.barh(y, x_max, height=0.74, color=bgc, alpha=0.28, zorder=1, left=0)

        # main bars
        bars = ax.barh(y_pos, absvals, height=0.62, color=colors,
                    alpha=0.93, zorder=3, linewidth=0)

        # pct labels at bar tip
        for bar, val, av in zip(bars, values, absvals):
            sign = '+' if val > 0 else '−'
            ax.text(av + x_max * 0.022,
                    bar.get_y() + bar.get_height() / 2,
                    f"{sign}{av:.2f}%",
                    va='center', ha='left', fontsize=11, fontweight='bold',
                    color=GREEN if val >= 0 else RED,
                    fontfamily=MONO, zorder=5)

        # y-axis labels
        y_labels = [f"{row.symbol:<5}" for row in df.itertuples()]
        ax.set_yticks(y_pos)
        ax.set_yticklabels(y_labels, fontsize=8, color=TEXT_PRI, fontfamily=MONO)
        ax.tick_params(axis='y', length=0, pad=10)

        # dashed divider: dynamic position between gainers and losers
        n_gainers = int((df['change_pct'] >= 0).sum())
        if 0 < n_gainers < n:
            divider_y = n - n_gainers - 0.5
            ax.axhline(y=divider_y, color=DIVIDER, linewidth=1.5,
                       linestyle='--', zorder=6, alpha=0.85)

        # x-axis
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f%%'))
        ax.tick_params(axis='x', colors=TEXT_SEC, labelsize=9)
        ax.set_xlim(0, x_max * 1.38)
        ax.xaxis.grid(True, color=GRID, linewidth=0.7, linestyle='--', zorder=0, alpha=0.7)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

        # title
        ax.set_title(
            f"S&P 500 Sector Performance  "
            f"Green:{(df['change_pct'] >= 0).sum()}, Red:{(df['change_pct'] < 0).sum()}",
            loc='left', pad=14, fontsize=9, fontweight='normal', color=TEXT_PRI)

        fig.text(0.99, 0.008, "Source: Yahoo Finance",
                ha='right', va='bottom', fontsize=7, color=TEXT_SEC, style='italic')

        plt.tight_layout(rect=[0, 0.01, 0.93, 1])
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=160, bbox_inches='tight',
                    facecolor=BG, edgecolor='none')
        buf.seek(0)
        plt.close(fig)
        return buf

    def processrequest(self):
        sectorperf = SectorPerformance()
        df  = sectorperf.fetch_sector_data()
        out = sectorperf.plot_sector_chart(df, out_path="sector_performance.png")

        return df, out