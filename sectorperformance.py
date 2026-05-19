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
        Downloads the last two trading-day closes for each sector ETF.

        FIX: Column names use underscores (no spaces) so itertuples()
            returns proper named attributes, avoiding the _3/_4/_5 fallbacks.

        Returns DataFrame with columns:
            symbol | sector | prev_close | curr_price | change | change_pct
        sorted descending by change_pct, filtered to Top-3 + Bottom-3.
        """
        records = []

        for symbol, sector in self.SECTORS.items():
            try:
                ticker = yf.Ticker(symbol)
                data= ticker.history(period="2d")

                if data.empty:
                    print(f"  {symbol}: not enough data ({len(closes)} bar(s))")
                    continue

                prev = round(float(data['Close'].iloc[-2]), 2)
                curr = round(float(data['Close'].iloc[-1]), 2)
                chg  = round(curr - prev, 2)
                pct  = round((chg / prev) * 100, 2)

                records.append({'symbol': symbol, 'sector': sector,
                                'prev_close': prev, 'curr_price': curr,
                                'change': chg, 'change_pct': pct})
                #print(f"  {symbol:5s}  prev={prev:8.2f}  curr={curr:8.2f}  {pct:+.2f}%")

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

        # top3    = df_all.head(3)
        # bottom3 = df_all.tail(3)

        print("\n── All sectors ───────────────────────────────────────")
        print(df_all[['symbol','prev_close','curr_price','change_pct']].to_string(index=False))
        # print("\n── Top 3 gainers ───────────────────────────────────────")
        # print(top3[['symbol','prev_close','curr_price','change_pct']].to_string(index=False))
        # print("\n── Bottom 3 losers ─────────────────────────────────────")
        # print(bottom3[['symbol','prev_close','curr_price','change_pct']].to_string(index=False))

        return df_all
        #return pd.concat([top3, bottom3]).reset_index(drop=True)


    # ── 3. Bar chart ──────────────────────────────────────────────────────────────

    def plot_sector_chart(self, df: pd.DataFrame, out_path: str = "sector_performance.png"):
        """
        Single-side horizontal bar chart.
        All bars extend right; length = abs(change_pct).
        Green = gain, Red = loss.
        Top-3 / Bottom-3 separated by a dashed divider.
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
        fig_h  = max(5.8, n * 0.62 + 1.8)  # scale height to row count
        fig, ax = plt.subplots(figsize=(11, fig_h), facecolor=BG)
        ax.set_facecolor(CARD_BG)

        # y_pos reversed so row 0 (best gainer) appears at top
        y_pos     = np.arange(n)[::-1]
        values    = df['change_pct'].tolist()
        absvals   = [abs(v) for v in values]
        colors    = [GREEN if v >= 0 else RED    for v in values]
        bg_colors = [GREEN_BG if v >= 0 else RED_BG for v in values]
        x_max     = max(absvals) * 1.55

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

        # y-axis labels built from named attributes (FIX applied here)
        y_labels = []
        for row in df.itertuples():
            arrow = "▲" if row.change_pct >= 0 else "▼"
            y_labels.append(
                f"{row.symbol:<5}"
            )
        ax.set_yticks(y_pos)
        ax.set_yticklabels(y_labels, fontsize=10.5, color=TEXT_PRI, fontfamily=MONO)
        ax.tick_params(axis='y', length=0, pad=10)

        # dashed divider: dynamic position between gainers and losers
        n_gainers = int((df['change_pct'] >= 0).sum())
        n_losers  = n - n_gainers
        if 0 < n_gainers < n:
            divider_y = n - n_gainers - 0.5
            ax.axhline(y=divider_y, color=DIVIDER, linewidth=1.5,
                       linestyle='--', zorder=6, alpha=0.85)
            gainer_mid = n - n_gainers / 2 - 0.5 + 0.35
            loser_mid  = n_losers / 2 - 0.5 - 0.35
            # ax.text(x_max * 0.01, gainer_mid,
            #         f"▲  GAINERS ({n_gainers})",
            #         color=GREEN, fontsize=7.8, fontfamily=MONO, fontweight='bold', alpha=0.85)
            # ax.text(x_max * 0.01, loser_mid,
            #         f"▼  LOSERS ({n_losers})",
            #         color=RED,   fontsize=7.8, fontfamily=MONO, fontweight='bold', alpha=0.85)

        # x-axis
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f%%'))
        ax.tick_params(axis='x', colors=TEXT_SEC, labelsize=9)
        ax.set_xlim(0, x_max * 1.38)
        ax.xaxis.grid(True, color=GRID, linewidth=0.7, linestyle='--', zorder=0, alpha=0.7)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

        # right-side prev close column
        #ax.text(1.01, 1.035, "PREV CLOSE", transform=ax.transAxes,
        #        fontsize=7.5, color=TEXT_SEC, ha='left', fontfamily=MONO, fontweight='bold')
        # for row, y in zip(df.itertuples(), y_pos):
        #     ax.text(1.01, (y + 0.5) / n, f"${row.prev_close:.2f}",
        #             transform=ax.transAxes, va='center', ha='left',
        #             fontsize=9, color=TEXT_SEC, fontfamily=MONO)

        # title
        today     = datetime.now().strftime("%b %d, %Y  %H:%M ET")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%b %d")
        ax.set_title(
            f"S&P 500 Sector Performance  "
            f"Green:{(df['change_pct'] >= 0).sum()}, Red:{(df['change_pct'] < 0).sum()}",
            loc='left', pad=14, fontsize=12, fontweight='normal', color=TEXT_PRI)

        fig.text(0.99, 0.008, "Source: Yahoo Finance",
                ha='right', va='bottom', fontsize=7, color=TEXT_SEC, style='italic')

        plt.tight_layout(rect=[0, 0.01, 0.93, 1])
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=160, bbox_inches='tight',
                    facecolor=BG, edgecolor='none')
        buf.seek(0)
        plt.close(fig)
        return buf   # FIX: was incorrectly returning out_path (a str) instead of the BytesIO buffer

    def processrequest(self):
        sectorperf = SectorPerformance()
        df  = sectorperf.fetch_sector_data()
        out = sectorperf.plot_sector_chart(df, out_path="sector_performance.png")

        return df, out

# ── main ──────────────────────────────────────────────────────────────────────

# def main():
#     print("=" * 55)
#     print("  S&P 500 Sector ETF Performance")
#     print("=" * 55)
#     print("\nFetching data …\n")
#     sectorperf = SectorPerformance()
#     df  = sectorperf.fetch_sector_data()
#     out = sectorperf.plot_sector_chart(df, out_path="sector_performance.png")
#     return df , out


# if __name__ == "__main__":
#     df, chart_path = main()
    
