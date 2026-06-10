"""
stockAnalysis.py
================
Flask Blueprint — mounts at /stockAnalysis

Features:
  1. Fetches 15-minute OHLCV data for the last 3 working days (incl. pre/post market)
     via Yahoo Finance v8 (same endpoint as dataManager.py).
  2. Derives key price levels from yesterday's session:
       • previous day close  (last regular-hours bar ≤ 16:00 ET)
       • pre-market low / high (bars strictly before 09:30 ET on current trading day)
  3. Calculates open-range low / high (09:30 – 10:00 ET today).
  4. Computes MACD and RSI using ServiceManager helpers (same logic as the rest of the app).
  5. Renders a Plotly candlestick chart (today only, 15 m bars) with horizontal level lines.
  6. Returns an HTML page that embeds the interactive chart + a compact data table.

Register in your main Flask app:
    from stockAnalysis import stock_analysis_bp
    app.register_blueprint(stock_analysis_bp)

Query params:
    symbol  (str, default 'SPY') — ticker symbol
"""

import gc
import time
import base64
import warnings
import io

import numpy  as np
import pandas as pd
import requests
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.trend import MACD
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from alertManager import AlertManager

warnings.filterwarnings('ignore')

from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from flask import Blueprint, request, render_template_string

from dataManager import ServiceManager

# ---------------------------------------------------------------------------
# Blueprint
# ---------------------------------------------------------------------------
stock_analysis_bp = Blueprint('stock_analysis', __name__)

_objMgr = ServiceManager()

ET = ZoneInfo('America/New_York')

# ---------------------------------------------------------------------------
# Helper — raw 15 m fetch with pre/post market
# ---------------------------------------------------------------------------

def _fetch_15m_raw(symbol: str, days: int = 5) -> pd.DataFrame:
    """
    Downloads 15-minute bars (including pre/post market) for the last `days`
    calendar days from Yahoo Finance v8.  Returns a DataFrame with a
    tz-aware (America/New_York) DatetimeIndex and columns:
        unixtime, open, high, low, close, rec_dt, hour, minute
    """
    end_ts   = int(datetime.now(timezone.utc).timestamp())
    start_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

    url    = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        'period1':        start_ts,
        'period2':        end_ts,
        'interval':       '15m',
        'includePrePost': 'true',
    }
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    resp = requests.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data   = resp.json()
    result = data['chart']['result'][0]
    quotes = result['indicators']['quote'][0]

    ts_arr = np.asarray(result['timestamp'], dtype='int64')
    df = pd.DataFrame({
        'unixtime': ts_arr,
        'open':  np.round(np.asarray(quotes['open'],  dtype='float64'), 2),
        'high':  np.round(np.asarray(quotes['high'],  dtype='float64'), 2),
        'low':   np.round(np.asarray(quotes['low'],   dtype='float64'), 2),
        'close': np.round(np.asarray(quotes['close'], dtype='float64'), 2),
    })
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Build tz-aware index in ET
    ts_et = (
        pd.to_datetime(df['unixtime'], unit='s')
        .dt.tz_localize('UTC')
        .dt.tz_convert('America/New_York')
    )
    df.index      = ts_et
    df.index.name = 'timestamp'

    # Derive date/time columns from the DatetimeIndex after it is set.
    # Using df.index (not the detached ts_et Series) guarantees alignment
    # and always produces proper Python datetime.date objects — never floats.
    df['rec_dt'] = pd.Series(df.index.date, index=df.index)
    df['hour']   = df.index.hour
    df['minute'] = df.index.minute

    # Belt-and-suspenders: coerce any stray non-date values to date
    df['rec_dt'] = df['rec_dt'].apply(
        lambda x: x if isinstance(x, date) else pd.Timestamp(x).date()
    )

    return df


# ---------------------------------------------------------------------------
# Helper — derive price levels
# ---------------------------------------------------------------------------

def _last_n_trading_days(df: pd.DataFrame, n: int = 3) -> list:
    """Return the last n unique trading dates present in df (sorted asc).
    Always returns proper datetime.date objects regardless of how rec_dt is stored.
    """
    raw = df['rec_dt'].dropna().unique()
    dates = sorted(pd.Timestamp(d).date() for d in raw)
    return dates[-n:] if len(dates) >= n else dates


def _derive_levels(df: pd.DataFrame, today: date, yesterday: date):
    """
    Returns a dict with price levels derived from the raw 15-m frame.

    prev_close      — last bar's close whose timestamp is between 09:29 and 16:00 ET on *yesterday*
    premarket_low   — min low of bars on *today* strictly before 09:30 ET
    premarket_high  — max high of bars on *today* strictly before 09:30 ET
    open_range_low  — min low of bars on *today* between 09:30 and 10:00 ET (inclusive start)
    open_range_high — max high of bars on *today* between 09:30 and 10:00 ET
    """
    levels = {
        'prev_close':      None,
        'premarket_low':   None,
        'premarket_high':  None,
        'open_range_low':  None,
        'open_range_high': None,
    }

    # ---- previous day close (last regular-session bar ≤ 16:00) ----
    yest_mask = (df['rec_dt'] == yesterday) & \
                (df['hour'] >= 9) & \
                ~((df['hour'] == 9) & (df['minute'] < 30)) & \
                (df['hour'] < 16)
    yest_reg = df[yest_mask]
    if not yest_reg.empty:
        levels['prev_close'] = round(float(yest_reg['close'].iloc[-1]), 2)

    # ---- today pre-market (before 09:30) ----
    pm_mask = (df['rec_dt'] == today) & \
              ((df['hour'] < 9) | ((df['hour'] == 9) & (df['minute'] < 30)))
    pm_df = df[pm_mask]
    if not pm_df.empty:
        levels['premarket_low']  = round(float(pm_df['low'].min()),  2)
        levels['premarket_high'] = round(float(pm_df['high'].max()), 2)

    # ---- open range 09:30 – 10:00 ----
    # 09:30 bar → hour=9, minute=30
    # 09:45 bar → hour=9, minute=45
    # 10:00 bar → hour=10, minute=0
    or_mask = (df['rec_dt'] == today) & (
        ((df['hour'] == 9)  & (df['minute'] >= 30)) |
        ((df['hour'] == 10) & (df['minute'] == 0))
    )
    or_df = df[or_mask]
    if not or_df.empty:
        levels['open_range_low']  = round(float(or_df['low'].min()),  2)
        levels['open_range_high'] = round(float(or_df['high'].max()), 2)

    return levels


# ---------------------------------------------------------------------------
# Helper — compute MACD + RSI on the 15-m slice
# ---------------------------------------------------------------------------

def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add macd, msignal, histogram, rsi columns in-place (reuses ServiceManager statics)."""
    
    macd_indicator = MACD(close=df['close'], window_slow=26, window_fast=12, window_sign=9)
    df['macd'] = macd_indicator.macd().round(2).astype('float32')           
    df['msignal'] = macd_indicator.macd_signal().round(2).astype('float32') 
    df['histogram'] = macd_indicator.macd_diff().round(2).astype('float32')

    rsi_indicator = RSIIndicator(close=df['close'], window=14)
    df['rsi'] = rsi_indicator.rsi().round(2).astype('float32')
    rsignal = df['rsi'].astype('float64').ewm(span=14).mean().round(2).astype('float32')
    df['rsignal']   = rsignal
    df = _objMgr.calculate_TrendAlert(df)
    df = _objMgr.calculate_RSITrendAlert(df)
    del rsi_indicator, rsignal, macd_indicator
    return df


# ---------------------------------------------------------------------------
# Helper — build chart image (matplotlib, server-side PNG)
# ---------------------------------------------------------------------------

def _build_chart(today_df: pd.DataFrame, levels: dict, symbol: str) :
    """
    Renders a candlestick chart (today's 15-m bars) with:
      • Prev-day close (dashed white)
      • Pre-market low/high (dashed orange)
      • Open-range low/high (dashed cyan)
    Returns a base64-encoded PNG string for embedding in HTML.
    """
    BG      = "#0d1117"
    CARD_BG = "#161b22"
    GREEN   = "#3fb950"
    RED     = "#f85149"
    TEXT    = "#e6edf3"
    GRID    = "#21262d"
    MONO    = "DejaVu Sans Mono"
    WHITE   = "#fff"
    GRAY    = "#8b949e"


    df = today_df.copy().reset_index()
    n  = len(df)
    if n == 0:
        return None

    # .tail(1) returns a DataFrame; use .iloc[0] to get a scalar for comparisons
    last_crossover = str(df['crossover'].iloc[-1]) if 'crossover' in df.columns else ""
    macdtrendval = {
        "3":  "-strong bullish",
        "2":  "-moderate bullish",
        "1":  "-weak bullish",
        "-1": "-weak bearish",
        "-2": "-moderate bearish",
        "-3": "-strong bearish",
    }.get(last_crossover, "neutral")
    rsistrendval = df['rsicrossover'].iloc[-1]
    xs = np.arange(n)

    fig, (ax_candle, ax_macd, ax_rsi) = plt.subplots(
        3, 1, figsize=(14, 9),
        gridspec_kw={'height_ratios': [3, 1, 1]},
        facecolor=BG
    )
    for ax in (ax_candle, ax_macd, ax_rsi):
        ax.set_facecolor(CARD_BG)
        ax.tick_params(colors=TEXT, labelsize=8)
        ax.xaxis.grid(True, color=GRID, lw=0.6, ls='--', alpha=0.7)
        ax.yaxis.grid(True, color=GRID, lw=0.6, ls='--', alpha=0.7)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID)

    # ---- Candlestick bars ----
    bar_w = 0.6
    for i, row in df.iterrows():
        o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
        color = GREEN if c >= o else RED
        # body
        ax_candle.bar(xs[i], abs(c - o), bottom=min(o, c), width=bar_w,
                      color=color, linewidth=0, zorder=3)
        # wick
        ax_candle.plot([xs[i], xs[i]], [l, h], color=color, lw=0.9, zorder=2)

    # ---- Horizontal level lines ----
    level_styles = {
        'prev_close':      ('white',   '--', 1.4, 'Prev Close'),
        'premarket_low':   ('#FFA726', '--', 1.2, 'PM Low'),
        'premarket_high':  ('#FFA726', '--', 1.2, 'PM High'),
        'open_range_low':  ('#29B6F6', ':',  1.2, 'OR Low'),
        'open_range_high': ('#29B6F6', ':',  1.2, 'OR High'),
    }
    legend_handles = []
    for key, (color, ls, lw, label) in level_styles.items():
        val = levels.get(key)
        if val is not None:
            ax_candle.axhline(val, color=color, ls=ls, lw=lw, zorder=4, alpha=0.85)
            ax_candle.text(n - 0.5, val, f' {val:.2f}',
                           color=color, fontsize=7, va='center',
                           fontfamily=MONO, zorder=5)
            legend_handles.append(
                Line2D([0], [0], color=color, ls=ls, lw=lw, label=label)
            )

    ax_candle.set_title(f"{symbol} — 15m Candlestick (MACD trend: {macdtrendval}, RSI trend: {rsistrendval})",
                        color=TEXT, fontsize=11, fontweight='bold', loc='left', pad=10)
    ax_candle.set_ylabel('Price', color=TEXT, fontsize=9)
    ax_candle.set_xlim(-0.8, n - 0.2)
    if legend_handles:
        ax_candle.legend(handles=legend_handles, loc='upper left',
                         fontsize=7, framealpha=0.3,
                         labelcolor=TEXT, facecolor=CARD_BG, edgecolor=GRID)

    # ---- x-tick labels (time) ----
    step  = max(1, n // 10)
    ticks = xs[::step]
    labels = [
        df.iloc[i]['timestamp'].strftime('%H:%M') if hasattr(df.iloc[i]['timestamp'], 'strftime')
        else str(df.iloc[i].get('hour', '')) + ':' + str(df.iloc[i].get('minute', '')).zfill(2)
        for i in ticks
    ]
    for ax in (ax_candle, ax_macd, ax_rsi):
        ax.set_xticks(ticks)
    ax_candle.set_xticklabels([])
    ax_macd.set_xticklabels([])
    ax_rsi.set_xticklabels(labels, rotation=45, ha='right', color=TEXT, fontsize=7)

    # ---- MACD subplot ----
    if 'macd' in df.columns and 'msignal' in df.columns and 'histogram' in df.columns:
        hist = df['histogram'].astype(float)
        bar_colors = [GREEN if v >= 0 else RED for v in hist]
        ax_macd.bar(xs, hist, color=bar_colors, width=0.6, alpha=0.8, zorder=3)
        ax_macd.plot(xs, df['macd'].astype(float),    color='#E040FB', lw=1.2, label='MACD',   zorder=4)
        ax_macd.plot(xs, df['msignal'].astype(float), color='#FFC107', lw=1.0, label='Signal', zorder=4)
        ax_macd.axhline(0, color=GRID, lw=0.8)
        ax_macd.set_ylabel('MACD', color=TEXT, fontsize=8)
        ax_macd.legend(fontsize=7, labelcolor=TEXT, facecolor=CARD_BG, edgecolor=GRID, framealpha=0.4)

    # ---- RSI subplot ----
    if 'rsi' in df.columns:
        rsi_vals = df['rsi'].astype(float)
        ax_rsi.plot(xs, rsi_vals, color='#29B6F6', lw=1.2, zorder=4)
        ax_rsi.plot(xs, df['rsignal'].astype(float), color='#FFC107', lw=1.0, label='Signal', zorder=4)
        ax_rsi.axhline(70, color=RED,   lw=0.8, ls='--', alpha=0.7)
        ax_rsi.axhline(50, color=WHITE, lw=0.8, ls='--', alpha=0.7)
        ax_rsi.axhline(30, color=GREEN, lw=0.8, ls='--', alpha=0.7)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_ylabel('RSI', color=TEXT, fontsize=8)
        ax_rsi.fill_between(xs, rsi_vals, 70, where=(rsi_vals >= 70),
                             color=RED,   alpha=0.25, zorder=2)
        ax_rsi.fill_between(xs, rsi_vals, 30, where=(rsi_vals <= 30),
                             color=GREEN, alpha=0.25, zorder=2)

    plt.tight_layout(rect=[0, 0, 1, 1])

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=BG, edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Inline HTML template
# ---------------------------------------------------------------------------

_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{{ symbol }} — Stock Analysis</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;500;700&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:        #0d1117;
    --card:      #161b22;
    --border:    #30363d;
    --text:      #e6edf3;
    --muted:     #8b949e;
    --green:     #3fb950;
    --red:       #f85149;
    --orange:    #FFA726;
    --cyan:      #29B6F6;
    --purple:    #E040FB;
    --yellow:    #FFC107;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 1.5rem;
  }

  .chart-wrap {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 1.5rem;
    overflow-x: auto;
  }
  .chart-wrap img { max-width: 100%; display: block; }

  .levels-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.5rem;
  }

  .level-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.9rem 1rem;
    position: relative;
    overflow: hidden;
  }
  .level-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    border-radius: 8px 0 0 8px;
  }
  .level-card.white::before  { background: #fff; }
  .level-card.orange::before { background: var(--orange); }
  .level-card.cyan::before   { background: var(--cyan); }

  .level-card .label {
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 4px;
  }
  .level-card .value {
    font-family: var(--mono);
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--text);
  }
  .level-card .na { color: var(--muted); font-size: 0.85rem; }

  .section-title {
    font-family: var(--mono);
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--muted);
    margin-bottom: 0.75rem;
  }

  .error-box {
    background: rgba(248,81,73,0.12);
    border: 1px solid var(--red);
    border-radius: 8px;
    padding: 1rem 1.2rem;
    font-family: var(--mono);
    font-size: 0.85rem;
    color: var(--red);
  }

  .closed-banner {
    background: rgba(255,167,38,0.12);
    border: 1px solid var(--orange);
    border-radius: 8px;
    padding: 0.65rem 1rem;
    font-family: var(--mono);
    font-size: 0.8rem;
    color: var(--orange);
    margin-bottom: 1.25rem;
  }

</style>
</head>
<body>

{% if error %}
  <div class="error-box">⚠ {{ error }}</div>
{% else %}

<!-- ── Candlestick Chart ── -->
<div class="chart-wrap">
  {% if chart_b64 %}
    <img src="data:image/png;base64,{{ chart_b64 }}" alt="{{ symbol }} 15m chart"/>
  {% else %}
    <p style="color:var(--muted); font-family:var(--mono); font-size:0.85rem; padding:1rem;">
      No chart data available for today's session yet.
    </p>
  {% endif %}
</div>

{% endif %}

</body>
</html>
"""


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@stock_analysis_bp.route("/stockAnalysis")
def stock_analysis():
    symbol = request.args.get('symbol', default='SPY', type=str).upper()
    as_of  = datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')

    try:
        # 1. Fetch raw 15m data — 10 calendar days guarantees 3+ trading days
        #    even across long weekends and public holidays.
        raw_df = _fetch_15m_raw(symbol, days=10)

        # 2. Identify actual trading days present in the fetched data (sorted asc).
        trading_days = _last_n_trading_days(raw_df, n=3)
        if len(trading_days) < 1:
            return render_template_string(
                _TEMPLATE,
                symbol=symbol, as_of=as_of, error="No trading data returned by Yahoo Finance.",
                levels={}, chart_b64="", rows=[], market_closed=False
            )

        # 3. Weekend / holiday awareness:
        #    • On a weekday → "today" is today's date if data exists, otherwise last trading day.
        #    • On a weekend / after close with no new bars → fall back to the last two
        #      trading days we actually have data for.
        now_et      = datetime.now(ET)
        cal_today   = now_et.date()
        is_weekend  = cal_today.weekday() >= 5   # 5=Sat, 6=Sun

        last_trading_day  = trading_days[-1]
        market_closed     = is_weekend or (cal_today not in trading_days)

        # "today" for display purposes = last date we have data for
        today     = last_trading_day
        yesterday = trading_days[-2] if len(trading_days) >= 2 else last_trading_day

        # 4. Derive key price levels
        levels = _derive_levels(raw_df, today, yesterday)

        # 5. Compute indicators on the full 15m slice (enough history for MACD/RSI warmup)
        indicator_df = raw_df.copy()
        indicator_df = _compute_indicators(indicator_df)

        # 6. Last trading day's bars — for chart + table
        today_df = indicator_df[(indicator_df['rec_dt'] == today) & (indicator_df['hour'] >= 8)].copy()

        # 7. Build chart image
        image_buffer = _build_chart(today_df, levels, symbol)
        chart_b64 = ""
        if image_buffer is not None:
            altMgr = AlertManager()
            altMgr.send_photo_alert(image_buffer)
            image_buffer.seek(0)
            chart_b64 = base64.b64encode(image_buffer.getvalue()).decode('utf-8')
            image_buffer.close()
            del altMgr, image_buffer

        del raw_df, indicator_df, today_df
        gc.collect()

        return render_template_string(
            _TEMPLATE,
            symbol=symbol,
            as_of=as_of,
            error=None,
            levels=levels,
            chart_b64=chart_b64,
            market_closed=market_closed,
            session_date=pd.Timestamp(today).strftime('%A, %b %d %Y'),
        )

    except requests.exceptions.HTTPError as e:
        return render_template_string(
            _TEMPLATE, symbol=symbol, as_of=as_of,
            error=f"HTTP error fetching data: {e}",
            levels={}, chart_b64="", rows=[], market_closed=False, session_date=""
        ), 502

    except Exception as e:
        import traceback
        traceback.print_exc()
        return render_template_string(
            _TEMPLATE, symbol=symbol, as_of=as_of,
            error=f"Unexpected error: {e}",
            levels={}, chart_b64="", rows=[], market_closed=False, session_date=""
        ), 500
