import gc
from io import BytesIO
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from flask import Blueprint, request,render_template
from dataManager import ServiceManager
from alertManager import AlertManager

# ---------------------------------------------------------------------------
# Blueprint — register in main.py with: app.register_blueprint(day_trend_alert_bp)
# ---------------------------------------------------------------------------
day_trend_alert_bp = Blueprint('day_trend_alert', __name__)

_objMgr = ServiceManager()
_altMgr = AlertManager()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Mirrors the intervals checked in prepare_crsovr_message
ALERT_INTERVALS = ['15m', '30m', '1h', '4h']

# Only columns relevant to a crossover signal — matches what alertManager uses
DISPLAY_COLS  = ['hour', 'minute', 'interval', 'crossover', 'close', 'macd', 'msignal', 'histogram']
COL_HEADERS   = ['Time', 'Interval', 'Close', 'MACD/signal/histogram']

DPI            = 150         # image resolution sent to Telegram
BULLISH_COLOR  = '#C8E6C9'   # light green
BEARISH_COLOR  = '#ffcdd2'   # light red
NEUTRAL_COLOR  = '#ffffff'
HEADER_COLOR   = '#343a40'   # dark header background
HEADER_FONT_CL = 'white'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_signal_rows(df):
    """
    Mirrors prepare_crsovr_message logic:
      - keep only 15m and 30m intervals
      - take the last 1 row per interval
      - keep only Bullish / Bearish crossover rows
    Returns a trimmed DataFrame (may be empty if no signals).
    """
    frames = []
    for interval in ALERT_INTERVALS:
        subset = df[df['interval'] == interval]
        if subset.empty:
            continue
        last_row = subset.tail(1)
        frames.append(last_row)
    frames.reverse()

    if not frames:
        return df.__class__()   # empty DataFrame

    import pandas as pd
    return pd.concat(frames, ignore_index=True)

def _build_image(symbol: str, df) -> BytesIO:
    """
    Filter to signal rows (mirrors prepare_crsovr_message), then render
    as a compact styled matplotlib table and return a PNG buffer.
    """
    print(df)
    signal_df = _filter_signal_rows(df)
    if signal_df.empty:
        return None

    display_df = signal_df.reset_index(drop=True)
    n_rows = len(display_df)
    n_cols = len(COL_HEADERS)

    # Explicit column width fractions — wide last col for MACD triple value
    COL_WIDTHS    = [0.10, 0.18, 0.10, 0.32]
    # Very small fixed cell heights in axes-fraction units
    HEADER_CELL_H = 0.040
    DATA_CELL_H   = 0.034

    # Figure dimensions: tight height per row, narrow fixed width
    FIG_W = 4.8
    FIG_H = 0.14 * (n_rows + 1) + 0.18   # 0.14in/row + 0.18in title space

    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    ax.axis('off')
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_title(
        f"{symbol} — Crossover Signals",
        fontsize=6, fontweight='bold', color='#212529', pad=2
    )

    cell_text = []
    for _, row in display_df.iterrows():
        trendval = ""
        if row['crossover'] == "3":
            trendval = "-strong bullish"
        elif row['crossover'] == "2":
            trendval = "-moderate bullish"
        elif row['crossover'] == "1":
            trendval = "-weak bullish"
        elif row['crossover'] == "-1":
            trendval = "-weak bearish"
        elif row['crossover'] == "-2":
            trendval = "-moderate bearish"
        elif row['crossover'] == "-3":
            trendval = "-strong bearish"
        cell_text.append([
            f"{str(row['hour'])}:{str(row['minute'])}",
            f"{str(row['interval'])}{trendval}",
            f"{float(row['close']):.2f}",
            f"{float(row['macd']):.2f}/{float(row['msignal']):.2f}/{float(row['histogram']):.2f}",
        ])

    table = ax.table(
        cellText=cell_text,
        colLabels=COL_HEADERS,
        colWidths=COL_WIDTHS,
        cellLoc='center',
        loc='center',
        bbox=[0, 0, 1, 1]        # lock table to fill axes exactly — no padding
    )
    table.auto_set_font_size(False)
    table.set_fontsize(5.5)

    # ---- header row ----
    for col_idx in range(n_cols):
        cell = table[0, col_idx]
        cell.set_facecolor(HEADER_COLOR)
        cell.set_text_props(color=HEADER_FONT_CL, fontweight='bold')
        cell.set_height(HEADER_CELL_H)

    # ---- data rows ----
    for row_idx in range(1, n_rows + 1):
        parts      = cell_text[row_idx - 1][3].split('/')
        hist_val   = float(parts[2].strip())
        cell_color = BULLISH_COLOR if hist_val > 0 else (BEARISH_COLOR if hist_val < 0 else NEUTRAL_COLOR)
        for col_idx in range(n_cols):
            cell = table[row_idx, col_idx]
            cell.set_height(DATA_CELL_H)
            cell.set_facecolor(cell_color if col_idx == 3 else NEUTRAL_COLOR)
            cell.set_text_props(color='#212529')

    plt.subplots_adjust(left=0, right=1, top=0.88, bottom=0)
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=DPI, bbox_inches='tight',
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@day_trend_alert_bp.route("/dayTrendAlert")
def day_trend_alert():
    """
    Fetch day-trend data for a symbol, filter to Bullish/Bearish crossover
    signals on 15m and 30m intervals (same logic as prepare_crsovr_message),
    render as a PNG table, and send to the configured Telegram channel.

    Query params:
        symbol  (str, default 'SPY')  — ticker symbol
    """
    symbol = request.args.get('symbol', default='SPY', type=str).upper()

    df = _objMgr.analyze_stockdata(symbol)

    if df is None or df.empty:
        return f"No data available for {symbol}.", 404

    buf = _build_image(symbol, df)

    del df
    gc.collect()

    if buf is None:
        return f"No Bullish/Bearish crossover signals found for {symbol} on 15m/30m.", 200

    _altMgr.send_photo_alert(buf, filename=f"{symbol}_daytrend.png", set_title="Trend alert")
    chart_image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
    buf.close()

    #return f"Day trend signal image for {symbol} sent to Telegram."
    return render_template('./sectorperformance.html', page_title="Trend alert", chart_image=chart_image_base64)
