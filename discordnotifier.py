"""
discordnotifier.py
==================
Sends the S&P 500 sector-performance chart (and an optional text summary)
to a Discord channel via a Webhook URL.

Usage (standalone):
    python discordnotifier.py

Usage (from main.py / sectorperformance.py):
    from discordnotifier import DiscordNotifier

    notifier = DiscordNotifier()
    buf      = sectorperf.plot_sector_chart(df)   # returns BytesIO
    notifier.send(image_buffer=buf, df=df)
"""

import io
import requests
import pandas as pd
from datetime import datetime
import os

class DiscordNotifier:
    """
    Sends sector-performance data to a Discord channel via Webhook.

    Parameters
    ----------
    webhook_url : str
        Discord Webhook URL.  Defaults to the module-level placeholder.
    """

    def __init__(self):
        webhook_url = os.getenv("DISCORD_MKTINSIGHT_URL")
        if not webhook_url or "YOUR_WEBHOOK" in webhook_url:
            raise ValueError(
                "Set DISCORD_WEBHOOK_URL to a real Discord webhook before running.\n"
                "Create one at: Discord Server → Settings → Integrations → Webhooks"
            )
        self.webhook_url = webhook_url

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(
        self,
        image_buffer: io.BytesIO,
        df: pd.DataFrame | None = None,
        filename: str = "sector_performance.png",
    ) -> bool:
        """
        Post the chart image (and an optional embed summary) to Discord.

        Parameters
        ----------
        image_buffer : io.BytesIO
            In-memory PNG returned by SectorPerformance.plot_sector_chart().
        df           : pd.DataFrame, optional
            The sector DataFrame (columns: symbol, sector, change_pct,
            curr_price).  When provided a rich embed is included.
        filename     : str
            Filename shown in Discord for the attachment.

        Returns
        -------
        bool  True on success, False on any HTTP error.
        """
        payload  = self._build_payload(df)
        image_buffer.seek(0)

        files = {
            # 'files[0]' is Discord's multipart field name for the first attachment
            "files[0]": (filename, image_buffer, "image/png"),
        }

        try:
            resp = requests.post(
                self.webhook_url,
                data=payload,
                files=files,
                timeout=15,
            )
            resp.raise_for_status()
            print(f"[Discord] ✓ Chart sent successfully  (HTTP {resp.status_code})")
            return True

        except requests.exceptions.HTTPError as exc:
            print(f"[Discord] ✗ HTTP error – {exc}")
            print(f"[Discord]   Response body: {exc.response.text[:300]}")
            return False

        except requests.exceptions.ConnectionError:
            print("[Discord] ✗ Connection error – check your internet connection.")
            return False

        except requests.exceptions.Timeout:
            print("[Discord] ✗ Request timed out.")
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_payload(self, df: pd.DataFrame | None) -> dict:
        """
        Build the multipart form payload.

        Discord's multipart webhook format:
          - 'payload_json'  → JSON string with embeds / content
          - 'files[N]'      → binary attachment(s)

        The image is referenced inside the embed via attachment://filename.
        """
        import json

        now       = datetime.now().strftime("%b %d, %Y  %H:%M ET")
        n_green   = int((df["change_pct"] >= 0).sum()) if df is not None else 0
        n_red     = int((df["change_pct"] <  0).sum()) if df is not None else 0

        embed = {
            "title":       "📊 S&P 500 Sector Performance",
            #"description": self._build_description(df),
            "color":       0x3fb950 if n_green >= n_red else 0xf85149,
            "footer":      {"text": f"Source: Yahoo Finance  ·  {now}"},
            "image":       {"url": "attachment://sector_performance.png"},
            "fields":      self._build_fields(n_green, n_red),
        }

        payload_json = json.dumps({"embeds": [embed]})
        return {"payload_json": payload_json}

    def _build_description(self, df: pd.DataFrame | None) -> str:
        """Compact text table of all sectors for the embed description."""
        if df is None or df.empty:
            return ""

        lines = ["```"]
        lines.append(f"{'SYM':<5} {'SECTOR':<14} {'PREV':>7} {'CURR':>7} {'CHG%':>7}")
        lines.append("─" * 44)

        for row in df.itertuples():
            arrow = "▲" if row.change_pct >= 0 else "▼"
            sign  = "+" if row.change_pct >= 0 else ""
            lines.append(
                f"{row.symbol:<5} {row.sector:<14} "
                f"${row.prev_close:>6.2f} ${row.curr_price:>6.2f} "
                f"{arrow}{sign}{row.change_pct:.2f}%"
            )

        lines.append("```")
        return "\n".join(lines)

    @staticmethod
    def _build_fields(n_green: int, n_red: int) -> list[dict]:
        """Summary field chips shown below the embed image."""
        return [
            {"name": "🟢 Gaining",  "value": str(n_green), "inline": True},
            {"name": "🔴 Declining","value": str(n_red),   "inline": True},
            {"name": "📈 Breadth",
             "value": f"{n_green}/{n_green + n_red}",       "inline": True},
        ]


# ---------------------------------------------------------------------------
# Integration helper used by main.py / sectorperformance.py
# ---------------------------------------------------------------------------

def send_sector_performance(
    image_buffer: io.BytesIO,
    df: pd.DataFrame | None = None) -> bool:
    """
    One-liner convenience wrapper.

    Example in main.py:
        from discordnotifier import send_sector_performance

        buf = sectorperf.plot_sector_chart(df)
        send_sector_performance(buf, df=df)
    """
    notifier = DiscordNotifier()
    return notifier.send(image_buffer=image_buffer, df=df)


# ---------------------------------------------------------------------------
# Standalone smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")

    # Build a representative DataFrame (no live fetch needed for testing)
    sample = [
        {"symbol": "XLC",  "sector": "Comm Svcs",    "prev_close":  82.15, "curr_price":  83.67, "change":  1.52, "change_pct":  1.85},
        {"symbol": "XLF",  "sector": "Financials",   "prev_close":  44.78, "curr_price":  45.52, "change":  0.74, "change_pct":  1.65},
        {"symbol": "XLK",  "sector": "Technology",   "prev_close": 218.90, "curr_price": 222.45, "change":  3.55, "change_pct":  1.62},
        {"symbol": "XLI",  "sector": "Industrials",  "prev_close": 120.64, "curr_price": 121.88, "change":  1.24, "change_pct":  1.03},
        {"symbol": "XLB",  "sector": "Materials",    "prev_close":  87.42, "curr_price":  88.31, "change":  0.89, "change_pct":  1.02},
        {"symbol": "XLV",  "sector": "Health Care",  "prev_close": 144.60, "curr_price": 145.83, "change":  1.23, "change_pct":  0.85},
        {"symbol": "XLP",  "sector": "Cons Staples", "prev_close":  79.55, "curr_price":  79.38, "change": -0.17, "change_pct": -0.21},
        {"symbol": "XLU",  "sector": "Utilities",    "prev_close":  71.80, "curr_price":  71.44, "change": -0.36, "change_pct": -0.50},
        {"symbol": "XLRE", "sector": "Real Estate",  "prev_close":  41.22, "curr_price":  40.87, "change": -0.35, "change_pct": -0.85},
        {"symbol": "XLE",  "sector": "Energy",       "prev_close":  91.33, "curr_price":  90.18, "change": -1.15, "change_pct": -1.26},
        {"symbol": "XLY",  "sector": "Cons Discret", "prev_close": 193.20, "curr_price": 190.55, "change": -2.65, "change_pct": -1.37},
    ]
    df = pd.DataFrame(sample).sort_values("change_pct", ascending=False).reset_index(drop=True)

    # Generate the chart using SectorPerformance
    try:
        from sectorperformance import SectorPerformance
        sp   = SectorPerformance()
        buf  = sp.plot_sector_chart(df)
    except ImportError:
        # Fallback: create a minimal PNG in-memory
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "Sector Chart Placeholder", ha="center")
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        plt.close(fig)

    # Send to Discord
    try:
        notifier = DiscordNotifier()   # reads DISCORD_WEBHOOK_URL at top of file
        success  = notifier.send(image_buffer=buf, df=df)
        sys.exit(0 if success else 1)
    except ValueError as e:
        print(f"\n[Discord] Configuration needed:\n  {e}")
        sys.exit(1)
