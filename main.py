import os
import base64
import gc
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from flask import Flask, render_template, request, render_template_string

from dataManager import ServiceManager
from alertManager import AlertManager
from supresrange import SupportResistanceByInputInterval
from csPattern import csPattern

app = Flask(__name__)

# Use a single shared instance; do NOT store DataFrames as globals
g_message = []
objMgr = ServiceManager()
altMgr = AlertManager()


def process_stocksignal(symbol="SPY"):
    """Fetch and analyze stock data, prepare alert messages. Returns DataFrame."""
    global g_message

    df = objMgr.analyze_stockdata(symbol)
    altMgr.prepare_crsovr_message(df)
    g_message = altMgr.get_message()
    return df


@app.route("/csPattern")
def CandleStickPattern():
    stocksymbols = request.args.get('symbol', type=str).upper()
    if  stocksymbols:
            stocksymbols = stocksymbols.split(",")
    else:
        stocksymbols = os.getenv("CUSTOM_ALERT_SYMBOL")
        stocksymbols = stocksymbols.split(",") if stocksymbols else ['SPY']

    allsymbols_data = []

    for symbol in stocksymbols:
        dbrecval = altMgr.GetStockOrderRecordfromDB(symbol, 'Open')
        cs = csPattern()
        if dbrecval is not None:
            cs.openorderon5m = dbrecval

        cs.analyze_stockcandlesLTF(symbol)

        open_order  = cs.openorderon5m
        close_order = cs.closeorderon5m

        if open_order is not None and close_order is None:
            existing = altMgr.GetStockOrderRecordusingUnixTime(
                symbol,
                str(open_order['unixtime']),
                str(open_order['hour']),
                str(open_order['minute'])
            )
            if existing is None:
                altMgr.AddOpenStockOrderRecordtoDB(open_order)
                if open_order['updatedTriggerTime'] == open_order['unixtime']:
                    allsymbols_data.append(
                        f"Symbol: {open_order['symbol']} "
                        f"Time: {open_order['hour']}:{open_order['minute']} "
                        f"Pattern: {open_order['cspattern']}, "
                        f"open price: {open_order['stockprice']}, "
                        f"stoploss: {open_order['stoploss']}, "
                        f"profittarget: {open_order['profittarget']}"
                    )

        if close_order is not None:
            altMgr.AddOpenStockOrderRecordtoDB(open_order, "OpenClose")
            allsymbols_data.append(
                f"Symbol: {close_order['symbol']} "
                f"Time: {close_order['hour']}:{close_order['minute']} "
                f"Pattern: {close_order['cspattern']}, "
                f"close price: {close_order['stockprice']}"
            )
            altMgr.openorderon5m  = None
            altMgr.closeorderon5m = None

        # Release per-symbol object immediately
        del cs, open_order, close_order, dbrecval

    resultdata = ",".join(allsymbols_data)
    if allsymbols_data:
        altMgr.send_chart_alert(resultdata)

    del allsymbols_data
    gc.collect()
    return resultdata if resultdata else "done!"


@app.route("/marketPattern")
def marketPattern():
    stocksymbols = ['NQ%3DF', 'RTY%3DF', 'GC%3DF']
    allsymbols_data = []

    for symbol in stocksymbols:
        cs = csPattern()
        ret = cs.analyze_stockcandlesHTF(symbol)
        if ret is not None:
            allsymbols_data.append(ret)
        del cs  # free per-symbol object immediately

    sentmsg = "done!"
    if allsymbols_data:
        resultdata = ", ".join(str(item) for item in allsymbols_data)
        sentmsg = altMgr.send_chart_alert(resultdata)
        sentmsg = resultdata

    del allsymbols_data
    gc.collect()
    return sentmsg


@app.route("/scalpPattern")
def ScalpPattern():
    symbol   = request.args.get('symbol', default='SPY', type=str).upper()
    interval = request.args.get('interval', default='15m', type=str)

    scalper = SupportResistanceByInputInterval(symbol, interval, days_back=2)
    summary = scalper.get_scalping_summary()

    if not summary:
        del scalper
        return "<h1>Error: Could not generate analysis.</h1>", 500

    image_buffer = scalper.plot_15min_chart(bars_to_show=96)
    chart_image_base64 = base64.b64encode(image_buffer.getvalue()).decode('utf-8')

    # Free the buffer and scalper object immediately after encoding
    image_buffer.close()
    del scalper, image_buffer

    return render_template('./scalp.html', summary=summary, chart_image=chart_image_base64)


@app.route("/bkOutInvoke")
def BkOutInvoke():
    return render_template('./second.html')


@app.route('/rangePattern')
def RangePattern():
    dt = datetime.now()
    fmt = '%Y-%m-%d %H:%M:%S %z'
    pmst_dt  = datetime.strptime(f"{dt.date()} 4:00:00 -0400",  fmt)
    regst_dt = datetime.strptime(f"{dt.date()} 9:30:00 -0400",  fmt)
    reget_dt = datetime.strptime(f"{dt.date()} 10:00:00 -0400", fmt)

    # Cap end period to now if market hasn't opened yet
    if int(datetime.now().timestamp()) < reget_dt.timestamp():
        reget_dt = datetime.now().astimezone(reget_dt.tzinfo)

    stocksymbols = ['SPY']
    allsymbols_data = []
    pm_data = rg_data = ""

    for ss in stocksymbols:
        df = objMgr.download_stock_data(
            ss,
            startPeriod=pmst_dt.timestamp(),
            endPeriod=reget_dt.timestamp(),
            interval="15m"
        )

        # Pre-market slice
        df_pm = df[df['unixtime'] <= regst_dt.timestamp() - 1]
        if not df_pm.empty:
            pm_data = (
                f"O:{df_pm['open'].iloc[0]}, "
                f"C:{df_pm['close'].iloc[-1]}, "
                f"L:{df_pm['low'].min()}, "
                f"H:{df_pm['high'].max()}"
            )
        del df_pm

        # Regular session slice
        df_rg = df[df['unixtime'] >= regst_dt.timestamp() - 1]
        if not df_rg.empty:
            rg_data = (
                f"O:{df_rg['open'].iloc[0]}, "
                f"C:{df_rg['close'].iloc[-1]}, "
                f"L:{df_rg['low'].min()}, "
                f"H:{df_rg['high'].max()}"
            )
        del df_rg, df  # free the full frame right away

        allsymbols_data.append(
            f'{{ "symbol": "{ss}", "pmdata": "{{{pm_data}}}", "rgdata": "{{{rg_data}}}" }}'
        )

    resultdata = ",".join(allsymbols_data)
    del allsymbols_data

    if pm_data or rg_data:
        altMgr.send_chart_alert(resultdata)

    altMgr.DelOldRecordsFromDB()
    gc.collect()
    return resultdata


@app.route("/inputsym")
def inputsym():
    global g_message
    g_message = []
    return render_template('./inputsym.html')


@app.route("/dayTrend")
def dayTrend():
    global g_message
    g_message = []
    symbol = request.args.get('symbol', default='', type=str).upper()
    return render_template('./dayTrend.html', symbol=symbol)


@app.route('/returnPattern')
def ReturnPattern():
    global g_message

    g_message = []
    altMgr.set_message(g_message)

    symbol = request.args.get('symbol', default='', type=str).upper()
    stocksymbols = [symbol] if symbol else ['GLD', 'QQQ', 'IWM']

    # Collect slices, then concat once — avoids growing a frame in a loop
    frames = []
    for ss in stocksymbols:
        df_stock = process_stocksignal(ss)
        frames.append(df_stock)
        del df_stock  # drop reference; concat below will handle memory

    df_allsymbols = pd.concat(frames, ignore_index=False) if frames else pd.DataFrame()
    del frames
    gc.collect()

    if g_message:
        sentmsg = altMgr.send_chart_alert(g_message)
        print(sentmsg)

    result = df_allsymbols.to_json(orient='records', index=False)
    del df_allsymbols
    return result


@app.route("/")
def default():
    return render_template('./default.html')


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=80)
