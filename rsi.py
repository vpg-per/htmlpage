import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from time import strftime
from flask import Flask, json, render_template, request, session, render_template_string
from dataManager import ServiceManager
from alertManager import AlertManager
from supresrange import SupportResistanceByInputInterval
import base64

app = Flask(__name__)
g_message = []
objMgr = ServiceManager()
altMgr = AlertManager()

def process_stocksignal(symbol="SPY", interval="1d"):
    global objMgr
    global g_message
    global altMgr
    
    df = objMgr.analyze_stockdata(symbol)
    altMgr.prepare_crsovr_message(df)
    g_message = altMgr.get_message()    

    return df

@app.route("/scalpPattern")
def ScalpPattern():
    """Run analysis and display on a web page."""
    symbol = request.args.get('symbol', default='SPY', type=str).upper()
    interval = request.args.get('interval', default='15m', type=str)
    scalper = SupportResistanceByInputInterval(symbol, interval, days_back=2)
    summary = scalper.get_scalping_summary()

    if not summary:
        return "<h1>Error: Could not generate analysis.</h1>", 500

    # Generate chart image
    image_buffer = scalper.plot_15min_chart(bars_to_show=96)
    chart_image_base64 = base64.b64encode(image_buffer.getvalue()).decode('utf-8')

    #return render_template_string(HTML_TEMPLATE, summary=summary, chart_image=chart_image_base64)
    return render_template('./scalp.html', summary=summary, chart_image=chart_image_base64)

@app.route("/bkOutInvoke")
def BkOutInvoke():
    return render_template('./second.html')
    
@app.route('/rangePattern')
def RangePattern():
    dt = datetime.now()
    pmst_string = (f"{dt.date()} 4:00:00 -0400")
    pmst_dt = datetime.strptime(pmst_string, '%Y-%m-%d %H:%M:%S %z')
    regst_string = (f"{dt.date()} 9:30:00 -0400")
    regst_dt = datetime.strptime(regst_string, '%Y-%m-%d %H:%M:%S %z')
    reget_string = (f"{dt.date()} 10:00:00 -0400")
    reget_dt = datetime.strptime(reget_string, '%Y-%m-%d %H:%M:%S %z')

    c_reget = reget_dt.timestamp()
    if ( int(datetime.now().timestamp()) < c_reget):
        c_reget = int(datetime.now().timestamp())

    global objMgr
    global altMgr 
    stocksymbols = ['SPY']
    #stocksymbols = ['NQ%3DF', 'RTY%3DF', 'GC%3DF']
    allsymbols_data = []
    pm_data, rg_data = "", ""
    for ss in stocksymbols:  
        df = objMgr.download_stock_data(ss, startPeriod=pmst_dt.timestamp(), endPeriod=reget_dt.timestamp(), interval="15m")
        df_mng_stock = df[ (df['unixtime'] <= regst_dt.timestamp() - 1) ]
        if (df_mng_stock.shape[0] > 0):
            pm_open, pm_highest_score, pm_lowest_score = df_mng_stock['open'].iloc[0], df_mng_stock['high'].max(), df_mng_stock['low'].min()
            pm_close = df_mng_stock['close'].iloc[-1]
            pm_data = f"O:{pm_open}, C:{pm_close}, L:{pm_lowest_score}, H:{pm_highest_score}"
    
        df_rg_stock = df[ (df['unixtime'] >= regst_dt.timestamp() - 1) ]
        if (len(df_rg_stock) > 0):
            rg_open, rg_highest_score, rg_lowest_score = df_rg_stock['open'].iloc[0], df_rg_stock['high'].max(), df_rg_stock['low'].min()
            rg_close = df_rg_stock['close'].iloc[-1]
            rg_data = f"O:{rg_open}, C:{rg_close}, L:{rg_lowest_score}, H:{rg_highest_score}"
        allsymbols_data.append(f"{{ \"symbol\": \"{ss}\", \"pmdata\": \"{{{pm_data}}}\", \"rgdata\": \"{{{rg_data}}}\" }}")
    
    resultdata = ",".join(allsymbols_data)
    if(pm_data != "" or rg_data != ""):
        sentmsg = altMgr.send_chart_alert(resultdata)

    altMgr.DelOldRecordsFromDB()
    json_string = '{"result": "Processing is complete."}'
    return resultdata

@app.route("/")
def index():
#def main():
    global g_message
    g_message = []
    symbol = request.args.get('symbol', default='', type=str).upper()
    return render_template('./index.html', symbol=symbol)

@app.route('/returnPattern')
def ReturnPattern():
    global g_message
    global objMgr
    global altMgr

    g_message = []
    altMgr.set_message(g_message)
    symbol = request.args.get('symbol', default='', type=str).upper()
    stocksymbols = ['GLD', 'QQQ','IWM']
    if (symbol != ""):
        stocksymbols = [symbol]
    #stocksymbols = ['NQ%3DF', 'RTY%3DF', 'GC%3DF']
    df_allsymbols = {}
    for ss in stocksymbols:  
        df_stock = process_stocksignal(ss)

        df_len = len(df_allsymbols)
        if df_len == 0:
            df_allsymbols = df_stock.copy()
        else:
            df_allsymbols = pd.concat([df_allsymbols, df_stock], ignore_index=False)

    if (len(g_message) > 0):
        sentmsg = altMgr.send_chart_alert(g_message)
        print(sentmsg)

    return df_allsymbols.to_json(orient='records', index=False)

if __name__ == "__main__":
    # Run the analysis
    #spy_data = main()
    app.run(debug=True, host='0.0.0.0', port=80) 