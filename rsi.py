import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from time import strftime
from flask import Flask, json, render_template, request, session
from dataManager import ServiceManager
app = Flask(__name__)
g_message = []
objMgr = ServiceManager()

def process_stocksignal(symbol="SPY", interval="1d"):
    global objMgr
    global g_message
    
    df = objMgr.calculate_rsi_signal(symbol)    
    g_message = objMgr.get_message()    

    return df


@app.route('/rangePattern')
def RangePattern():
    pmst = request.args['pmst']
    pmet = request.args['pmet']
    curTime = request.args['ct']

    datetime_object_local = datetime.fromtimestamp(int(curTime))
    print(f"Local input datetime: {datetime_object_local}")
    global objMgr
    
    stocksymbols = ['SPY']
    #stocksymbols = ['NQ%3DF', 'RTY%3DF', 'GC%3DF']
    allsymbols_data = []
    for ss in stocksymbols:  
        df_mng_stock = objMgr.fetch_stock_data(ss, startPeriod=pmst, endPeriod=pmet, interval="15m")
        df_rg_stock = objMgr.fetch_stock_data(ss, startPeriod=pmet, endPeriod=curTime, interval="15m")

        pm_open = df_mng_stock[((df_mng_stock['hour']=="04") & (df_mng_stock['minute']=="00"))]['open'].iloc[0]
        pm_close = df_mng_stock[((df_mng_stock['hour']=="09") & (df_mng_stock['minute']=="15"))]['close'].iloc[0]
        pm_highest_score = df_mng_stock['high'].max()
        pm_lowest_score = df_mng_stock['low'].min()
        pm_data = f"o:{pm_open}, h:{pm_highest_score}, l:{pm_lowest_score}, c:{pm_close}"

        rg_open = df_rg_stock[((df_rg_stock['hour']=="09") & (df_rg_stock['minute']=="30"))]['open'].iloc[0]
        rg_close = df_rg_stock[((df_rg_stock['hour']=="09") & (df_rg_stock['minute']=="45"))]['close'].iloc[0]
        rg_highest_score = df_rg_stock['high'].max()
        rg_lowest_score = df_rg_stock['low'].min()
        rg_data = f"o:{rg_open}, h:{rg_highest_score}, l:{rg_lowest_score}, c:{rg_close}"
        allsymbols_data.append(f"{{ 'symbol': {ss}, 'pmdata': {{{pm_data}}}, 'rgdata': {{{rg_data}}} }}")
    
    resultdata = ",".join(allsymbols_data)
    if(len(allsymbols_data) > 0):
        sentmsg = objMgr.send_chart_alert(resultdata)

    objMgr.DelOldRecordsFromDB()
    json_string = '{"result": "Processing is complete."}'
    return json_string

@app.route('/returnPattern')
def ReturnPattern():
    global g_message
    global objMgr

    g_message = []
    objMgr.set_message(g_message)
    stocksymbols = ['QQQ','IWM','GLD']
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
        sentmsg = objMgr.send_chart_alert(g_message)
        print(sentmsg)

    return df_allsymbols.to_json(orient='records', index=False)


@app.route("/bkOutInvoke")
def BkOutInvoke():
    return render_template('./second.html')

@app.route("/")
def index():
#def main():
    global g_message
    g_message = []
    
    return render_template('./index.html')

if __name__ == "__main__":
    # Run the analysis
    #spy_data = main()
    app.run(host='0.0.0.0', port=80) 