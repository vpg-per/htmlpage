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
    dt = datetime.now()
    #pmst_string = (f"{dt.date()- timedelta(days=1)} 4:00:00 -0400")
    pmst_string = (f"{dt.date()} 4:00:00 -0400")
    pmst_dt = datetime.strptime(pmst_string, '%Y-%m-%d %H:%M:%S %z')
    #print(f"pmstTime: { pmst_dt }, unixtime: {pmst_dt.timestamp()}")

    regst_string = (f"{dt.date()} 9:30:00 -0400")
    regst_dt = datetime.strptime(regst_string, '%Y-%m-%d %H:%M:%S %z')
    #print(f"pmstTime: { regst_dt }, unixtime: {regst_dt.timestamp()}")

    reget_string = (f"{dt.date()} 10:00:00 -0400")
    reget_dt = datetime.strptime(reget_string, '%Y-%m-%d %H:%M:%S %z')
    #print(f"pmetTime: { reget_dt }, unixtime: {reget_dt.timestamp()}")
    #print(f"{ int(datetime.now().timestamp())}")

    c_reget = reget_dt.timestamp()
    if ( int(datetime.now().timestamp()) < c_reget):
        c_reget = int(datetime.now().timestamp())

    global objMgr    
    stocksymbols = ['SPY']
    #stocksymbols = ['NQ%3DF', 'RTY%3DF', 'GC%3DF']
    allsymbols_data = []
    pm_data = ""
    rg_data = ""
    for ss in stocksymbols:  
        df = objMgr.fetch_stock_data(ss, startPeriod=pmst_dt.timestamp(), endPeriod=reget_dt.timestamp(), interval="15m")
        df_mng_stock = df[ (df['unixtime'] <= regst_dt.timestamp() - 1) ]
        if (df_mng_stock.shape[0] > 0):
            pm_open = df_mng_stock['open'].iloc[0]
            pm_close = df_mng_stock['close'].iloc[-1]
            pm_highest_score = df_mng_stock['high'].max()
            pm_lowest_score = df_mng_stock['low'].min()
            pm_data = f"O:{pm_open}, C:{pm_close}, L:{pm_lowest_score}, H:{pm_highest_score}"
    
        df_rg_stock = df[ (df['unixtime'] >= regst_dt.timestamp() - 1) ]
        if (len(df_rg_stock) > 0):
            rg_open = df_rg_stock['open'].iloc[0]
            rg_close = df_rg_stock['close'].iloc[-1]
            rg_highest_score = df_rg_stock['high'].max()
            rg_lowest_score = df_rg_stock['low'].min()
            rg_data = f"O:{rg_open}, C:{rg_close}, L:{rg_lowest_score}, H:{rg_highest_score}"
        allsymbols_data.append(f"{{ \"symbol\": \"{ss}\", \"pmdata\": \"{{{pm_data}}}\", \"rgdata\": \"{{{rg_data}}}\" }}")
    
    resultdata = ",".join(allsymbols_data)
    if(pm_data != "" or rg_data != ""):
        sentmsg = objMgr.send_chart_alert(resultdata)

    objMgr.DelOldRecordsFromDB()
    json_string = '{"result": "Processing is complete."}'
    return resultdata

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