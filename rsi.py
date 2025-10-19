import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from time import strftime
from flask import Flask, json, render_template, request, session, render_template_string
from dataManager import ServiceManager
from supresrange import SupportResistanceByInputInterval
import base64

app = Flask(__name__)
g_message = []
objMgr = ServiceManager()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Scalping Analysis for {{ symbol }}</title>
    <style>
        body { font-family: Arial, sans-serif; }
        h1, h2 { color: #333; }
        .container { display: flex; }
        .chart img { max-width: 100%; border: 1px solid #ccc; }
        table { border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>    
    <div class="container">
        <div class="chart">
            <h5>Scalping Analysis for {{ summary.symbol }} -- {{ summary.timeframe}} Chart</h5>
            <img src="data:image/png;base64,{{ chart_image }}" >
        </div>
    </div>
</body>
</html>
"""


# Scalping/Day Trading Example
def scalping_example():
    """Example optimized for scalping and short-term day trading"""
    
    # Initialize for 15-minute scalping (2 days of data)
    scalper = FifteenMinuteSupportResistance("SPY", days_back=1)
    
    # Get comprehensive analysis
    full_results = scalper.calculate_all_15min_levels()
    
    # Get scalping summary
    summary = scalper.get_scalping_summary()
    
    if summary and full_results:
        print(f"\n=== SCALPING SETUP: {summary['symbol']} ( {summary['timeframe']} + Pre-Market) ===")
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
    stocksymbols = ['GLD', 'QQQ', 'IWM']
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

    return render_template_string(HTML_TEMPLATE, summary=summary, chart_image=chart_image_base64)


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
    app.run(debug=True, host='0.0.0.0', port=80) 