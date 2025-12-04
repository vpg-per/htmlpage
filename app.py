from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo
from csPattern import csPattern
from alertManager import AlertManager
import os

def main():

    stocksymbols = os.getenv("CUSTOM_ALERT_SYMBOL")
    if (stocksymbols is None):
        stocksymbols = ['SPY']
    else:
        stocksymbols = stocksymbols.split(",")
    altMgr = AlertManager()
    dbrecval = altMgr.GetStockOrderRecordfromDB(symbol)
    #dbrecval = {'symbol': 'ES%3DF', 'stockprice': '6642.5', 'cspattern': 'Bullish', 'unixtime': '1763562600', 'stoploss': '6661.75', 'profittarget': '6693.75', 'hour': '9', 'minute': '35', 'transstate': 'Open', 'updatedTriggerTime': '1763562600'}

    cs_pattern = csPattern()
    if (dbrecval is not None):
        cs_pattern.openorderon5m = dbrecval
    cs_pattern.analyze_stockcandlesLTF(symbol)

    if (cs_pattern.openorderon5m is not None and cs_pattern.closeorderon5m is None):
        recordinDBTable = altMgr.GetStockOrderRecordfromDB(symbol, 'OpenClose')
        if (recordinDBTable is None):
            altMgr.AddOpenStockOrderRecordtoDB(cs_pattern.openorderon5m)
            msgval = f"""Symbol: {cs_pattern.openorderon5m['symbol']} Time: {cs_pattern.openorderon5m['hour']}:{cs_pattern.openorderon5m['minute']} Pattern: {cs_pattern.openorderon5m['cspattern']} open price: {cs_pattern.openorderon5m['stockprice']} stoploss: {cs_pattern.openorderon5m['stoploss']} profittarget: {cs_pattern.openorderon5m['profittarget']}"""
            if (cs_pattern.openorderon5m['updatedTriggerTime'] != cs_pattern.openorderon5m['unixtime']):
                msgval = f"""Update -- Symbol: {cs_pattern.openorderon5m['symbol']} Time: {cs_pattern.openorderon5m['hour']}:{cs_pattern.openorderon5m['minute']}, Pattern: {cs_pattern.openorderon5m['cspattern']} trigger price: {cs_pattern.openorderon5m['stockprice']} cstwopattern: {cs_pattern.openorderon5m['cstwopattern']} csfvgpattern: {cs_pattern.openorderon5m['csfvgpattern']}"""
            print(msgval)
        
    if (cs_pattern.closeorderon5m is not None):
        print(f"Debug 1: {cs_pattern.openorderon5m}")
        altMgr.AddOpenStockOrderRecordtoDB(cs_pattern.openorderon5m, "OpenClose")
        print(f"Debug 2: {cs_pattern.closeorderon5m}")
        altMgr.AddCloseStockOrderRecordtoDB(cs_pattern.closeorderon5m)
        msgval = f"Symbol: {cs_pattern.closeorderon5m['symbol']} Time: {cs_pattern.closeorderon5m['hour']}:{cs_pattern.closeorderon5m['minute']} Pattern: {cs_pattern.closeorderon5m['cspattern']}, close price: {cs_pattern.closeorderon5m['stockprice']}"


def timeeg():
    
    dt = datetime.now()
    minutes_to_subtract = dt.minute % 5    
    # Create a new datetime object with the rounded minutes and reset seconds/microseconds
    rounded_dt = dt.replace(minute=dt.minute - minutes_to_subtract, second=0, microsecond=0).timestamp() 
    print(f"Original time: {rounded_dt}")
    return 

    now = datetime.now()
    local_now = now.astimezone()
    timezone_name = local_now.tzname()
    timezone_info = local_now.tzinfo
    
    #print(local_now.date()- timedelta(days=5))
    #time_string = (f"{local_now.date()- timedelta(days=5)} 1:00:00 -0400")
    #dt_from_string = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S %z')
    time_string = (f"{local_now.date()- timedelta(days=5)} 1:00:00")
    dt_from_string = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')
    local_now_time = local_now.strftime("%H:%M:%S")
    dt_from_string_time = dt_from_string.strftime("%H:%M:%S")

    #target_timezone = ZoneInfo('America/New_York')
    target_timezone = ZoneInfo('UTC')
    converted_dt_time = dt_from_string.astimezone(target_timezone)

    print(f"time_string: {time_string}")
    print(f"dt_from_string: {dt_from_string.timestamp()}, {dt_from_string.tzname()}, {dt_from_string.tzinfo}, {dt_from_string.date()}, DIV, {dt_from_string_time}")
    print(f"Date: {local_now.date()}, Time: {local_now_time} ")
    print(f"{converted_dt_time}")
    print("Hello")

    
    
    now = datetime.now()
    local_now = now.astimezone()
    timezone_name = local_now.tzname()
    timezone_info = local_now.tzinfo
    
    #print(local_now.date()- timedelta(days=5))
    time_string = (f"{local_now.date()- timedelta(days=5)} 1:00:00 -0400")
    dt_from_string = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S %z')

    print(f"time_string: {time_string}")
    print(f"dt_from_string: {dt_from_string.timestamp()}, {dt_from_string.tzname()}, {dt_from_string.tzinfo}")

    return "Hello, this is a sample Python Web App running on Flask Framework! -- app"


def round_time_to_nearest_5_minutes(dt):
    """
    Rounds a datetime object to the nearest 5 minutes.
    """
    # Calculate the total minutes from the beginning of the day
    total_minutes = dt.hour * 60 + dt.minute + dt.second / 60

    # Round the total minutes to the nearest multiple of 5
    rounded_total_minutes = round(total_minutes / 5) * 5

    # Calculate the difference in minutes
    minute_difference = rounded_total_minutes - total_minutes

    # Create a new datetime object by adding the difference
    rounded_dt = dt + timedelta(minutes=minute_difference)

    # Set seconds and microseconds to zero for a clean 5-minute interval
    return rounded_dt.replace(second=0, microsecond=0)
    
    
if __name__ == '__main__':
    spy_data = main()
    #app.run(host='0.0.0.0', port=80)   
