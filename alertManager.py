import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from time import gmtime, strftime
from zoneinfo import ZoneInfo
import os
import psycopg2

class AlertManager:
    def __init__(self):
        
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        
        self._message = []
        self.token = os.getenv("TELE_TOKEN")
        self.chat_id = os.getenv("TELE_CHAT_ID")

    def prepare_crsovr_message(self, df):
        # This alert is initiated for 15 or 30 minute time frame only

        arr_interval = [ "15m", "30m"]
        for i in range(1, len(arr_interval)):
            df_sel_rows = df[df['interval'] == arr_interval[i]]
            for date, row in df_sel_rows.tail(1).iterrows():
                if (( row['crossover'] == "Bullish") | ( row['crossover'] == "Bearish")):
                    if (self.isExistsinDB(row) == False):
                        message = ""
                        if (row['crossover'] == "Bullish" & float( row['buyval']) > 0):
                            message = (f"{row['symbol']} Buy signal on {row['interval']} consider trade at {row['buyval']}:{row['sellval']}:{row['stoploss']}")
                        elif (row['crossover'] == "Bearish" & float( row['buyval']) > 0):
                            message = (f"{row['symbol']} Sell signal on {row['interval']} consider trade at {row['buyval']}:{row['sellval']}:{row['stoploss']}")
                        if (len(message) > 0):
                            self._message.append( message )
                            self.AddRecordtoDB(row)

        return

    def send_chart_alert(self, s_message):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={s_message}"
        return requests.get(url).json()

    def get_message(self):
        return self._message
    
    def set_message(self, new_message):
        self._message = new_message

    def isExistsinDB(self, row):
        retval=False
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            dtlookupval = f"{row['nmonth']}-{row['nday']} {row['hour']}:{row['minute']}"
            with psycopg2.connect(conn_string) as conn:
                # Open a cursor to perform database operations
                with conn.cursor() as cur:
                    cur.execute("Select \"triggerTime\", \"interval\", \"crossover\" from rsicrossover where \"triggerTime\"=%s and \"interval\"=%s and \"stocksymbol\"=%s and \"NotificationSent\"=True; ", (dtlookupval, row['interval'], row['symbol'],))
                    if (cur.rowcount > 0 ):
                        retval = True
                cur.close()
            conn.close()
            return retval
            
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")

    def AddRecordtoDB(self, row):
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            dttimeval = f"{row['nmonth']}-{row['nday']} {row['hour']}:{row['minute']}"
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO rsicrossover (\"triggerTime\", \"interval\", \"crossover\", \"stocksymbol\", \"Open\", \"Close\", \"Low\", \"High\", \"NotificationSent\", \"rsiVal\", \"signal\", \"midbnd\", \"ubnd\", \"lbnd\") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                        (dttimeval, row['interval'], row['crossover'], row['symbol'], row['open'], row['close'], row['low'], row['high'], "TRUE", row['rsi'], row['rsignal'], row['midbnd'], row['ubnd'], row['lbnd'])
                    )
        
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")
        return

    def DelOldRecordsFromDB(self):
        conn_string = os.getenv("DATABASE_URL")
        conn = None
        try:
            nowdt = datetime.now().date()- timedelta(days=1)
            dttimeval = f"%{nowdt.strftime('%m')}-{nowdt.strftime('%d')}%"
            delete_sql = "DELETE FROM rsicrossover WHERE \"triggerTime\" like %s;"
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(delete_sql, (dttimeval,))
        
        except psycopg2.Error as e:
            print(f"Error connecting to or querying the database: {e}")
        return
