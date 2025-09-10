import datetime
from flask import Flask
app = Flask(__name__)

def main():
    now = datetime.now()
    # Convert it to an aware datetime object in the local timezone
    local_now = now.astimezone()
    # Extract the timezone name
    timezone_name = local_now.tzname()
    # Extract the timezone information (including offset)
    timezone_info = local_now.tzinfo
    print(f"Current local timezone name: {timezone_name}")    
    return "Hello, this is a sample Python Web App running on Flask Framework! -- app"
    
    
if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    spy_data = main()
