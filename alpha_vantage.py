import os

import requests
from dotenv import load_dotenv

load_dotenv()
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_daily(ticker):

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}"
    response = requests.get(url)

    data = response.json()
    return data

if __name__ == "__main__":
    result = fetch_daily("AAPL")
    print(result)