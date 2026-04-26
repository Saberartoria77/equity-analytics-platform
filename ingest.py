import yfinance as yf
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")

DB_URL = DB_URL = "postgresql://postgres:Bernie1217@localhost:5432/equity_analytics"
engine = create_engine(DB_URL)

import time

def fetch_with_retry(ticker: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="5y")
            if df.empty:
                raise ValueError(f"Empty data for {ticker}")
            return df
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def ingest_stock(ticker: str):
    df = fetch_with_retry(ticker)

    with engine.begin() as conn:
        conn.execute(text("""
                          INSERT INTO stocks (ticker)
                          VALUES (:ticker) ON CONFLICT (ticker) DO NOTHING
                          """), {"ticker": ticker})

        stock_id = conn.execute(text(
            "SELECT id FROM stocks WHERE ticker = :ticker"
        ), {"ticker": ticker}).scalar()

        for date, row in df.iterrows():
            conn.execute(text("""
                              INSERT INTO daily_prices (stock_id, date, open, high, low, close, volume)
                              VALUES (:stock_id, :date, :open, :high, :low, :close,
                                      :volume) ON CONFLICT (stock_id, date) DO NOTHING
                              """), {
                             "stock_id": stock_id,
                             "date": date.date(),
                             "open": float(row["Open"]),
                             "high": float(row["High"]),
                             "low": float(row["Low"]),
                             "close": float(row["Close"]),
                             "volume": int(row["Volume"])
                         })

    print(f"Done: {ticker}, {len(df)} rows inserted")

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AVGO", "ORCL", "ASML",
    "AMD", "QCOM", "TXN", "MU", "AMAT",
    "INTC", "ADI", "KLAC", "LRCX", "MRVL"
]
if __name__ == "__main__":
    for ticker in TICKERS:
        ingest_stock(ticker)