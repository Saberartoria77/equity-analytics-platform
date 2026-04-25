import yfinance as yf
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")

DB_URL = DB_URL = "postgresql://postgres:Bernie1217@localhost:5432/equity_analytics"
engine = create_engine(DB_URL)


def ingest_stock(ticker: str):
    stock = yf.Ticker(ticker)
    df = stock.history(period="5y")

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


if __name__ == "__main__":
    ingest_stock("AAPL")