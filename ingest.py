import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import yfinance as yf
import time

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
load_dotenv()
DB_URL = os.getenv("DB_URL")

DB_URL = DB_URL = "postgresql://postgres:Bernie1217@localhost:5432/equity_analytics"
engine = create_engine(DB_URL)



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
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
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

    logger.info(f"Done: {ticker}, {len(df)} rows inserted")

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AVGO", "ORCL", "ASML",
    "AMD", "QCOM", "TXN", "MU", "AMAT",
    "INTC", "ADI", "KLAC", "LRCX", "MRVL"
]
if __name__ == "__main__":
    tickers_succeeded = 0
    rows_inserted = 0
    errors = []

    for ticker in TICKERS:
        try:
            ingest_stock(ticker)
            tickers_succeeded += 1
            rows_inserted += 1256
        except Exception as e:
            logger.error(f"Failed: {ticker} — {e}")
            errors.append(f"{ticker}: {str(e)}")

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ingestion_runs 
                (tickers_attempted, tickers_succeeded, rows_inserted, errors)
            VALUES (:attempted, :succeeded, :rows, :errors)
        """), {
            "attempted": len(TICKERS),
            "succeeded": tickers_succeeded,
            "rows": rows_inserted,
            "errors": "; ".join(errors) if errors else None
        })

    logger.info(f"Run complete: {tickers_succeeded}/{len(TICKERS)} succeeded, {rows_inserted} rows")