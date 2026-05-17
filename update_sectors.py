import os
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)


def update_sectors():
    with engine.begin() as conn:
        tickers = conn.execute(text("SELECT id, ticker FROM stocks")).fetchall()

    for stock_id, ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            sector = info.get("sector", None)
            industry = info.get("industry", None)

            with engine.begin() as conn:
                conn.execute(text("""
                                  UPDATE stocks
                                  SET sector   = :sector,
                                      industry = :industry
                                  WHERE id = :id
                                  """), {"sector": sector, "industry": industry, "id": stock_id})

            print(f"{ticker}: {sector} / {industry}")
        except Exception as e:
            print(f"{ticker}: Failed — {e}")


if __name__ == "__main__":
    update_sectors()
