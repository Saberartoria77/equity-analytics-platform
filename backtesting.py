import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

def get_prices(stock_id):
    df = pd.read_sql(
        text("SELECT date, close FROM daily_prices WHERE stock_id = :sid ORDER BY date"),
        engine,
        params={"sid": stock_id}
    )
    return df

def get_backtest_data(ticker):
    df = pd.read_sql(
        text("""
    SELECT dp.date, dp.close, i.rsi_14
    FROM daily_prices dp
    JOIN indicators i ON dp.stock_id = i.stock_id AND dp.date = i.date
    JOIN stocks s ON s.id = dp.stock_id
    WHERE s.ticker = :ticker
    ORDER BY dp.date
"""),
        engine,
        params={"ticker": ticker}
    )
    return df



def backtest(df):
    in_position = False
    buy_price = 0
    returns = []

    for _, row in df.iterrows():
        if row["rsi_14"]<30 and not in_position:

            in_position = True
            buy_price = row["close"]
        elif row["rsi_14"]>70 and in_position:

            in_position = False
            pnl = (row["close"] - buy_price) / buy_price * 100
            returns.append(pnl)

    return returns



if __name__ == "__main__":
    df = get_backtest_data("AAPL")
    returns = backtest(df)

    print(f"=== RSI Strategy ===")
    print(f"Total trades: {len(returns)}")
    print(f"Average return per trade: {sum(returns) / len(returns):.2f}%")
    print(f"Total return: {sum(returns):.2f}%")

    # Buy & Hold
    buy_hold = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0] * 100
    print(f"\n=== Buy & Hold ===")
    print(f"Total return: {buy_hold:.2f}%")