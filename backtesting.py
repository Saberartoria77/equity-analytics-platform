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
            cost = 1.5  # Wealthsimple FX fee per trade (CAD account)
            pnl = (row["close"] - buy_price) / buy_price * 100 - (cost * 2)
            returns.append(pnl)

    return returns



def compute_metrics(returns):
    if len(returns) == 0:
        return {}

    win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
    avg_return = sum(returns) / len(returns)

    # Sharpe ratio (simplified, assuming risk-free rate = 0)
    import statistics
    if len(returns) > 1:
        sharpe = avg_return / statistics.stdev(returns)
    else:
        sharpe = 0

    # Max drawdown
    cumulative = 0
    peak = 0
    max_dd = 0
    for r in returns:
        cumulative += r
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_dd:
            max_dd = drawdown

    return {
        "win_rate": round(win_rate, 1),
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_dd, 2)
    }



if __name__ == "__main__":
    tickers = ["AAPL", "NVDA", "TSLA", "MSFT"]

    for ticker in tickers:
        df = get_backtest_data(ticker)
        if df.empty:
            continue
        returns = backtest(df)
        if len(returns) == 0:
            print(f"{ticker}: No trades triggered")
            continue
        metrics = compute_metrics(returns)
        buy_hold = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0] * 100
        print(f"{ticker} | Trades: {len(returns)} | Return: {sum(returns):.1f}% | B&H: {buy_hold:.1f}% | Win%: {metrics['win_rate']} | Sharpe: {metrics['sharpe']} | MaxDD: {metrics['max_drawdown']}%")