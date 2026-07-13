"""Bias-safe RSI backtesting with compounded daily metrics."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import TYPE_CHECKING, Sequence

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from database import create_db_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class Trade:
    entry_date: object
    exit_date: object | None
    entry_price: float
    exit_price: float
    net_return: float
    is_open: bool


@dataclass(frozen=True)
class BacktestResult:
    daily: pd.DataFrame
    trades: tuple[Trade, ...]
    metrics: dict[str, float | int]


def get_backtest_data(engine: Engine, ticker: str) -> pd.DataFrame:
    """Load aligned closing-price and RSI data for one ticker."""
    return pd.read_sql(
        text(
            """
            SELECT dp.date, dp.close, i.rsi_14
            FROM daily_prices dp
            JOIN indicators i ON dp.stock_id = i.stock_id AND dp.date = i.date
            JOIN stocks s ON s.id = dp.stock_id
            WHERE s.ticker = :ticker
            ORDER BY dp.date
            """
        ),
        engine,
        params={"ticker": ticker},
    )


def _target_positions(rsi: pd.Series) -> pd.Series:
    signals = pd.Series(float("nan"), index=rsi.index, dtype=float)
    signals.loc[rsi < 30] = 1.0
    signals.loc[rsi > 70] = 0.0
    return signals.ffill().fillna(0.0)


def _build_trades(
    daily: pd.DataFrame, transaction_cost: float
) -> tuple[Trade, ...]:
    trades: list[Trade] = []
    entry_date = None
    entry_price = None
    previous_target = 0.0

    for row in daily.itertuples(index=False):
        target = float(row.target_position)
        if previous_target == 0 and target == 1:
            entry_date = row.date
            entry_price = float(row.close)
        elif previous_target == 1 and target == 0 and entry_price is not None:
            exit_price = float(row.close)
            net_return = (exit_price / entry_price) * (1 - transaction_cost) ** 2 - 1
            trades.append(
                Trade(entry_date, row.date, entry_price, exit_price, net_return, False)
            )
            entry_date = None
            entry_price = None
        previous_target = target

    if entry_price is not None:
        final = daily.iloc[-1]
        exit_price = float(final["close"])
        net_return = (exit_price / entry_price) * (1 - transaction_cost) - 1
        trades.append(Trade(entry_date, None, entry_price, exit_price, net_return, True))
    return tuple(trades)


def compute_metrics(
    strategy_returns: pd.Series, trades: Sequence[Trade]
) -> dict[str, float | int]:
    """Calculate metrics from a compounded daily equity curve."""
    returns = pd.Series(strategy_returns, dtype=float).fillna(0.0)
    if returns.empty:
        return {
            "total_return": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
        }

    equity = (1 + returns).cumprod()
    running_peak = equity.cummax().clip(lower=1.0)
    max_drawdown = float((equity / running_peak - 1).min())
    standard_deviation = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (
        float(returns.mean()) / standard_deviation * sqrt(252)
        if standard_deviation > 0
        else 0.0
    )
    completed = [trade for trade in trades if not trade.is_open]
    win_rate = (
        sum(trade.net_return > 0 for trade in completed) / len(completed)
        if completed
        else 0.0
    )
    return {
        "total_return": float(equity.iloc[-1] - 1),
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "trade_count": len(completed),
    }


def run_backtest(df: pd.DataFrame, transaction_cost: float = 0.015) -> BacktestResult:
    """Apply prior-close RSI state to subsequent close-to-close returns."""
    if not 0 <= transaction_cost < 1:
        raise ValueError("transaction_cost must be between 0 and 1")
    required = {"date", "close", "rsi_14"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")
    if df.empty:
        empty = df.copy(deep=True)
        for column in [
            "market_return",
            "target_position",
            "held_position",
            "transaction_cost",
            "strategy_return",
            "equity",
        ]:
            empty[column] = pd.Series(dtype=float)
        return BacktestResult(empty, (), compute_metrics(pd.Series(dtype=float), ()))

    daily = df.copy(deep=True).sort_values("date").reset_index(drop=True)
    daily["close"] = pd.to_numeric(daily["close"], errors="raise")
    daily["rsi_14"] = pd.to_numeric(daily["rsi_14"], errors="coerce")
    daily["market_return"] = daily["close"].pct_change().fillna(0.0)
    daily["target_position"] = _target_positions(daily["rsi_14"])
    daily["held_position"] = daily["target_position"].shift(1).fillna(0.0)

    position_changes = daily["target_position"].diff().abs()
    position_changes.iloc[0] = abs(float(daily["target_position"].iloc[0]))
    daily["transaction_cost"] = position_changes * transaction_cost
    gross_return = daily["market_return"] * daily["held_position"]
    daily["strategy_return"] = (1 + gross_return) * (
        1 - daily["transaction_cost"]
    ) - 1
    daily["equity"] = (1 + daily["strategy_return"]).cumprod()

    trades = _build_trades(daily, transaction_cost)
    metrics = compute_metrics(daily["strategy_return"], trades)
    return BacktestResult(daily, trades, metrics)


def main() -> int:
    load_dotenv()
    engine = create_db_engine()
    for ticker in ["AAPL", "NVDA", "TSLA", "MSFT"]:
        frame = get_backtest_data(engine, ticker)
        if frame.empty:
            print(f"{ticker}: no data")
            continue
        result = run_backtest(frame)
        metrics = result.metrics
        print(
            f"{ticker} | Trades: {metrics['trade_count']} | "
            f"Return: {metrics['total_return']:.1%} | "
            f"Sharpe: {metrics['sharpe']:.2f} | "
            f"MaxDD: {metrics['max_drawdown']:.1%}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
