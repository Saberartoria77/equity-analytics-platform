import pandas as pd
import pytest

from backtesting import compute_metrics, run_backtest


def frame(close, rsi):
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=len(close), freq="B"),
            "close": close,
            "rsi_14": rsi,
        }
    )


def test_signal_is_applied_to_next_return():
    result = run_backtest(frame(close=[100, 110, 121], rsi=[20, 50, 80]), transaction_cost=0)

    assert result.daily["strategy_return"].tolist() == pytest.approx([0, 0.10, 0.10])


def test_transaction_costs_apply_on_entry_and_exit():
    result = run_backtest(
        frame(close=[100, 110, 121, 120], rsi=[20, 50, 80, 50]),
        transaction_cost=0.01,
    )

    assert result.daily["transaction_cost"].tolist() == pytest.approx([0.01, 0, 0.01, 0])


def test_total_return_compounds_daily_returns():
    metrics = compute_metrics(pd.Series([0.10, 0.10]), [])

    assert metrics["total_return"] == pytest.approx(0.21)


def test_max_drawdown_uses_compounded_equity_curve():
    metrics = compute_metrics(pd.Series([0.10, -0.20, 0.05]), [])

    assert metrics["max_drawdown"] == pytest.approx(-0.20)


def test_open_position_is_marked_to_final_close():
    result = run_backtest(frame(close=[100, 105, 110], rsi=[20, 50, 50]), transaction_cost=0)

    assert len(result.trades) == 1
    assert result.trades[0].is_open is True
    assert result.trades[0].net_return == pytest.approx(0.10)


def test_input_is_not_mutated():
    source = frame(close=[100, 101, 102], rsi=[20, 50, 80])
    original = source.copy(deep=True)

    run_backtest(source)

    pd.testing.assert_frame_equal(source, original)
