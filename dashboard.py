"""Streamlit dashboard for prices, corrected backtests, and sector returns."""

from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from backtesting import run_backtest
from database import create_db_engine, get_database_url

st.set_page_config(page_title="Equity Analytics Platform", layout="wide")


def configured_database_url() -> str:
    """Resolve Streamlit secrets first, then the ordinary environment."""
    try:
        secret_url = st.secrets.get("DB_URL")
    except (FileNotFoundError, KeyError):
        secret_url = None
    return get_database_url(secret_url or os.getenv("DB_URL"))


@st.cache_resource
def database_engine(database_url: str):
    return create_db_engine(database_url)


@st.cache_data(ttl=300)
def load_tickers(database_url: str) -> list[str]:
    frame = pd.read_sql(
        text("SELECT ticker FROM stocks ORDER BY ticker"), database_engine(database_url)
    )
    return frame["ticker"].tolist()


@st.cache_data(ttl=300)
def load_stock_data(database_url: str, ticker: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT dp.date, dp.close, i.rsi_14, i.sma_20, i.sma_50,
                   i.bb_upper, i.bb_lower
            FROM daily_prices dp
            JOIN indicators i ON dp.stock_id = i.stock_id AND dp.date = i.date
            JOIN stocks s ON s.id = dp.stock_id
            WHERE s.ticker = :ticker
            ORDER BY dp.date
            """
        ),
        database_engine(database_url),
        params={"ticker": ticker},
    )


@st.cache_data(ttl=300)
def load_sector_data(database_url: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
            SELECT
                s.sector,
                AVG(dr.daily_return_pct) * 100 AS avg_daily_return_pct,
                COUNT(DISTINCT s.id) AS num_stocks
            FROM daily_returns_view dr
            JOIN stocks s ON s.id = dr.stock_id
            WHERE dr.daily_return_pct IS NOT NULL AND s.sector IS NOT NULL
            GROUP BY s.sector
            ORDER BY avg_daily_return_pct DESC
            """
        ),
        database_engine(database_url),
    )


def render_price_page(frame: pd.DataFrame, ticker: str) -> None:
    if frame.empty:
        st.warning(f"No aligned price and indicator data is available for {ticker}.")
        return
    latest = frame.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Close", f"${latest['close']:.2f}")
    col2.metric("RSI (14)", f"{latest['rsi_14']:.1f}")
    col3.metric("SMA 20", f"${latest['sma_20']:.2f}")

    price_figure = go.Figure()
    price_figure.add_trace(go.Scatter(x=frame["date"], y=frame["close"], name="Close"))
    price_figure.add_trace(go.Scatter(x=frame["date"], y=frame["sma_20"], name="SMA 20"))
    price_figure.add_trace(go.Scatter(x=frame["date"], y=frame["sma_50"], name="SMA 50"))
    price_figure.add_trace(
        go.Scatter(x=frame["date"], y=frame["bb_upper"], name="BB Upper", line={"dash": "dash"})
    )
    price_figure.add_trace(
        go.Scatter(x=frame["date"], y=frame["bb_lower"], name="BB Lower", line={"dash": "dash"})
    )
    price_figure.update_layout(title=f"{ticker} — Price & Bollinger Bands", height=420)
    st.plotly_chart(price_figure, width="stretch")

    rsi_figure = go.Figure(go.Scatter(x=frame["date"], y=frame["rsi_14"], name="RSI 14"))
    rsi_figure.add_hline(y=70, line_dash="dash", line_color="red")
    rsi_figure.add_hline(y=30, line_dash="dash", line_color="green")
    rsi_figure.update_layout(title=f"{ticker} — RSI (14)", yaxis_range=[0, 100], height=300)
    st.plotly_chart(rsi_figure, width="stretch")


def render_backtest_page(frame: pd.DataFrame, ticker: str) -> None:
    st.header("Backtest Results")
    st.caption(
        "Prior-day RSI state is applied to the next return. "
        "Entry and exit costs are 1.5% each."
    )
    if frame.empty:
        st.warning(f"No aligned price and indicator data is available for {ticker}.")
        return

    result = run_backtest(frame[["date", "close", "rsi_14"]])
    metrics = result.metrics
    buy_hold = float(frame["close"].iloc[-1] / frame["close"].iloc[0] - 1)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Completed Trades", metrics["trade_count"])
    col2.metric(
        "RSI Return",
        f"{metrics['total_return']:.1%}",
        delta=f"{metrics['total_return'] - buy_hold:.1%} vs B&H",
    )
    col3.metric("Buy & Hold", f"{buy_hold:.1%}")
    col4.metric("Win Rate", f"{metrics['win_rate']:.1%}")
    col5, col6 = st.columns(2)
    col5.metric("Annualized Sharpe", f"{metrics['sharpe']:.2f}")
    col6.metric("Max Drawdown", f"{metrics['max_drawdown']:.1%}")

    equity_figure = go.Figure(
        go.Scatter(
            x=result.daily["date"],
            y=result.daily["equity"],
            name="Strategy equity",
            fill="tozeroy",
        )
    )
    equity_figure.update_layout(
        title=f"{ticker} — Compounded Strategy Equity", yaxis_title="Growth of $1", height=360
    )
    st.plotly_chart(equity_figure, width="stretch")


def render_sector_page(frame: pd.DataFrame) -> None:
    st.header("Sector Analysis")
    st.caption("Average daily return by recorded sector.")
    if frame.empty:
        st.warning("No sector return data is available.")
        return
    figure = go.Figure(
        go.Bar(
            x=frame["sector"],
            y=frame["avg_daily_return_pct"],
            marker_color=[
                "#00a86b" if value > 0 else "#c33" for value in frame["avg_daily_return_pct"]
            ],
            text=frame["num_stocks"].map(lambda count: f"{count} stocks"),
            textposition="outside",
        )
    )
    figure.update_layout(title="Average Daily Return by Sector", yaxis_title="Percent", height=420)
    st.plotly_chart(figure, width="stretch")
    st.dataframe(frame, width="stretch", hide_index=True)


def main() -> None:
    st.title("Equity Analytics Platform")
    try:
        database_url = configured_database_url()
        tickers = load_tickers(database_url)
    except Exception as error:
        st.error(f"The analytics database is unavailable: {error}")
        st.stop()
    if not tickers:
        st.warning("The database contains no stocks. Run the ingestion pipeline first.")
        st.stop()

    page = st.sidebar.selectbox(
        "Navigation", ["Price & Indicators", "Backtest Results", "Sector Analysis"]
    )
    if page == "Sector Analysis":
        try:
            render_sector_page(load_sector_data(database_url))
        except Exception as error:
            st.error(f"Sector data could not be loaded: {error}")
        return

    ticker = st.selectbox("Select Stock", tickers)
    try:
        frame = load_stock_data(database_url, ticker)
    except Exception as error:
        st.error(f"Data for {ticker} could not be loaded: {error}")
        return
    if page == "Price & Indicators":
        render_price_page(frame, ticker)
    else:
        render_backtest_page(frame, ticker)


main()
