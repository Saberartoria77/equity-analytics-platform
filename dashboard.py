import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
try:
    DB_URL = st.secrets["DB_URL"]
except:
    DB_URL = os.getenv("DB_URL")

st.set_page_config(page_title="Equity Analytics Platform", layout="wide")
st.title("Equity Analytics Platform")

page = st.sidebar.selectbox("Navigation", ["Price & Indicators", "Backtest Results", "Sector Analysis"])

tickers = pd.read_sql(text("SELECT ticker FROM stocks ORDER BY ticker"), engine)["ticker"].tolist()

if page != "Sector Analysis":
    selected = st.selectbox("Select Stock", tickers)

if page == "Price & Indicators":
    df = pd.read_sql(text("""
        SELECT dp.date, dp.close, i.rsi_14, i.sma_20, i.sma_50, i.bb_upper, i.bb_lower
        FROM daily_prices dp
        JOIN indicators i ON dp.stock_id = i.stock_id AND dp.date = i.date
        JOIN stocks s ON s.id = dp.stock_id
        WHERE s.ticker = :ticker
        ORDER BY dp.date
    """), engine, params={"ticker": selected})

    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Close", f"${df['close'].iloc[-1]:.2f}")
    col2.metric("RSI (14)", f"{df['rsi_14'].iloc[-1]:.1f}")
    col3.metric("SMA 20", f"${df['sma_20'].iloc[-1]:.2f}")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], name="Close", line=dict(color="#00d4ff")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["sma_20"], name="SMA 20", line=dict(color="orange")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["sma_50"], name="SMA 50", line=dict(color="#4444ff")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bb_upper"], name="BB Upper", line=dict(color="gray", dash="dash")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bb_lower"], name="BB Lower", line=dict(color="gray", dash="dash")))
    fig.update_layout(title=f"{selected} — Price & Bollinger Bands", template="plotly_dark", height=400)
    st.plotly_chart(fig, width='stretch')

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df["date"], y=df["rsi_14"], name="RSI 14", line=dict(color="#cc44ff")))
    fig2.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
    fig2.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
    fig2.update_layout(title=f"{selected} — RSI (14)", template="plotly_dark", yaxis_range=[0, 100], height=300)
    st.plotly_chart(fig2, width='stretch')

elif page == "Backtest Results":
    st.header("Backtest Results")
    st.caption("Strategy: Buy when RSI < 30, Sell when RSI > 70 | Transaction cost: 1.5% per trade (Wealthsimple FX fee)")

    from backtesting import backtest, compute_metrics, get_backtest_data

    df_bt = get_backtest_data(selected)

    if df_bt.empty:
        st.warning("No data available")
    else:
        returns = backtest(df_bt)

        if len(returns) == 0:
            st.info("No trades triggered for this stock")
        else:
            metrics = compute_metrics(returns)
            buy_hold = (df_bt["close"].iloc[-1] - df_bt["close"].iloc[0]) / df_bt["close"].iloc[0] * 100

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Trades", len(returns))
            col2.metric("RSI Return", f"{sum(returns):.1f}%", delta=f"{sum(returns)-buy_hold:.1f}% vs B&H")
            col3.metric("Buy & Hold", f"{buy_hold:.1f}%")
            col4.metric("Win Rate", f"{metrics['win_rate']}%")

            col5, col6 = st.columns(2)
            col5.metric("Sharpe Ratio", metrics['sharpe'])
            col6.metric("Max Drawdown", f"{metrics['max_drawdown']}%")

            cumulative = []
            total = 0
            for r in returns:
                total += r
                cumulative.append(total)

            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=list(range(1, len(cumulative)+1)),
                y=cumulative,
                name="Cumulative PnL",
                line=dict(color="green"),
                fill="tozeroy"
            ))
            fig3.update_layout(
                title=f"{selected} — Cumulative PnL (%)",
                xaxis_title="Trade #",
                yaxis_title="Cumulative Return (%)",
                template="plotly_dark",
                height=350
            )
            st.plotly_chart(fig3, width='stretch')

elif page == "Sector Analysis":
    st.header("Sector Analysis")
    st.caption("Average daily return by GICS sector across 100 S&P 500 stocks")

    df_sector = pd.read_sql(text("""
        SELECT 
            s.sector,
            ROUND(AVG(dr.daily_return_pct)::numeric, 4) AS avg_daily_return,
            COUNT(DISTINCT s.id) AS num_stocks
        FROM daily_returns_view dr
        JOIN stocks s ON s.id = dr.stock_id
        WHERE dr.daily_return_pct IS NOT NULL
          AND s.sector IS NOT NULL
        GROUP BY s.sector
        ORDER BY avg_daily_return DESC
    """), engine)

    fig4 = go.Figure(go.Bar(
        x=df_sector["sector"],
        y=df_sector["avg_daily_return"],
        marker_color=["#00c853" if v > 0 else "#d32f2f" for v in df_sector["avg_daily_return"]],
        text=df_sector["num_stocks"].apply(lambda x: f"{x} stocks"),
        textposition="outside"
    ))
    fig4.update_layout(
        title="Average Daily Return by Sector",
        xaxis_title="Sector",
        yaxis_title="Avg Daily Return (%)",
        template="plotly_dark",
        height=400
    )
    st.plotly_chart(fig4, width='stretch')

    st.subheader("Sector Summary")
    st.dataframe(df_sector, use_container_width=True)