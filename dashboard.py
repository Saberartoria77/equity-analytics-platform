import os
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

st.title("Equity Analytics Platform")

# Ticker selector
tickers = pd.read_sql(text("SELECT ticker FROM stocks ORDER BY ticker"), engine)["ticker"].tolist()
selected = st.selectbox("Select Stock", tickers)

# Load data
df = pd.read_sql(text("""
    SELECT dp.date, dp.close, i.rsi_14, i.sma_20, i.sma_50, i.bb_upper, i.bb_lower
    FROM daily_prices dp
    JOIN indicators i ON dp.stock_id = i.stock_id AND dp.date = i.date
    JOIN stocks s ON s.id = dp.stock_id
    WHERE s.ticker = :ticker
    ORDER BY dp.date
"""), engine, params={"ticker": selected})

# Price chart
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["date"], y=df["close"], name="Close", line=dict(color="white")))
fig.add_trace(go.Scatter(x=df["date"], y=df["sma_20"], name="SMA 20", line=dict(color="orange")))
fig.add_trace(go.Scatter(x=df["date"], y=df["sma_50"], name="SMA 50", line=dict(color="blue")))
fig.add_trace(go.Scatter(x=df["date"], y=df["bb_upper"], name="BB Upper", line=dict(color="gray", dash="dash")))
fig.add_trace(go.Scatter(x=df["date"], y=df["bb_lower"], name="BB Lower", line=dict(color="gray", dash="dash")))
fig.update_layout(title=f"{selected} Price Chart", template="plotly_dark")
st.plotly_chart(fig, use_container_width=True)

# RSI chart
fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=df["date"], y=df["rsi_14"], name="RSI 14", line=dict(color="purple")))
fig2.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought")
fig2.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold")
fig2.update_layout(title="RSI (14)", template="plotly_dark", yaxis_range=[0, 100])
st.plotly_chart(fig2, use_container_width=True)