import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("indicators.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


#SMA
def get_prices(stock_id):
    df = pd.read_sql(
        text("SELECT date, close FROM daily_prices WHERE stock_id = :sid ORDER BY date"),
        engine,
        params={"sid": stock_id}
    )
    return df

##compute indicator
def compute_indicators(df):
    df["sma_20"] = df["close"].rolling(20).mean()
    df["sma_50"] = df["close"].rolling(50).mean()
    df["sma_200"] = df["close"].rolling(100).mean()

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - 100 / (1 + rs)

    ema_12 = df["close"].ewm(span=12).mean()
    ema_26 = df["close"].ewm(span=26).mean()
    df["macd_line"] = ema_12 - ema_26
    df["macd_signal"] = df["macd_line"].ewm(span=9).mean()
    df["macd_histogram"] = df["macd_line"] - df["macd_signal"]
    df["bb_middle"] = df["sma_20"]
    df["bb_upper"] = df["bb_middle"] + 2 * df["close"].rolling(20).std()
    df["bb_lower"] = df["bb_middle"] - 2 * df["close"].rolling(20).std()

    return df

def save_indicators(stock_id, df):
    df = df.dropna(subset=["sma_200"])
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO indicators 
                    (stock_id, date, sma_20, sma_50, sma_200, rsi_14,
                     macd_line, macd_signal, macd_histogram,
                     bb_upper, bb_middle, bb_lower)
                VALUES (:sid, :date, :sma_20, :sma_50, :sma_200, :rsi_14,
                        :macd_line, :macd_signal, :macd_histogram,
                        :bb_upper, :bb_middle, :bb_lower)
                ON CONFLICT (stock_id, date) DO NOTHING
            """), {
                "sid": stock_id,
                "date": row["date"],
                "sma_20": float(row["sma_20"]),
                "sma_50": float(row["sma_50"]),
                "sma_200": float(row["sma_200"]),
                "rsi_14": float(row["rsi_14"]),
                "macd_line": float(row["macd_line"]),
                "macd_signal": float(row["macd_signal"]),
                "macd_histogram": float(row["macd_histogram"]),
                "bb_upper": float(row["bb_upper"]),
                "bb_lower": float(row["bb_lower"]),
                "bb_middle": float(row["bb_middle"]),
            })
    logger.info(f"Saved {len(df)} rows for stock_id {stock_id}")


if __name__ == "__main__":
    stock_ids = pd.read_sql(text("SELECT id FROM stocks"), engine)["id"].tolist()
    for sid in stock_ids:
        try:
            df = get_prices(sid)
            df = compute_indicators(df)
            save_indicators(sid, df)
        except Exception as e:
            logger.error(f"Failed stock_id {sid}: {e}")
    logger.info("All indicators computed.")
