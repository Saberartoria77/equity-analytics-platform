import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# Local DB
LOCAL_URL = os.getenv("DB_URL")
local_engine = create_engine(LOCAL_URL)

# Railway DB —
RAILWAY_URL = os.getenv("RAILWAY_URL")
railway_engine = create_engine(RAILWAY_URL)


def migrate():
    # 1. Create schema on Railway
    with open("schema.sql", "r") as f:
        schema = f.read()

    with railway_engine.begin() as conn:
        conn.execute(text(schema))
    print("Schema created on Railway")

    # 2. Migrate stocks
    stocks = pd.read_sql(text("SELECT * FROM stocks"), local_engine)
    stocks.to_sql("stocks", railway_engine, if_exists="append", index=False)
    print(f"Migrated {len(stocks)} stocks")

    # 3. Migrate daily_prices
    print("Migrating daily_prices (this will take a few minutes)...")
    daily_prices = pd.read_sql(text("SELECT * FROM daily_prices"), local_engine)
    daily_prices.to_sql("daily_prices", railway_engine, if_exists="append", index=False, chunksize=1000)
    print(f"Migrated {len(daily_prices)} daily_prices rows")

    # 4. Migrate indicators
    print("Migrating indicators...")
    indicators = pd.read_sql(text("SELECT * FROM indicators"), local_engine)
    indicators.to_sql("indicators", railway_engine, if_exists="append", index=False, chunksize=1000)
    print(f"Migrated {len(indicators)} indicator rows")

    print("Migration complete!")


if __name__ == "__main__":
    migrate()