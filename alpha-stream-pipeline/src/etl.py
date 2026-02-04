import yfinance as yf # Our financial data provider
import pandas as pd
from src.database import engine # Import the "engine" created in database.py
from sqlalchemy import text # To write raw SQL inside Python

def fetch_and_load_data(tickers):
    # This function pulls historical prices from Yahoo Finance and saves them to our PostgreSQL database.
    print(f"Starting ETL process for: {tickers}")

    for ticker in tickers:
        print(f"Processing {ticker}...")

        # 1. Download: Get the last 2 years of daily data. Need 2 years to ensure we can calculate 50-day and 200-day averages later.
        data = yf.download(ticker, period="2y", interval="1d")

        if data.empty:
            print(f"Warning: No data found for {ticker}. Skipping.")
            continue
        
        # 2. Transform: Flatten MultiIndex and Clean Columns
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data.reset_index()
        data.columns = [str(col).strip().lower().replace(" ", "_").replace(".", "") for col in data.columns]
        print(f"Debug: Cleaned columns are: {list(data.columns)}")

        # 3. Load: Save to Database 
        with engine.connect() as conn:

            # -- Step A: Ensure the ticker exists in the "assets" table --
            # Use "ON CONFLICT DO NOTHING" so we don't get errors if the ticker is already there.
            conn.execute(text("INSERT INTO assets (ticker) VALUES (:t) ON CONFLICT (ticker) DO NOTHING"), {"t": ticker})

            # -- Step B: Get the unique asset_id for this ticker --
            result = conn.execute(text("SELECT asset_id FROM assets WHERE ticker = :t"), {"t": ticker}).fetchone()
            asset_id = result[0]

            # -- Step C: Load the prices into "daily_prices" --
            # Loop through the rows and "UPSERT" (Update or Insert)
            print(f"Saving {len(data)} rows to database...")
            for index, row in data.iterrows():
                # The user must use the lowercase keys defined in the transformation step above
                p_date = row.get("date")
                price = row.get("adj_close") or row.get("close")
                vol = row.get("volume", 0)
                if p_date is None or price is None:
                    continue
                conn.execute(text("""INSERT INTO 
                                  daily_prices (asset_id, price_date, adj_close_price, volume) VALUES (:a_id, :p_date, :price, :vol)ON CONFLICT (asset_id, price_date) DO NOTHING
                                  """), {"a_id": asset_id, "p_date": p_date, "price": float(price), "vol": int(vol)})
                
                # Finalize the transaction
                conn.commit()
                print(f"{ticker} loaded successfully.")

# This block runs when "python -m src.etl" is executed
if __name__ == "__main__":
    # Add any tickers here!
    my_watchlist = ["AAPL", "NVDA", "TSLA", "BTC-USD"]
    fetch_and_load_data(my_watchlist)