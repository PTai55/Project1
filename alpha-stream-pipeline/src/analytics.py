import pandas as pd
import numpy as np
from src.database import engine
from sqlalchemy import text

def calculate_metrics():
    # The system pulls raw data from SQL, performs mathematical analysis, and stores the results in the "signals" table.
    print("Initiating Financial Analysis...")

    with engine.connect() as conn:
        # 1. Extract: The system pulls all tickers currently in the database.
        assets = conn.execute(text("SELECT asset_id, ticker FROM assets")).fetchall()

        for asset_id, ticker in assets:
            print(f"Analyzing {ticker}...")

            # 2. Load Data: The system retrieves price history for the specific asset
            query = text("""SELECT price_date, adj_close_price FROM daily_prices WHERE asset_id = :a_id ORDER BY price_date ASC""")
            df = pd.read_sql(query, conn, params={"a_id": asset_id})

            if len(df) < 20: # Ensure enough data exists for a basic calculation
                print(f"Insufficient data for {ticker}. Skipping.")
                continue

            # 3. Transform (The Math):
            # Calculate Daily Returns: The percentage change from one day to the next
            df["daily_return"] = df["adj_close_price"].pct_change()

            # Calculate Moving Averages: Used by Quants to find "Golden Crosses"
            df["SMA_20"] = df["adj_close_price"].rolling(window=20).mean()

            # Calculate Sharpe Ratio: (Mean Return / Standard Deviation)
            # We annualize it by multiplying by the square root of 252 (trading days in a year)
            avg_return = df["daily_return"].mean()
            std_dev = df["daily_return"].std()
            sharpe = (avg_return / std_dev) * np.sqrt(252) if std_dev != 0 else 0

            # 4. Signal Logic: Simple Trend Following
            # If current price > 20-day average, the signal is "BUY", else "HOLD"
            latest_price = df["adj_close_price"].iloc[-1]
            latest_sma = df["SMA_20"].iloc[-1]
            signal = "BUY" if latest_price > latest_sma else "HOLD"

            # 5. Load: Save the calculated signals back to the database
            conn.execute(text("""INSERT INTO signals (asset_id, price_date, signal_type, sharpe_ratio) 
                            VALUES (:a_id, :p_date, :sig, :sharpe)
                            ON CONFLICT (asset_id, price_date)
                            DO UPDATE SET
                            signal_type = EXCLUDED.signal_type,
                            sharpe_ratio = EXCLUDED.sharpe_ratio"""), {"a_id": asset_id, "p_date": df["price_date"].iloc[-1], "sig": signal, "sharpe": float(sharpe)})
            
            conn.commit()
            print(f"{ticker} Analysis Complete.\nSignal: {signal}\nSharpe: {sharpe:2f}")

if __name__ == "__main__":
    calculate_metrics()