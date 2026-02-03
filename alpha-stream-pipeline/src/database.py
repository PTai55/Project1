import os # Provides access to operating system functionality, such as environment variables and file paths
from sqlalchemy import create_engine, text
# create_engine: used to establish a connection to a database
# text: allows writing raw SQL queries safely within SQLAlchemy
from dotenv import load_dotenv # Loads environment variables from a .env file into the application's environment

## 1. SETUP and CONFIGURATION ##

# load_dotenv looks for a file named '.env' in your project root
# It loads variables like DATABASE_URL so we don't 'hardcode' secrets

# Load credentials
load_dotenv()

# We pull the connection string from the environment
# In a hedge fund, you'd have different URLs for 'dev', 'test', and 'prod'
DB_URL = os.getenv("DATABASE_URL", "postgresql://quant_user:password123@localhost:5432/alpha_db") # Fallback if environment variable isn't found

# The 'engine' is the core of SQLAlchemy. It manages a 'pool' of connections to the PostgreSQL database.
engine = create_engine(DB_URL)

## 2. THE INITIALIZATION FUNCTION ##

def init_db():
    # This function acts as a 'Setup' script. It connects to the database and ensures all required tables exist.
    print("Connecting to database...")
    try:

        # A 'with' block is used here as a 'Context Manager'. It ensures the connection is automatically CLOSED even if the code crashes.
        with engine.connect() as conn:
            print("Connection to PostgreSQL successful! Creating table...")

            # -- Table 1: Assets --
            # This stores high-level info about the companies we track. "SERIAL PRIMARY KEY" means the database will auto-assign IDs (1,2,3...).
            # "UNIQUE" ensures we don't have two "AAPL" entries
            conn.execute(text("""CREATE TABLE IF NOT EXISTS assets (
                                asset_id SERIAL PRIMARY KEY,
                                ticker VARCHAR(10) UNIQUE NOT NULL,
                                company_name VARCHAR(10)
                              );"""))

            # -- Table 2: Daily Prices --
            # This is our "Time Series" table. We use "NUMERIC(19,4)" for prices because it prevents the round errors that occur with standard "Float" numbers.
            # "REFERENCES assets" is a Foreign Key. It links prices to a specific company.
            conn.execute(text("""CREATE TABLE IF NOT EXISTS daily_prices (
                                price_id SERIAL PRIMARY KEY,
                                asset_id INTEGER REFERENCES assets(asset_id),
                                price_date DATE NOT NULL,
                                adj_close_price NUMERIC(19, 4),
                                volume BIGINT,
                                UNIQUE (asset_id, price_date) -- This prevents duplicate data for the same day
                              );"""))
            
            # -- Table 3: Signals --
            # This stores the output of our math (Sharpe Ratio, Buy/Sell signals).
            conn.execute(text("""CREATE TABLE IF NOT EXISTS signals (
                                signal_id SERIAL PRIMARY KEY,
                                asset_id INTEGER REFERENCES assets(asset_id),
                                price_date DATE NOT NULL,
                                signal_type VARCHAR(10),
                                sharpe_ratio NUMERIC(10, 2),
                                max_drawdown NUMERIC(10, 4),
                                UNIQUE (asset_id, price_date)
                              );"""))
            
            # "Commit" saves for all the changes just made.
            conn.commit()
            print("Tables created or verified successfully!")

    except Exception as e:
        # If the database is off or the password is wrong, this block will catch it.
        print(f"Error: Could not connect to the database.\nDetails: {e}")

## 3. The Trigger

# This check ensures that the code inside only runs if I run this file.
# If another file imports "engine", it won't run init_db() automatically.
if __name__ == "__main__":
    init_db()