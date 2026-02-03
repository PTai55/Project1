import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

## 1. SETUP and CONFIGURATION ##

# load_dotenv looks for a file named '.env' in your project root
# It loads variables like DATABASE_URL so we don't 'hardcode' secrets

# Load credentials
load_dotenv()

# We pull the connection string from the environment
# In a hedge fund, you'd have different URLs for 'dev', 'test', and 'prod'
DB_URL = os.getenv("DATABASE_URL")

# The 'engine' is the core of SQLAlchemy. It manages a 'pool' of connections to the PostgreSQL database.
engine = create_engine(DB_URL)

## 2. THE INITIALIZATION FUNCTION ##

def init_db():
    # This function creates the tables in my database
    print("Connecting to database...")
    try:
        with engine.connect() as conn:
            print("Connection successful! Creating table...")

            conn.execute(text("CREATE TABLE IF NOT EXISTS"))

if __name__ == "__main__":
    init_db()