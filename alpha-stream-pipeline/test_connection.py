import sqlalchemy
from src.database import engine

# This script performs a direct "interrogation" of the database metadata

def run_test():
    try:
        with engine.connect() as conn:
            # Query the database system catalog for a list of public tables
            inspector = sqlalchemy.inspect(engine)
            tables = inspector.get_table_names()
            print("--- System Health Report ---")
            if "assets" in tables and "daily_prices" in tables:
                print("Test Passed: Database is fully operational")
            else:
                print("Test Failed: Tables were not found")
    except Exception as e:
        print(f"Connection Error: The script could not reach the database \n{e}")

if __name__ == "__main__":
    run_test()