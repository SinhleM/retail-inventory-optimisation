# etl/load.py
import os
from sqlalchemy import create_engine, text # Import 'text' for raw SQL execution
import pandas as pd

def get_db_engine():
    """Creates a SQLAlchemy engine from environment variables."""
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

def load_data(transformed_data):
    """
    Loads transformed dataframes into the PostgreSQL data warehouse.
    This version truncates existing tables before loading to prevent duplicate key errors.
    """
    print("Starting data loading...")
    engine = get_db_engine()
    
    # Establish a connection to execute raw SQL for truncation
    with engine.connect() as connection:
        with connection.begin() as transaction: # Use a transaction for atomicity
            try:
                # --- Truncate Tables ---
                # TRUNCATE RESTART IDENTITY resets SERIAL sequences
                # CASCADE truncates tables that have foreign key dependencies on these tables
                print("Truncating existing tables: dim_product, dim_branch, dim_date, fact_sales, fact_inventory...")
                connection.execute(text("TRUNCATE TABLE dim_product RESTART IDENTITY CASCADE;"))
                connection.execute(text("TRUNCATE TABLE dim_branch RESTART IDENTITY CASCADE;"))
                connection.execute(text("TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE;"))
                connection.execute(text("TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE;"))
                connection.execute(text("TRUNCATE TABLE fact_inventory RESTART IDENTITY CASCADE;"))
                print("✅ Tables truncated successfully.")
                
                # --- Load Dimensions ---
                # The 'key' columns will be auto-populated by SERIAL
                # 'if_exists='append'' is correct here as tables are now empty
                transformed_data['dim_product'].to_sql('dim_product', connection, if_exists='append', index=False)
                transformed_data['dim_branch'].to_sql('dim_branch', connection, if_exists='append', index=False)
                transformed_data['dim_date'].to_sql('dim_date', connection, if_exists='append', index=False)
                print("  -> Loaded dimension tables.")

                # --- Prepare and Load Facts ---
                # We need to map business keys (e.g., product_id) to the new surrogate keys (product_key)
                
                # Create mapping tables from the newly loaded dimensions
                map_product = pd.read_sql('SELECT product_key, product_id FROM dim_product', connection)
                map_branch = pd.read_sql('SELECT branch_key, branch_id FROM dim_branch', connection)

                # 1. Load Fact Sales
                fact_sales = transformed_data['fact_sales']
                fact_sales = fact_sales.merge(map_product, on='product_id', how='left').merge(map_branch, on='branch_id', how='left')
                # Ensure product_key and branch_key are not NaN after merge if there were mismatches
                fact_sales = fact_sales[['date_key', 'product_key', 'branch_key', 'quantity_sold', 'sale_amount']]
                fact_sales.to_sql('fact_sales', connection, if_exists='append', index=False)
                print("  -> Loaded fact_sales.")

                # 2. Load Fact Inventory
                fact_inventory = transformed_data['fact_inventory']
                fact_inventory = fact_inventory.merge(map_product, on='product_id', how='left').merge(map_branch, on='branch_id', how='left')
                # Ensure product_key and branch_key are not NaN after merge if there were mismatches
                fact_inventory = fact_inventory[['date_key', 'product_key', 'branch_key', 'stock_on_hand']]
                fact_inventory.to_sql('fact_inventory', connection, if_exists='append', index=False)
                print("  -> Loaded fact_inventory.")

                transaction.commit() # Commit the transaction if everything succeeded
            except Exception as e:
                transaction.rollback() # Rollback on error
                print(f"❌ Error during data loading: {e}")
                raise e # Re-raise the exception to be caught by the pipeline
    
    print("✅ Loading complete.")
