# etl/load.py
import os
from sqlalchemy import create_engine
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
    """
    print("Starting data loading...")
    engine = get_db_engine()
    
    # --- Load Dimensions ---
    # The 'key' columns will be auto-populated by SERIAL
    transformed_data['dim_product'].to_sql('dim_product', engine, if_exists='append', index=False)
    transformed_data['dim_branch'].to_sql('dim_branch', engine, if_exists='append', index=False)
    transformed_data['dim_date'].to_sql('dim_date', engine, if_exists='append', index=False)
    print("  -> Loaded dimension tables.")
 
    # --- Prepare and Load Facts ---
    # We need to map business keys (e.g., product_id) to the new surrogate keys (product_key)
    
    # Create mapping tables from the newly loaded dimensions
    map_product = pd.read_sql('SELECT product_key, product_id FROM dim_product', engine)
    map_branch = pd.read_sql('SELECT branch_key, branch_id FROM dim_branch', engine)

    # 1. Load Fact Sales
    fact_sales = transformed_data['fact_sales']
    fact_sales = fact_sales.merge(map_product, on='product_id').merge(map_branch, on='branch_id')
    fact_sales = fact_sales[['date_key', 'product_key', 'branch_key', 'quantity_sold', 'sale_amount']]
    fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)
    print("  -> Loaded fact_sales.")

    # 2. Load Fact Inventory
    fact_inventory = transformed_data['fact_inventory']
    fact_inventory = fact_inventory.merge(map_product, on='product_id').merge(map_branch, on='branch_id')
    fact_inventory = fact_inventory[['date_key', 'product_key', 'branch_key', 'stock_on_hand']]
    fact_inventory.to_sql('fact_inventory', engine, if_exists='append', index=False)
    print("  -> Loaded fact_inventory.")

    print("✅ Loading complete.")