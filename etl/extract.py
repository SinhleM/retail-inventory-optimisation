# etl/extract.py
import pandas as pd
import glob
import os

RAW_DATA_PATH = 'data/raw'

def extract_data():
    """
    Extracts data from all CSV files in the raw data directory.
    - Reads and concatenates all sales_YYYYMMDD.csv files.
    - Reads products, branches, and inventory snapshot files.
    """
    print("Starting data extraction...")
    
    # Extract sales data
    sales_files = glob.glob(os.path.join(RAW_DATA_PATH, 'sales_*.csv'))
    df_sales = pd.concat((pd.read_csv(f) for f in sales_files), ignore_index=True)
    print(f"  -> Extracted {len(df_sales)} sales records.")
    
    # Extract other data
    df_products = pd.read_csv(os.path.join(RAW_DATA_PATH, 'products.csv'))
    df_branches = pd.read_csv(os.path.join(RAW_DATA_PATH, 'branches.csv'))
    df_inventory = pd.read_csv(os.path.join(RAW_DATA_PATH, 'inventory_snapshot.csv'))
    print("  -> Extracted products, branches, and inventory data.")
    
    print("✅ Extraction complete.")
    return {
        'sales': df_sales,
        'products': df_products,
        'branches': df_branches,
        'inventory': df_inventory
    }