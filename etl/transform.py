# etl/transform.py
import pandas as pd

def transform_data(raw_data):
    """
    Transforms raw dataframes into a star schema model.
    """
    print("Starting data transformation...")
    
    # --- Transform Dimensions ---
    dim_product = raw_data['products'].copy()
    dim_branch = raw_data['branches'].copy()
    
    # Create dim_date from the sales transaction times
    df_sales = raw_data['sales'].copy()
    df_sales['transaction_time'] = pd.to_datetime(df_sales['transaction_time'])
    unique_dates = df_sales['transaction_time'].dt.normalize().unique()
    
    dim_date = pd.DataFrame({'full_date': unique_dates})
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
    dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek
    print("  -> Transformed dimension tables: product, branch, date.")

    # --- Transform Fact Tables ---
    # 1. Fact Sales
    fact_sales = df_sales.merge(raw_data['products'], on='product_id')
    fact_sales['sale_amount'] = fact_sales['quantity_sold'] * fact_sales['price']
    fact_sales['date_key'] = fact_sales['transaction_time'].dt.strftime('%Y%m%d').astype(int)
    
    fact_sales = fact_sales[['date_key', 'product_id', 'branch_id', 'quantity_sold', 'sale_amount']]
    print("  -> Transformed fact_sales.")
    
    # 2. Fact Inventory
    # For simplicity, we'll assume the snapshot is for the first date in our dataset
    fact_inventory = raw_data['inventory'].copy()
    fact_inventory['date_key'] = dim_date['date_key'].min()
    fact_inventory = fact_inventory[['date_key', 'product_id', 'branch_id', 'stock_on_hand']]
    print("  -> Transformed fact_inventory.")

    print("✅ Transformation complete.")
    return {
        'dim_product': dim_product,
        'dim_branch': dim_branch,
        'dim_date': dim_date,
        'fact_sales': fact_sales,
        'fact_inventory': fact_inventory
    }