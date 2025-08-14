# utils/generate_sample_data.py

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os

# Initialize Faker with South African locale
fake = Faker('en_ZA')

# --- Configuration ---
NUM_PRODUCTS = 100
NUM_BRANCHES = 10
START_DATE = datetime(2025, 7, 1)
DAYS_OF_DATA = 45
OUTPUT_DIR = '../data/raw'

# Some typical South African categories (can customize further)
PRODUCT_CATEGORIES = ['Electronics', 'Apparel', 'Groceries', 'Homeware', 'Toys', 'Automotive', 'Health']

# Typical South African provinces for branch locations
SA_PROVINCES = [
    'Gauteng', 'Western Cape', 'KwaZulu-Natal', 'Eastern Cape', 'Limpopo',
    'Mpumalanga', 'Northern Cape', 'Free State', 'North West'
]

# --- Ensure output directory exists ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 1. Generate Products ---
print("Generating product catalog...")
products_data = {
    'product_id': range(1, NUM_PRODUCTS + 1),
    'product_name': [fake.word().capitalize() + ' ' + fake.word() for _ in range(NUM_PRODUCTS)],
    'category': np.random.choice(PRODUCT_CATEGORIES, size=NUM_PRODUCTS),
    'price': np.round(np.random.uniform(5.0, 5000.0, size=NUM_PRODUCTS), 2)  # adjusted price range for local context
}
df_products = pd.DataFrame(products_data)
df_products.to_csv(f'{OUTPUT_DIR}/products.csv', index=False)
print("✅ Products data generated.")

# --- 2. Generate Branches ---
print("Generating branch information...")
branches_data = {
    'branch_id': range(1, NUM_BRANCHES + 1),
    'branch_name': [f'Branch {fake.city()}' for _ in range(NUM_BRANCHES)],
    'location': np.random.choice(SA_PROVINCES, size=NUM_BRANCHES)
}
df_branches = pd.DataFrame(branches_data)
df_branches.to_csv(f'{OUTPUT_DIR}/branches.csv', index=False)
print("✅ Branches data generated.")

# --- 3. Generate Initial Inventory Snapshot ---
print("Generating inventory snapshot...")
inventory_data = []
for branch_id in df_branches['branch_id']:
    for product_id in df_products['product_id']:
        inventory_data.append({
            'branch_id': branch_id,
            'product_id': product_id,
            'stock_on_hand': np.random.randint(0, 200)
        })
df_inventory = pd.DataFrame(inventory_data)
df_inventory.to_csv(f'{OUTPUT_DIR}/inventory_snapshot.csv', index=False)
print("✅ Inventory snapshot generated.")

# --- 4. Generate Daily Sales Data ---
print(f"Generating sales data for {DAYS_OF_DATA} days...")
all_sales = []
for i in range(DAYS_OF_DATA):
    current_date = START_DATE + timedelta(days=i)
    date_str = current_date.strftime('%Y%m%d')
    
    # Each day has a random number of transactions
    num_transactions = np.random.randint(50, 200)
    
    sales_data = {
        'transaction_id': [fake.uuid4() for _ in range(num_transactions)],
        'product_id': np.random.choice(df_products['product_id'], size=num_transactions),
        'branch_id': np.random.choice(df_branches['branch_id'], size=num_transactions),
        'quantity_sold': np.random.randint(1, 6, size=num_transactions),
        'transaction_time': [(current_date + timedelta(seconds=np.random.randint(0, 86400))).isoformat() for _ in range(num_transactions)]
    }
    df_sales_day = pd.DataFrame(sales_data)
    df_sales_day.to_csv(f'{OUTPUT_DIR}/sales_{date_str}.csv', index=False)
    print(f"  -> Generated sales_{date_str}.csv")

print("✅ All sample data generated successfully!")
