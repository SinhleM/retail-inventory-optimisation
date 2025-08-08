# etl/pipeline.py
import logging
from extract import extract_data
from transform import transform_data
from load import load_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Main ETL pipeline orchestration function."""
    try:
        logging.info("ETL pipeline started.")
        
        # Step 1: Extract
        raw_data = extract_data()
        
        # Step 2: Transform
        transformed_data = transform_data(raw_data)
        
        # Step 3: Load
        load_data(transformed_data)
        
        logging.info("ETL pipeline finished successfully. 🎉")
        
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()