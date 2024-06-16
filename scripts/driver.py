import logging
from scripts.ingest_data import fetch_data, read_data
from scripts.process_data import process_data
from scripts.store_data import store_data

# Configuring
logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def main():
    logging.info("Starting the data pipeline process")
    
    try:
        # Ingesting the data
        logging.info("Ingesting data")
        api_key = "your_api_key"
        symbol = "AAPL"
        fetch_data(api_key, symbol)
        df = read_data(symbol)
        logging.info("Data ingestion completed")

        #Processing the data
        logging.info("Processing data")
        processed_df = process_data(df)
        logging.info("Data processing completed")

        # Storing the data
        logging.info("Storing data")
        store_data(processed_df, f"data/{symbol}_processed.parquet")
        logging.info("Data storage completed")
        
    except Exception as e:
        logging.error(f"Error in pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
