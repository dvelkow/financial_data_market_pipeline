from pyspark.sql import SparkSession

# 
spark = SparkSession.builder \
    .appName("FinancialDataIngestion") \
    .getOrCreate()

def fetch_data(url, output_path):
    # Using spark for it to read data from a url
    df = spark.read.csv(url, header=True, inferSchema=True)
    # Saving raw data to a prequel file for higher performance
    df.write.mode("overwrite").parquet(output_path)
    return df

if __name__ == "__main__":
    #historical stock data for Apple from Yahoo Finance
    url = "https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=1577836800&period2=1609459200&interval=1d&events=history&includeAdjustedClose=true"
    output_path = "data/large_financial_data.parquet"
    df = fetch_data(url, output_path)
    df.show()
