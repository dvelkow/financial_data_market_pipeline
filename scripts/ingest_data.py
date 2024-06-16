from pyspark.sql import SparkSession
import requests
import pandas as pd

# Initializing session
spark = SparkSession.builder \
    .appName("FinancialDataIngestion") \
    .getOrCreate()

# Fetching data from API
def fetch_data(api_key, symbol, function="TIME_SERIES_DAILY"):
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}&datatype=csv"
    response = requests.get(url)
    with open(f"data/{symbol}_daily.csv", 'wb') as f:
        f.write(response.content)

def read_data(symbol):
    df = spark.read.csv(f"data/{symbol}_daily.csv", header=True, inferSchema=True)
    return df

if __name__ == "__main__":
    api_key = "your_api_key"
    symbol = "AAPL"
    fetch_data(api_key, symbol)
    df = read_data(symbol)
    df.show()
