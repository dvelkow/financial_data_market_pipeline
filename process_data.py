from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, window

# initializing session
spark = SparkSession.builder \
    .appName("FinancialDataProcessing") \
    .getOrCreate()

def process_data(df):
    # Calculate moving average and standard deviation
    processed_df = df.withColumn("date", col("timestamp").cast("date")) \
        .groupBy(window(col("date"), "30 days").alias("window")) \
        .agg(
            avg("close").alias("avg_close"),
            stddev("close").alias("stddev_close")
        )
    return processed_df
