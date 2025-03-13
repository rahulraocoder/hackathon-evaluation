from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, lit, avg, max, sum as spark_sum, count
import time
import matplotlib.pyplot as plt
import pandas as pd
import os
import json

def process_with_pyspark():
    """Process data using the PySpark library"""
    print("Processing data with PySpark...")
    start_time = time.time()
    
    # Initialize Spark session with constrained resources
    spark = SparkSession.builder \
        .appName("HackathonExample") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()
    
    # Read data - parquet is more efficient for Spark
    df = spark.read.parquet("sample_data.parquet")
    row_count = df.count()
    print(f"Loaded {row_count} rows")
    
    # Basic transformations
    result = df.withColumn("product", col("value_a") * col("value_b")) \
        .withColumn("ratio", col("value_a") / col("value_b")) \
        .withColumn("date", col("timestamp").cast("timestamp")) \
        .withColumn("active_quantity", when(col("is_active") == True, col("quantity")).otherwise(0))
    
    # Aggregations by category
    agg_by_category = result.groupBy("category").agg(
        count("*").alias("count"),
        avg("value_a").alias("avg_value_a"),
        spark_sum("value_b").alias("total_value_b"),
        max("product").alias("max_product"),
        spark_sum("active_quantity").alias("total_active_quantity")
    )
    
    # Time-based analysis - extract hour and group by it
    result = result.withColumn("hour", hour(col("date")))
    
    hourly_stats = result.groupBy("hour").agg(
        count("*").alias("count"),
        avg("product").alias("avg_product"),
        spark_sum("quantity").alias("total_quantity")
    )
    
    # Create output directory if needed
    os.makedirs("output", exist_ok=True)
    
    # Convert to pandas for visualization and saving
    agg_by_category_pd = agg_by_category.toPandas()
    hourly_stats_pd = hourly_stats.toPandas()
    
    # Save results
    agg_by_category_pd.to_csv("output/pyspark_category_summary.csv", index=False)
    hourly_stats_pd.to_csv("output/pyspark_hourly_stats.csv", index=False)
    
    # Create a simple visualization
    plt.figure(figsize=(10, 6))
    plt.bar(hourly_stats_pd["hour"], hourly_stats_pd["total_quantity"])
    plt.title("Total Quantity by Hour (PySpark)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Total Quantity")
    plt.savefig("output/pyspark_hourly_quantity.png")
    
    # Stop Spark session
    spark.stop()
    
    # Report performance
    duration = time.time() - start_time
    print(f"PySpark processing completed in {duration:.2f} seconds")
    
    # Create metrics JSON
    metrics = {
        "framework": "PySpark",
        "rows_processed": int(row_count),
        "duration_seconds": duration,
        "timestamp": time.time()
    }
    
    # Save metrics
    with open("output/pyspark_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    
    return metrics

if __name__ == "__main__":
    result = process_with_pyspark()
    print(f"PySpark processed {result['rows_processed']} rows in {result['duration_seconds']:.2f} seconds")