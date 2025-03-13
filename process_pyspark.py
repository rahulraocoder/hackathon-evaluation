from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, when
import time
import os

def process_with_pyspark():
    """Process data using PySpark"""
    print("Processing data with PySpark...")
    start_time = time.time()

    # Initialize Spark session with specific configurations
    spark = SparkSession.builder \
        .appName("PySpark Processing") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()

    try:
        # Try reading parquet first
        print("Attempting to read parquet file...")
        df = spark.read.csv("sample_data.csv", header=True, inferSchema=True)
        print("Successfully read the data")
        
        # Print schema and sample data
        print("\nSchema:")
        df.printSchema()
        print("\nSample Data:")
        df.show(5)

        # Perform calculations
        print("\nPerforming calculations...")
        
        # Basic statistics
        stats = df.agg(
            avg("value_a").alias("avg_value_a"),
            avg("value_b").alias("avg_value_b"),
            count("*").alias("total_records")
        )

        # Category-wise aggregation
        category_stats = df.groupBy("category").agg(
            count("*").alias("count"),
            avg("value_a").alias("avg_value_a"),
            sum("quantity").alias("total_quantity")
        )

        # Active vs Inactive analysis
        active_stats = df.groupBy("is_active").agg(
            count("*").alias("count"),
            avg("value_a").alias("avg_value_a")
        )

        # Save results
        output_path = "output"
        os.makedirs(output_path, exist_ok=True)

        # Convert to pandas and save results
        stats.toPandas().to_csv(f"{output_path}/pyspark_basic_stats.csv", index=False)
        category_stats.toPandas().to_csv(f"{output_path}/pyspark_category_stats.csv", index=False)
        active_stats.toPandas().to_csv(f"{output_path}/pyspark_active_stats.csv", index=False)

        # Calculate execution time
        execution_time = time.time() - start_time
        
        # Save metrics
        with open(f"{output_path}/spark_metrics.txt", "w") as f:
            f.write(f"Execution time: {execution_time:.2f} seconds\n")
            f.write(f"Total records processed: {df.count()}\n")

        print(f"\nProcessing completed in {execution_time:.2f} seconds")
        return True

    except Exception as e:
        print(f"Error during processing: {str(e)}")
        return False

    finally:
        spark.stop()

if __name__ == "__main__":
    result = process_with_pyspark()
    exit(0 if result else 1)