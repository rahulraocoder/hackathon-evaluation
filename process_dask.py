import dask.dataframe as dd
import pandas as pd
import time
import matplotlib.pyplot as plt
import os
import json

def process_with_dask():
    """Process data using the Dask library"""
    print("Processing data with Dask...")
    start_time = time.time()
    
    # Read data
    df = dd.read_csv("sample_data.csv")
    print(f"Loaded dataframe with {len(df.columns)} columns")
    
    # Basic transformations
    df["product"] = df["value_a"] * df["value_b"]
    df["ratio"] = df["value_a"] / df["value_b"]
    df["date"] = dd.to_datetime(df["timestamp"])
    df["active_quantity"] = df["quantity"] * df["is_active"]
    
    # Aggregations by category
    agg_by_category = df.groupby("category").agg({
        "id": "count",
        "value_a": "mean",
        "value_b": "sum",
        "product": "max",
        "active_quantity": "sum"
    }).compute()
    
    # Rename columns for clarity
    agg_by_category.columns = ["count", "avg_value_a", "total_value_b", "max_product", "total_active_quantity"]
    agg_by_category = agg_by_category.reset_index()
    
    # Time-based analysis - extract hour and group by it
    df["hour"] = df["date"].dt.hour
    hourly_stats = df.groupby("hour").agg({
        "id": "count",
        "product": "mean",
        "quantity": "sum"
    }).compute()
    
    # Rename columns for clarity
    hourly_stats.columns = ["count", "avg_product", "total_quantity"]
    hourly_stats = hourly_stats.reset_index()
    
    # Create output directory if needed
    os.makedirs("output", exist_ok=True)
    
    # Save results
    agg_by_category.to_csv("output/dask_category_summary.csv", index=False)
    hourly_stats.to_csv("output/dask_hourly_stats.csv", index=False)
    
    # Create a simple visualization
    plt.figure(figsize=(10, 6))
    plt.bar(hourly_stats["hour"], hourly_stats["total_quantity"])
    plt.title("Total Quantity by Hour (Dask)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Total Quantity")
    plt.savefig("output/dask_hourly_quantity.png")
    
    # Report performance
    final_df = df.compute()  # Materialize to measure accurate row count
    duration = time.time() - start_time
    print(f"Dask processing completed in {duration:.2f} seconds")
    
    # Create metrics JSON
    metrics = {
        "framework": "Dask",
        "rows_processed": len(final_df),
        "duration_seconds": duration,
        "timestamp": time.time()
    }
    
    # Save metrics
    with open("output/dask_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    
    return metrics

if __name__ == "__main__":
    result = process_with_dask()
    print(f"Dask processed {result['rows_processed']} rows in {result['duration_seconds']:.2f} seconds")