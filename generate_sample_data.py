import pandas as pd
import numpy as np
import os

def generate_sample_data(rows=100000):
    """Generate sample data for processing"""
    print(f"Generating sample dataset with {rows} rows...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Generate data
    data = {
        "id": np.arange(1, rows + 1),
        "value_a": np.random.rand(rows) * 100,
        "value_b": np.random.rand(rows) * 50,
        "timestamp": pd.date_range(start="2023-01-01", periods=rows, freq="1min"),
        "category": np.random.choice(["A", "B", "C", "D", "E"], size=rows),
        "is_active": np.random.choice([True, False], size=rows),
        "quantity": np.random.randint(1, 100, size=rows)
    }
    
    # Create pandas DataFrame and save to CSV
    df = pd.DataFrame(data)
    df.to_csv("sample_data.csv", index=False)
    print(f"Sample data saved to sample_data.csv")
    
    # Create a parquet file as well (better for Spark)
    df.to_parquet("sample_data.parquet", index=False)
    print(f"Sample data saved to sample_data.parquet")

if __name__ == "__main__":
    generate_sample_data()