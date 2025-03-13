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
        # Convert timestamp to string format that Spark can handle
        "timestamp": [pd.Timestamp(ts).strftime('%Y-%m-%d %H:%M:%S') 
                     for ts in pd.date_range(start="2023-01-01", periods=rows, freq="1min")],
        "category": np.random.choice(["A", "B", "C", "D", "E"], size=rows),
        "is_active": np.random.choice([True, False], size=rows),
        "quantity": np.random.randint(1, 100, size=rows)
    }
    
    # Create pandas DataFrame
    df = pd.DataFrame(data)
    
    # Save as CSV
    df.to_csv("sample_data.csv", index=False)
    print(f"Sample data saved to sample_data.csv")
    
    # Save as parquet with specific timestamp handling
    df.to_parquet(
        "sample_data.parquet",
        index=False,
        engine='pyarrow',
        compression='snappy'
    )
    print(f"Sample data saved to sample_data.parquet")

if __name__ == "__main__":
    generate_sample_data()