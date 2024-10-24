import pyarrow.parquet as pq
import numpy as np
import pyarrow as pa

# Simulate a large dataset
num_rows = 1000000  # One million rows
num_features = 10

# Generate random data
data = np.random.rand(num_rows, num_features)
labels = np.random.randint(0, 2, size=(num_rows, 1))  # Binary classification

# Create a dictionary with feature columns
data_dict = {f'feature_{i}': data[:, i] for i in range(num_features)}
data_dict['label'] = labels.flatten()  # Flatten the labels for 1D storage

# Create a PyArrow Table from the dictionary
table = pa.Table.from_pydict(data_dict)

# Specify Parquet file path
parquet_file = 'data/large_dataset_pyarrow.parquet'

# Write the Table to a Parquet file
pq.write_table(table, parquet_file)

print(f"Dataset saved to {parquet_file}")