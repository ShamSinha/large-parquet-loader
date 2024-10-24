import torch
from torch.utils.data import IterableDataset
import pyarrow.parquet as pq
import numpy as np

class ParquetIterableDataset(IterableDataset):
    def __init__(self, parquet_file):
        self.parquet_file = parquet_file
        self.parquet_reader = pq.ParquetFile(self.parquet_file)
        self.num_row_groups = self.parquet_reader.num_row_groups

    def __iter__(self):
        for rg in range(self.num_row_groups):
            # Read a row group
            row_group = self.parquet_reader.read_row_group(rg)
            num_rows = row_group.num_rows

            # Convert row group data into NumPy arrays
            columns = row_group.column_names
            data_arrays = [row_group.column(col).to_numpy() for col in columns]

            # Assuming the last column is the label
            features = np.column_stack(data_arrays[:-1])
            labels = data_arrays[-1]

            # Yield data row by row
            for i in range(num_rows):
                feature_tensor = torch.tensor(features[i], dtype=torch.float32)
                label_tensor = torch.tensor(labels[i], dtype=torch.long)
                yield feature_tensor, label_tensor