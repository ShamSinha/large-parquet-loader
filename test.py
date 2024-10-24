import torch
from torch.utils.data import DataLoader
from dataloader import ParquetIterableDataset

def test_dataloader(parquet_file , batch_size=256):
    # Initialize the dataset and DataLoader
    dataset = ParquetIterableDataset(parquet_file)
    dataloader = DataLoader(dataset, batch_size=batch_size, num_workers=2)

    # Variables to track total samples processed
    total_samples = 0

    # Iterate through DataLoader and validate the batch structure
    for batch_idx, (batch_features, batch_labels) in enumerate(dataloader):
        print(f"Batch {batch_idx + 1}:")
        print(f"Features shape: {batch_features.shape}, Labels shape: {batch_labels.shape}")

        # Assertions to ensure correctness
        assert batch_features.shape[1] > 0, "Features should have at least one column"
        assert batch_features.shape[0] <= batch_size, "Batch size exceeds the defined limit"
        assert batch_features.shape[0] == batch_labels.shape[0], "Mismatch in features and labels count"
        assert batch_features.dtype == torch.float32, "Features tensor must be float32"
        assert batch_labels.dtype == torch.long, "Labels tensor must be long"

        # Accumulate total samples
        total_samples += batch_features.shape[0]

    print(f"Total samples processed: {total_samples}")

if __name__ == "__main__":
    # Provide the path to the Parquet file
    parquet_file = 'data/large_dataset_pyarrow.parquet'

    # Run the test function
    batch_size = 256
    test_dataloader(parquet_file , batch_size=batch_size)
