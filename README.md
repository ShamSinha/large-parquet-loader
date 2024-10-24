# large-parquet-loader

This project demonstrates how to use a custom PyTorch DataLoader to handle larger-than-memory Parquet files. The ParquetIterableDataset class enables efficient data streaming by reading Parquet files in chunks, ensuring that even large datasets can be processed without exhausting memory.


This project demonstrates how to use a custom **PyTorch DataLoader** to handle **larger-than-memory Parquet files** efficiently. The `ParquetIterableDataset` streams data from Parquet files in chunks, enabling scalable data loading.

---

## Installation

1. **Clone the repository** (or download the project):
   ```bash
   git clone https://github.com/ShamSinha/large-parquet-loader.git
   cd large-parquet-loader
   ```

2. **Create a virtual environment**

3. **Install the required packages**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Run create_parquet.py to generate a sample Parquet file**:
   ```bash
   python create_parquet.py
   ```
5. **Run test.py to test the ParquetIterableDataset implemented in dataloader.py**:
   ```bash
   python test.py
   ```




