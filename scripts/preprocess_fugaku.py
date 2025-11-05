#!/usr/bin/env python3
"""
Split up the large fugaku parquets so druid doesn't choke on them when ingesting.
"""

from pathlib import Path
import pandas as pd
import sys
from collections.abc import Iterable
from pyarrow.parquet import ParquetFile
import pyarrow as pa 

def read_parquet_chunked(file, chunk_size) -> Iterable[pd.DataFrame]:
    pf = ParquetFile(file) 
    for chunk in pf.iter_batches(batch_size = chunk_size):
        yield chunk.to_pandas()

if __name__ == "__main__":
    data_path = Path(sys.argv[1])
    files = list(data_path.glob("*.parquet"))

    for file in files:
        for chunk_df in read_parquet_chunked(file, 100_000):
            chunk_df['date'] = pd.to_datetime(chunk_df['sdt']).dt.strftime("%Y-%m-%d")
            # fugaku dataset is indexed by submission date
            for date, date_df in chunk_df.groupby('date'):
                day_dir = data_path / Path(f"date={date}")
                day_dir.mkdir(exist_ok = True)
                num = max([int(p.stem) for p in day_dir.glob("*.parquet")], default=-1) + 1
                date_df.to_parquet(day_dir / f"{num:03}.parquet")

    # Delete the old parquets
    for file in data_path.glob("*.parquet"):
        file.unlink()
