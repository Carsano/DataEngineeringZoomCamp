import pandas as pd
from typing import Dict, List


class DataPrep():
    def __init__(self, dtype: Dict, parse_dates: List, prefix_path: str):
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.prefix_path = prefix_path

    def get_sample_data(self, file_name: str, sample_size: int = 100):
        df = pd.read_csv(
            self.prefix_path + file_name,
            nrows=sample_size,
            dtype=self.dtype,
            parse_dates=self.parse_dates
        )
        return df

    def get_iter_data(self, file_name: str, chunksize: int = 10000):
        df_iter = pd.read_csv(
            self.prefix_path + file_name,
            dtype=self.dtype,
            parse_dates=self.parse_dates,
            iterator=True,
            chunksize=chunksize
        )
        return df_iter
