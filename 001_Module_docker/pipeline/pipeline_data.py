from tqdm.auto import tqdm
from pandas import DataFrame
from sqlalchemy import Engine


class Pipeline():
    def __init__(self, engine: Engine, name: str):
        self.engine = engine
        self.name = name

    def ingest_chunks_data(self, df_iter: DataFrame):
        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(
                name=self.name,
                con=self.engine,
                if_exists="append"
            )
            print("Inserted chunk:", len(df_chunk))
