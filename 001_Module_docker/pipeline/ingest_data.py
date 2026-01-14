from data_prep import DataPrep
from pipeline_data import Pipeline
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()


# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/' \
         'nyc-tlc-data/releases/download/yellow/'
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
file_name = 'yellow_tripdata_2021-01.csv.gz'
db_supplier = os.getenv("DB_SUPPLIER")
host = os.getenv("HOSTNAME")
port = os.getenv("PORT")
user = os.getenv("USERNAME")
pwd = os.getenv("DB_PASSWORD")

data_prep = DataPrep(dtype=dtype, parse_dates=parse_dates, prefix_path=prefix)
engine = create_engine(f'{db_supplier}://{user}:{pwd}@{host}:{port}/ny_taxi')
print(engine)
data_prep.get_sample_data(
    file_name=file_name, sample_size=10).head(n=0).to_sql(
    name='yellow_taxi_data', con=engine, if_exists='replace')

df_iter = data_prep.get_iter_data(file_name=file_name)
pipeline_data = Pipeline(engine=engine, name='yellow_taxi_data')

pipeline_data.ingest_chunks_data(df_iter=df_iter)
