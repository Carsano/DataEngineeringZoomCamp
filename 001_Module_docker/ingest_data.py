#!/usr/bin/env python
# coding: utf-8

# # Data Gathering

# In[33]:


import pandas as pd

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
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

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[34]:


# Display first rows
df.head()


# In[35]:


# Check data types
df.dtypes


# In[36]:


# Check data shape
df.shape


# # SQL Prep

# In[37]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')


# In[38]:


# How to create a table from pandas to pg
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[39]:


# Create table (without any data n=0)
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# # Data Ingestion

# In[40]:


# Ingestion per batch
df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[41]:


from tqdm.auto import tqdm

for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )
    print("Inserted chunk:", len(df_chunk))

