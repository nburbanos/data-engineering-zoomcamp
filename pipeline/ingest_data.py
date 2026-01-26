#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pandas as pd
from sqlalchemy import create_engine
import tqdm as tqdm
import click


# In[2]:


# df = pd.read_csv(f'{prefix} + yellow_tripdata_{year}-{month:02d}.csv.gz')


# df.head()

# In[4]:

# df.dtypes



dtype = {
    "vendor_id": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "rate_code_id": "Int64",
    "store_and_fwd_flag": "string",
    "pu_location_id": "Int64",
    "do_location_id": "Int64",
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
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]




# In[7]:

#Added from the video 1:32


# get_ipython().system('uv add sqlalchemy psycopg2-binary')




# In[9]:


# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[11]:


#Create table


@click.command()
@click.option('--pg-user', default='root', help='Postgres user')
@click.option('--pg-pass', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-port', default=5432, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', help='Postgres database')
@click.option('--year', default=2025, type=int, help='Year of dataset')
@click.option('--month', default=11, type=int, help='Month of dataset')
@click.option('--target-table', default='yellow_taxi_data', help='Target Postgres table')
@click.option('--chunksize', default=100000, type=int, help='CSV read chunk size')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    newUrl = "https://raw.githubusercontent.com/nburbanos/data-engineering-zoomcamp/master/output.csv"
    df_iter = pd.read_csv(
        newUrl,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first=True
    for df_chunk in tqdm.tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

    print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    run()



