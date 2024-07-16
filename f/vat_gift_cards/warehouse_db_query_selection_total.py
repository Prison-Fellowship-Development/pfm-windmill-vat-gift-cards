import os
from pandas.core.dtypes.concat import concat_compat
from pandas.core.indexes.base import can_hold_element
import wmill
import boto3
import polars as pl
import pandas as pd
import awswrangler as wr
import connectorx as cx
import pyarrow
pl.Config.set_streaming_chunk_size(100000)

def main():  
    # CREATE S3 Connection
    # initiate s3 connection
    s3 = boto3.Session(
        region_name='us-east-2',
        aws_access_key_id = wmill.get_variable("u/joshua_chuang/s3_key"),
        aws_secret_access_key=wmill.get_variable("u/joshua_chuang/s3_secret")
        )
    # initiate s3 connection for polars
    storage_options = {
        "aws_access_key_id": wmill.get_variable("u/joshua_chuang/s3_key"),
        "aws_secret_access_key": wmill.get_variable("u/joshua_chuang/s3_secret"),
        "aws_region": "us-east-2",
    }
    # set s3 bucket name
    bucket = wmill.get_variable("u/joshua_chuang/s3_bucket_name")

    # get joined parquet
    join_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_join.parquet"
    lazy_df_join = pl.scan_parquet(join_s3_path, storage_options=storage_options)

    # select columns
    df_final = lazy_df_join.select(["caregiver_first_name", 
                        "caregiver_last_name", 
                        "caregiver_address1", 
                        "caregiver_address2", 
                        "caregiver_city", 
                        "caregiver_stateid", 
                        "caregiver_zipcode", 
                        "caregiver_email", 
                        "caregiver_phone", 
                        "child_first_name", 
                        "child_last_name", 
                        "message_to_child"]).collect()
    
    join_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_final.parquet"
    wr.s3.to_parquet(df=df_final.to_pandas(), path=join_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote warehouse_db_final data to S3")
