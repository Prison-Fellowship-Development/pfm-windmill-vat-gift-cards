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
import uuid
import re
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


    # read sample_data_csv into polars df from s3
    s3_path = f"s3://{bucket}/pca/pca_input_data.csv"
    pca_df = pl.scan_csv(s3_path, storage_options=storage_options).collect()


    # generate UUIDs for participant_id
    def generate_uuids(n):
        return [str(uuid.uuid4()) for _ in range(n)]
    

    # add participant_id column
    uuid_series = pl.Series("participant_id", generate_uuids(pca_df.height), dtype=pl.Utf8)
    if len(uuid_series) != len(set(uuid_series)):
        raise ValueError("UUID collision detected")

    pca_df = pca_df.with_columns(uuid_series)


    # select columns: participant_id, FACILITY, and all columns with "pca_" followed by numbers
    pca_columns = [col for col in pca_df.columns if re.match(r"pca_\d+", col)]
    columns_to_select = ["participant_id", "FACILITY"] + pca_columns
    pca_df = pca_df.select(columns_to_select)


    # rename the FACILITY column to facility_id
    pca_df = pca_df.rename({"FACILITY": "facility_name"})
    print(pca_df.head())


    s3_path = f"s3://{bucket}/pca/pca_materialized.parquet"
    wr.s3.to_parquet(df=pca_df.to_pandas(), path=s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote pca_df data to S3")

