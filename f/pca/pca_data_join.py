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


    # read data into polars df from s3
    input_data_s3_path = f"s3://{bucket}/pca/pca_materialized.parquet"
    pca_df = pl.scan_parquet(input_data_s3_path, storage_options=storage_options)
    cor_sites_s3_path = f"s3://{bucket}/pca/correction_sites_query.parquet"
    cor_sites_df = pl.scan_parquet(cor_sites_s3_path, storage_options=storage_options)
    survey_s3_path = f"s3://{bucket}/pca/survey_query.parquet"
    survey_df = pl.scan_parquet(survey_s3_path, storage_options=storage_options)

    # QUERY OPTIMIZATION STEP 2: PERFORM JOINS (left-deep trees)
    pca_df = pca_df.join(cor_sites_df, on="facility_name", how="inner").unique()
    print("Inner Join: + cor_sites_df")
    pca_df = pca_df.join(survey_df, on="facility_id", how="inner").unique()
    print("Inner Join: + survey_df")

    pca_df = pca_df.collect(streaming=True)

    s3_path = f"s3://{bucket}/pca/pca_join.parquet"
    wr.s3.to_parquet(df=pca_df.to_pandas(), path=s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote joined pca_df data to S3")

    print(pca_df.columns)

