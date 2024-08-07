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
    pca_join_s3_path = f"s3://{bucket}/pca/pca_join.parquet"
    pca_df = pl.read_parquet(pca_join_s3_path, storage_options=storage_options)

    # select relevant columns
    pca_columns = [col for col in pca_df.columns if re.match(r"pca_\d+", col)]
    selected_columns = ["participant_id", "survey_id"] + pca_columns
    pca_df = pca_df.select(selected_columns)

    # convert to pandas dataframe for easier manipulation
    pca_df = pca_df.to_pandas()

    # melt the pca columns
    pca_df = pd.melt(pca_df, id_vars=['participant_id', 'survey_id'], 
                     var_name='item_id', value_name='response')

    # sort values
    pca_df = pca_df.sort_values(['participant_id', 'item_id'])

    # drop rows with missing responses
    pca_df = pca_df.dropna(subset=['response'])

    # print("survey_id counts:")
    # print(pca_df['survey_id'].count())

    # save the final dataframe to s3
    s3_path = f"s3://{bucket}/pca/pca_final.parquet"
    wr.s3.to_parquet(df=pca_df, path=s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote joined final pca_df data to S3")
    print(pca_df.columns)