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
import pathlib

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
    # set s3 bucket name and base path
    bucket = wmill.get_variable("u/joshua_chuang/s3_bucket_name")
    s3_base_path = f"s3://{bucket}/warehouse_db_query_mat/"

    df_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_final.parquet"
    df = pl.read_parquet(f"{df_path}", storage_options=storage_options).to_pandas()
    df_import_s3_path = f"s3://{bucket}/export_data/df_final.csv"
    wr.s3.to_csv(df=df, path = df_import_s3_path, dataset=False, boto3_session=s3)


    # adrcrg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/adrcrg_query.parquet"
    # orgprg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/orgprg_query.parquet"
    # prg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/prg_query.parquet"
    # prsprg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/prsprg_query.parquet"

    # df_adrcrg = pl.read_parquet(f"{adrcrg_s3_path}", storage_options=storage_options).to_pandas()
    # df_orgprg = pl.read_parquet(f"{orgprg_s3_path}", storage_options=storage_options).to_pandas()
    # df_prg = pl.read_parquet(f"{prg_s3_path}", storage_options=storage_options).to_pandas()
    # df_prsprg = pl.read_parquet(f"{prsprg_s3_path}", storage_options=storage_options).to_pandas()

    # adrcrg_import_s3_path = f"s3://{bucket}/export_data/df_adrcrg.csv"
    # orgprg_import_s3_path = f"s3://{bucket}/export_data/df_orgprg.csv"
    # prg_import_s3_path = f"s3://{bucket}/export_data/df_prg.csv"
    # prsprg_import_s3_path = f"s3://{bucket}/export_data/df_prsprg.csv"
    # wr.s3.to_csv(df=df_adrcrg, path = adrcrg_import_s3_path, dataset=False, boto3_session=s3)
    # wr.s3.to_csv(df=df_orgprg, path = orgprg_import_s3_path, dataset=False, boto3_session=s3)
    # wr.s3.to_csv(df=df_prg, path = prg_import_s3_path, dataset=False, boto3_session=s3)
    # wr.s3.to_csv(df=df_prsprg, path = prsprg_import_s3_path, dataset=False, boto3_session=s3)