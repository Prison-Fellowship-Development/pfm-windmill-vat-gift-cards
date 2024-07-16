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

    # define file paths
    s3_base_path = f"s3://{bucket}/warehouse_db_query_mat/"
    file_paths = {
        "org": "org_query.parquet",
        "orgprg": "orgprg_query.parquet",
        "prg": "prg_query.parquet",
        "prsprg": "prsprg_query.parquet",
        "orgprgchdj": "orgprgchdj_query.parquet",
        "inmapp": "inmapp_query.parquet",
        "inmappchdj": "inmappchdj_query.parquet",
        "inm": "inm_query.parquet",
        "prs": "prs_query.parquet",
        "crg": "crg_query.parquet",
        "adrcrg": "adrcrg_query.parquet",
        "cntcrg": "cntcrg_query.parquet",
        "chd": "chd_query.parquet",
    }

    # read dataframes from s3
    dataframes = {name: pl.scan_parquet(s3_base_path + path, storage_options=storage_options) for name, path in file_paths.items()}
    print("Initialized lazy dataframes from s3")
    
    # QUERY OPTIMIZATION STEP 2: PERFORM JOINS (left-deep trees)
    # inner joins -----------------------------------------------------------
    lazy_df_join = dataframes['org'].join(dataframes['orgprg'], on="orgid", how="inner").unique()
    print("Inner Join: org + orgprg")
    lazy_df_join = lazy_df_join.join(dataframes['prg'], on="programid", how="inner").unique()
    print("Inner Join: + prg")
    lazy_df_join = lazy_df_join.join(dataframes['prsprg'], on="programid", how="inner").unique()
    print("Inner Join: + prsprg")
    lazy_df_join = lazy_df_join.join(dataframes['orgprgchdj'], on="orgprogramid", how="inner").unique()
    print("Inner Join: + orgprgchdj")
    
    df_join = lazy_df_join.collect(streaming=True)
    df_join = df_join.cast({"applicationid": pl.Int32})
    lazy_df_join = df_join.lazy()

    lazy_df_join = lazy_df_join.join(dataframes['inmapp'], on="applicationid", how="inner").unique()
    print("Inner Join: + inmapp")
    lazy_df_join = lazy_df_join.join(dataframes['inmappchdj'], on=["childid", "applicationid"], how="inner").unique()
    print("Inner Join: + inmappchdj")
    lazy_df_join = lazy_df_join.join(dataframes['inm'], on="inmateid", how="inner").unique()
    print("Inner Join: + inm")
    lazy_df_join = lazy_df_join.join(dataframes['prs'], on="prisonid", how="inner").unique()
    print("Inner Join: + prs")
    print(df_join)
    # left joins -----------------------------------------------------------
    lazy_df_join = lazy_df_join.join(dataframes['crg'], on="caregiverid", how="left").unique()
    print("Left Join: + crg")

    df_join = lazy_df_join.collect(streaming=True)
    df_join = df_join.cast({"addressid": pl.Int32, "contactid": pl.Int32})
    lazy_df_join = df_join.lazy()

    lazy_df_join = lazy_df_join.join(dataframes['adrcrg'], on="addressid", how="left").unique()
    print("Left Join: + adrcrg")
    lazy_df_join = lazy_df_join.join(dataframes['cntcrg'], on="contactid", how="left").unique()
    print("Left Join: + cntcrg")
    lazy_df_join = lazy_df_join.join(dataframes['chd'], on="childid", how="left").unique()
    print("Left Join: + chd")

    df_join = lazy_df_join.collect(streaming=True)
    print(df_join)

    join_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_join.parquet"
    wr.s3.to_parquet(df=df_join.to_pandas(), path=join_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote warehouse_db_join data to S3")

