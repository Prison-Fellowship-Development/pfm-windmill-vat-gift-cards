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
    # CREATE POSTGRESQL CONNECTION
    # fetch db warehouse database connection variables
    hst = wmill.get_variable("u/joshua_chuang/warehouse_hst")
    port = wmill.get_variable("u/joshua_chuang/warehouse_port")
    user = wmill.get_variable("u/joshua_chuang/warehouse_username")
    password = wmill.get_variable("u/joshua_chuang/warehouse_password")
    database = wmill.get_variable("u/joshua_chuang/warehouse_db")
    # build postgres connection string with variables
    uri = f"postgresql://{user}:{password}@{hst}:{port}/{database}"
    print("built uri")

    # CREATE S3 Connection
    # initiate s3 connection
    s3 = boto3.Session(
        region_name='us-east-2',
        aws_access_key_id = wmill.get_variable("u/joshua_chuang/s3_key"),
        aws_secret_access_key=wmill.get_variable("u/joshua_chuang/s3_secret")
        )
    # set s3 bucket name
    bucket = wmill.get_variable("u/joshua_chuang/s3_bucket_name")


    # QUERY OPTIMIZATION STEP 1: MATERIALIZE QUERIES (cached precomputed views)
    cor_sites_query = '''SELECT cs.facility_name, cs.facility_id
                FROM r_pca.correction_sites AS cs''' 
                #join with input data on facility_name
    
    survey_query = '''SELECT s.facility_id, s.survey_id
                FROM r_pca.survey AS s''' 
                #join with cor_site on facility_id

    # convert queries to polar dataframes
    cor_sites_df = pl.read_database_uri(query=cor_sites_query, uri=uri).to_pandas()
    print("built cor_sites_df")
    survey_df = pl.read_database_uri(query=survey_query, uri=uri).to_pandas()
    print("built survey_df")

    # set file path on s3 bucket to store files
    cor_sites_s3_path = f"s3://{bucket}/pca/correction_sites_query.parquet"
    survey_s3_path = f"s3://{bucket}/pca/survey_query.parquet"

    # write dataframes to parquet files on the s3 bucket
    wr.s3.to_parquet(df=cor_sites_df, path=cor_sites_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote correction_sites data to S3")
    wr.s3.to_parquet(df=survey_df, path=survey_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote survey data to S3")

    print(survey_df.head())
    print(cor_sites_df.head())
    
    