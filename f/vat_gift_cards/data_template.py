import os
import wmill
import boto3
import polars as pl
import pandas as pd
import awswrangler as wr


def main():

    #CREATE S3 Connection
    
    #initiate s3 connection
    s3 = boto3.Session(
        region_name='us-east-2',
        aws_access_key_id = wmill.get_variable("u/david_fulton/s3_key"),
        aws_secret_access_key=wmill.get_variable("u/david_fulton/s3_secret")
        )

    #set s3 bucket name
    bucket = wmill.get_variable("u/david_fulton/s3_bucket_name")

    #read s3 file
    read_file = wr.s3.read_parquet(path=f's3://{bucket}/folder/file_name', boto3_session = s3)

    #set file path on s3 bucket to store files
    s3_path = f"s3://{bucket}/folder/file_name"

    #write dataframes to parquet files on the s3 bucket
    wr.s3.to_parquet(df=[dataframe], path=s3_path, dataset=False, boto3_session=s3)