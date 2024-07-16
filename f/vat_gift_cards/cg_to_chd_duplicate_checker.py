import os
import wmill
import boto3
import polars as pl
import awswrangler as wr

pl.Config.set_streaming_chunk_size(100000)


def main():
    # CREATE S3 Connection
    s3 = boto3.Session(
        region_name="us-east-2",
        aws_access_key_id=wmill.get_variable("u/joshua_chuang/s3_key"),
        aws_secret_access_key=wmill.get_variable("u/joshua_chuang/s3_secret"),
    )
    # initiate s3 connection for polars
    storage_options = {
        "aws_access_key_id": wmill.get_variable("u/joshua_chuang/s3_key"),
        "aws_secret_access_key": wmill.get_variable("u/joshua_chuang/s3_secret"),
        "aws_region": "us-east-2",
    }
    # set s3 bucket name
    bucket = wmill.get_variable("u/joshua_chuang/s3_bucket_name")

    caregiver_to_children_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_caregiver_to_children.parquet"
    df_caregiver_to_children = pl.read_parquet(
        caregiver_to_children_s3_path, storage_options=storage_options
    )

    # Check for duplicate caregiver names
    caregiver_duplicates = (
        df_caregiver_to_children.filter(pl.col("program_year") == 2023)
        .group_by(["caregiverid", "caregiver_first_name", "caregiver_last_name"])
        .agg(pl.count("caregiverid").alias("count"))
        .filter(pl.col("count") > 1)
    )

    # Check for duplicate child names
    child_duplicates = (
        df_caregiver_to_children.filter(pl.col("program_year") == 2023)
        .group_by(["childid", "child_first_name", "child_last_name"])
        .agg(pl.count("childid").alias("count"))
        .filter(pl.col("count") > 1)
    )

    if not caregiver_duplicates.is_empty():
        print("Duplicate caregivers found:")
        print(caregiver_duplicates)
    else:
        print("No duplicate caregivers found.")

    if not child_duplicates.is_empty():
        print("Duplicate children found:")
        print(child_duplicates)
    else:
        print("No duplicate children found.")


if __name__ == "__main__":
    check_duplicates()
