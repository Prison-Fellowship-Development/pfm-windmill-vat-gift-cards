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

    # get joined parquet
    join_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_join.parquet"
    lazy_df_join = pl.scan_parquet(join_s3_path, storage_options=storage_options)

    # normalize caregivers table
    df_caregiver = (
        lazy_df_join
        .filter(pl.col("program_year") == 2023)
        .select(
            [
                "caregiverid",
                "caregiver_first_name",
                "caregiver_last_name",
                "caregiver_address1",
                "caregiver_address2",
                "caregiver_city",
                "caregiver_stateid",
                "caregiver_zipcode",
                "caregiver_email",
                "caregiver_phone",
                "program_year",
            ]
        )
        .unique()
        .collect()
    )

    caregiver_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_caregiver.parquet"
    wr.s3.to_parquet(
        df=df_caregiver.to_pandas(),
        path=caregiver_s3_path,
        dataset=False,
        boto3_session=s3,
    )
    print("Successfully wrote caregivers data to S3")

    # normalize children table
    df_children = (
        lazy_df_join
        .filter(pl.col("program_year") == 2023)
        .select(
            [
                "childid",
                "child_first_name",
                "child_last_name",
                "program_year",
            ]
        )
        .unique()
        .collect()
    )

    children_s3_path = (
        f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_children.parquet"
    )
    wr.s3.to_parquet(
        df=df_children.to_pandas(),
        path=children_s3_path,
        dataset=False,
        boto3_session=s3,
    )
    print("Successfully wrote children data to S3")

    # TODO: Add inmate first name, last name
    # normalize inmate table
    df_inmate = (
        lazy_df_join
        .filter(pl.col("program_year") == 2023)
        .select(
            [
                "inmateid",
                "prisonid",
                "inmate_first_name",
                "inmate_last_name",
                "program_year",
            ]
        )
        .unique()
        .collect()
    )

    inmate_s3_path = (
        f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_inmate.parquet"
    )
    wr.s3.to_parquet(
        df=df_inmate.to_pandas(), path=inmate_s3_path, dataset=False, boto3_session=s3
    )
    print("Successfully wrote inmate data to S3")

    # create caregiver to children relationship table
    df_caregiver_to_children = (
        lazy_df_join
        .filter(pl.col("program_year") == 2023)
        .select(
            [
                "caregiverid",
                "caregiver_first_name",
                "caregiver_last_name",
                "childid",
                "child_first_name",
                "child_last_name",
                "program_year",
            ]
        )
        .unique()
        .collect()
        .sort("childid", "child_first_name", "child_last_name")
    )

    caregiver_to_children_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_caregiver_to_children.parquet"
    wr.s3.to_parquet(
        df=df_caregiver_to_children.to_pandas(),
        path=caregiver_to_children_s3_path,
        dataset=False,
        boto3_session=s3,
    )
    print("Successfully wrote caregiver to children relationship data to S3")

    # create children to inmate parent relationship table
    df_children_to_inmate = (
        lazy_df_join
        .filter(pl.col("program_year") == 2023)
        .select(
            [
                "childid",
                "child_first_name",
                "child_last_name",
                "inmateid",
                "inmate_first_name",
                "inmate_last_name",
                "message_to_child",
                "program_year",
            ]
        )
        .unique()
        .collect()
    )

    children_to_inmate_s3_path = f"s3://{bucket}/warehouse_db_query_mat/warehouse_db_normalized_children_to_inmate.parquet"
    wr.s3.to_parquet(
        df=df_children_to_inmate.to_pandas(),
        path=children_to_inmate_s3_path,
        dataset=False,
        boto3_session=s3,
    )
    print("Successfully wrote children to inmate parent relationship data to S3")
