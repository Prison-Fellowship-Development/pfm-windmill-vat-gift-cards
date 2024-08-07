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

    # QUERY OPTIMIZATION STEP 1: MATERIALIZE QUERIES (cached precomputed views)
    # materialize sql queries for inner join -----------------------------
    org_query = '''SELECT org.orgid::INTEGER
                FROM r_ab.tblorg AS org
                WHERE org.isnational = 1''' 
                #join with orgprg on orgid
    orgprg_query = '''SELECT orgprg.orgid::INTEGER, orgprg.programid::INTEGER, orgprg.orgprogramid::INTEGER
                FROM r_ab.tblorgprogram AS orgprg''' 
                #org ...
                #join with (prg & prsprg) on programid
                #join with orgprgchdj on orgprogramid        
    prg_query = '''SELECT prg.programid::INTEGER, prg.programyear::INTEGER AS program_year
                FROM r_ab.tblprogram AS prg'''
                #orgprg ...
    prsprg_query = '''SELECT prsprg.programid::INTEGER 
                FROM r_ab.tblprisonprogram AS prsprg'''
                #orgprg ...
    orgprgchdj_query = '''SELECT orgprgchdj.orgprogramid::INTEGER, orgprgchdj.applicationid::INTEGER, orgprgchdj.childid::INTEGER
                FROM r_ab.tblorgprogramchildjoin AS orgprgchdj'''
                #orgprg ...
                #join with inmapp on applicationid
                #join with inmappchdj on childid
    inmapp_query = '''SELECT inmapp.applicationid::INTEGER, inmapp.caregiverid::INTEGER, inmapp.inmateid::INTEGER
                FROM r_ab.tblinmateapp AS inmapp
                WHERE inmapp.statusid = 1'''
                #orgprgchdj ...
                #join with inmappchdj on applicationid
                #join with inm on inmateid
                #join with crg on caregiverid
    inmappchdj_query = '''SELECT inmappchdj.applicationid::INTEGER, inmappchdj.childid::INTEGER,
                    inmappchdj.message AS message_to_child,
                    inmappchdj.parentnickname AS parent_nickname
                FROM r_ab.tblinmateappchildjoin AS inmappchdj
                WHERE inmappchdj.childstatusid = 1'''
                #orgprgchdj ...
                #inmapp ...
                #join with chd on childid
    inm_query = '''SELECT inm.inmateid::INTEGER, inm.prisonid::INTEGER,
                    inm.firstname AS inmate_first_name, 
                    inm.lastname AS inmate_last_name
                FROM r_ab.tblinmate AS inm'''
                #inmapp ...
                #join with prs on prisonid
    prs_query = '''SELECT prs.prisonid::INTEGER
                FROM r_ab.tblprison AS prs'''
                #inm ...
    # materialize sql queries for left join ----------------------------- 
    crg_query = '''SELECT crg.caregiverid::INTEGER, crg.addressid::INTEGER, crg.contactid::INTEGER,
                    crg.firstname AS caregiver_first_name, 
                    crg.lastname AS caregiver_last_name
                FROM r_ab.tblCaregiver AS crg''' 
                #inmapp...
                #join with adrcrg on addressid
                #join with cntcrg on contactid
    adrcrg_query = '''SELECT adrcrg.addressid::INTEGER, 
                    adrcrg.address1 AS caregiver_address1,
                    adrcrg.address2 AS caregiver_address2,
	                adrcrg.city AS caregiver_city,
	                adrcrg.stateid AS caregiver_stateid,
	                adrcrg.zipcode AS caregiver_zipcode
                FROM r_ab.tblAddressCaregiver AS adrcrg''' 
                #crg...
    cntcrg_query = '''SELECT cntcrg.contactid::INTEGER,
                    cntcrg.email AS caregiver_email,
	                cntcrg.phone1 AS caregiver_phone
                FROM r_ab.tblContactCaregiver AS cntcrg''' 
                #crg...
    chd_query = '''SELECT chd.childid::INTEGER,
                    chd.firstname AS child_first_name,
	                chd.lastname AS child_last_name
                FROM r_ab.tblChild AS chd
                WHERE chd.status = 1''' 
                #inmappchdj...

    # convert queries to polar dataframes
    # queries for inner joins -----------------------------
    df_org = pl.read_database_uri(query=org_query, uri=uri).to_pandas()
    print("built df_org")
    df_orgprg = pl.read_database_uri(query=orgprg_query, uri=uri).to_pandas()
    print("built df_orgprg")
    df_prg = pl.read_database_uri(query=prg_query, uri=uri).to_pandas()
    print("built df_prg")
    df_prsprg = pl.read_database_uri(query=prsprg_query, uri=uri).to_pandas()
    print("built df_prsprg")
    df_orgprgchdj = pl.read_database_uri(query=orgprgchdj_query, uri=uri)
    print("built df_orgprgchdj")

    # cast applicationid to i32
    df_orgprgchdj = df_orgprgchdj.cast({"applicationid": pl.Int32}).to_pandas()

    df_inmapp = pl.read_database_uri(query=inmapp_query, uri=uri).to_pandas()
    print("built df_inmapp")
    df_inmappchdj = pl.read_database_uri(query=inmappchdj_query, uri=uri).to_pandas()
    print("built df_inmappchdj")
    df_inm = pl.read_database_uri(query=inm_query, uri=uri).to_pandas()
    print("built df_inm")
    df_prs = pl.read_database_uri(query=prs_query, uri=uri).to_pandas()
    print("built df_prs")
    # queries for left joins -----------------------------
    df_crg = pl.read_database_uri(query=crg_query, uri=uri).to_pandas()
    print("built df_crg")
    df_adrcrg = pl.read_database_uri(query=adrcrg_query, uri=uri).to_pandas()
    print("built df_adrcrg")
    df_cntcrg = pl.read_database_uri(query=cntcrg_query, uri=uri).to_pandas()
    print("built df_cntcrg")
    df_chd = pl.read_database_uri(query=chd_query, uri=uri).to_pandas()
    print("built df_chd")
    print("")

    # CREATE S3 Connection
    # initiate s3 connection
    s3 = boto3.Session(
        region_name='us-east-2',
        aws_access_key_id = wmill.get_variable("u/joshua_chuang/s3_key"),
        aws_secret_access_key=wmill.get_variable("u/joshua_chuang/s3_secret")
        )
    # set s3 bucket name
    bucket = wmill.get_variable("u/joshua_chuang/s3_bucket_name")

    # set file path on s3 bucket to store files
    # file paths for mat queries to inner join -----------------------------
    org_s3_path = f"s3://{bucket}/warehouse_db_query_mat/org_query.parquet"
    orgprg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/orgprg_query.parquet"
    prg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/prg_query.parquet"
    prsprg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/prsprg_query.parquet"
    orgprgchdj_s3_path = f"s3://{bucket}/warehouse_db_query_mat/orgprgchdj_query.parquet"
    inmapp_s3_path = f"s3://{bucket}/warehouse_db_query_mat/inmapp_query.parquet"
    inmappchdj_s3_path = f"s3://{bucket}/warehouse_db_query_mat/inmappchdj_query.parquet"
    inm_s3_path = f"s3://{bucket}/warehouse_db_query_mat/inm_query.parquet"
    prs_s3_path = f"s3://{bucket}/warehouse_db_query_mat/prs_query.parquet"
    # file paths for mat queries to left join -----------------------------
    crg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/crg_query.parquet"
    adrcrg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/adrcrg_query.parquet"
    cntcrg_s3_path = f"s3://{bucket}/warehouse_db_query_mat/cntcrg_query.parquet"
    chd_s3_path = f"s3://{bucket}/warehouse_db_query_mat/chd_query.parquet"

    # write dataframes to parquet files on the s3 bucket
    # writes for mat queries to inner join -----------------------------
    wr.s3.to_parquet(df=df_org, path=org_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote org data to S3")
    wr.s3.to_parquet(df=df_orgprg, path=orgprg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote orgprg data to S3")
    wr.s3.to_parquet(df=df_prg, path=prg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote prg data to S3")
    wr.s3.to_parquet(df=df_prsprg, path=prsprg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote prsprg data to S3")
    wr.s3.to_parquet(df=df_orgprgchdj, path=orgprgchdj_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote orgprgchdj data to S3")
    wr.s3.to_parquet(df=df_inmapp, path=inmapp_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote inmapp data to S3")
    wr.s3.to_parquet(df=df_inmappchdj, path=inmappchdj_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote inmappchdj data to S3")
    wr.s3.to_parquet(df=df_inm, path=inm_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote inm data to S3")
    wr.s3.to_parquet(df=df_prs, path=prs_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote prs data to S3")
    # writes for mat queries to left join -----------------------------
    wr.s3.to_parquet(df=df_crg, path=crg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote crg data to S3")
    wr.s3.to_parquet(df=df_adrcrg, path=adrcrg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote adrcrg data to S3")
    wr.s3.to_parquet(df=df_cntcrg, path=cntcrg_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote cntcrg data to S3")
    wr.s3.to_parquet(df=df_chd, path=chd_s3_path, dataset=False, boto3_session=s3)
    print("Successfully wrote chd data to S3")


