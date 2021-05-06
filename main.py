from prefect import task, Flow
import requests
import json
import datetime
import pandas as pd
import snowflake
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    account=os.getenv("DB_NAME"),
    warehouse="COMPUTE_WH",
    database="PYTHONATHON",
    schema="PREFECTDB",
)

conn.cursor().execute(
    "CREATE TABLE IF NOT EXISTS "
    "bikes( "
    "is_installed integer, "
    "num_bikes_available integer, "
    "last_reported integer, "
    "is_renting integer, "
    "eightd_has_available_keys boolean, "
    "num_docks_available integer, "
    "num_docks_disabled integer, "
    "is_returning integer, "
    "station_id string, "
    "num_ebikes_available integer, "
    "num_bikes_disabled integer, "
    "time timestamp_ltz "
    ")"
)


@task
def feeds_urls(url: str, lang: str):
    try:
        feeds = json.loads(requests.get(url).content)
        return pd.json_normalize(
            feeds, record_path=["data", lang, "feeds"], meta="last_updated"
        )
    except:
        print("feed retrieval failed")


@task
def get_station_status(feeds):
    return feeds.loc[feeds["name"] == "station_status"]["url"].iloc[0]


@task
def get_data(url: str):
    try:
        dat = json.loads(requests.get(url).content)
        return pd.json_normalize(
            dat, record_path=["data", "stations"], meta=["last_updated", "ttl"]
        )
    except:
        print("data retrieval failed")


@task
def write_to_db(con, dframe):
    write_pandas(con, dframe, "BIKES")


@task
def append_time(df):
    df["time"] = df.last_updated.apply(lambda x: datetime.datetime.fromtimestamp(x))
    return df


# SCREAMING VARIABLE NAMES, THANKS SNOWFLAKE
@task
def case_to_upper(df):
    df.columns = df.columns.str.upper()
    return df


with Flow("get bike data") as flow:
    feeds = feeds_urls("https://gbfs.capitalbikeshare.com/gbfs/gbfs.json", "en")
    station_status = get_station_status(feeds)
    dat = get_data(station_status)
    df = append_time(dat)
    df = case_to_upper(df)
    df = df[
        [
            "IS_INSTALLED",
            "NUM_BIKES_AVAILABLE",
            "LAST_REPORTED",
            "IS_RENTING",
            "EIGHTD_HAS_AVAILABLE_KEYS",
            "NUM_DOCKS_AVAILABLE",
            "NUM_DOCKS_DISABLED",
            "IS_RETURNING",
            "STATION_ID",
            "NUM_EBIKES_AVAILABLE",
            "NUM_BIKES_DISABLED",
            "TIME",
        ]
    ]
    write_to_db(conn, df)

state = flow.run()
