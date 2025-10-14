#!/usr/bin/env python
# coding: utf-8


from bs4 import BeautifulSoup
from typing import Iterator
import argparse
import logging
import oracledb
import requests
import pandas as pd
from sqlalchemy import create_engine


def get_urls(url: str) -> Iterator[str]:
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a")
        for link in links:
            href = link.get("href")
            if (
                href
                and "2024" in href
                and "tripdata" in href
                # and "yellow_tripdata" not in href
            ):
                if href.startswith("http"):
                    yield href.rstrip(" ")
    else:
        logging.error(
            f"Failed to retrieve the webpage. Status code: {response.status_code}"
        )


def download_file(url: str) -> str:
    """
    Downloads a file from a URL and returns the local file path.

    Args:
        url (str): The URL of the file to download.

    Returns:
        str: The local file path of the downloaded file.
    """
    # Choose filename based on URL
    if url.endswith(".csv.gz"):
        out_name = "output.csv.gz"
    elif url.endswith(".parquet"):
        out_name = "output.parquet"
    else:
        out_name = "output.csv"

    logging.info("downloading %s to %s", url, out_name)
    # Stream the download to avoid loading the whole file into memory
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(out_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    logging.info("download complete: %s", out_name)
    return out_name


def read_data(file_path: str) -> pd.DataFrame:
    """
    Reads data from a file into a pandas DataFrame.

    Args:
        file_path (str): The path to the file.

    Returns:
        pd.DataFrame: The data from the file as a pandas DataFrame.
    """
    if file_path.endswith(".parquet"):
        df = pd.read_parquet(file_path)
    elif file_path.endswith(".csv") or file_path.endswith(".csv.gz"):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file format")
    # Convert columns with 'datetime' in name to datetime
    for col in df.columns:
        if "datetime" in col:
            df[col] = pd.to_datetime(df[col])
    df.columns = [col.upper() for col in df.columns]
    return df


def create_db_engine(user, password, host, port, db):
    """
    Creates a OracleDB engine.
    """
    dsn = f"{host}:{port}/{db}"
    connection = oracledb.connect(user=user, password=password, dsn=dsn)
    connection.autocommit = True
    engine = create_engine('oracle+oracledb://', creator=lambda: connection)
    return engine


def get_data_iterator(df: pd.DataFrame, batch_size: int) -> Iterator[pd.DataFrame]:
    """
    Returns an iterator that yields batches of a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to batch.
        batch_size (int): The size of each batch.

    Yields:
        Iterator[pd.DataFrame]: An iterator that yields batches of the DataFrame.
    """
    if len(df) <= batch_size:
        yield df

    else: 
        df = df.sample(batch_size).reset_index(drop=True)
        n = len(df)
        for i in range(0, n, batch_size):
            yield df.iloc[i : i + batch_size]


def ingest_data(engine, table_name: str, data_iterator: Iterator[pd.DataFrame]):
    """
    Ingests data into the database in batches using pandas to_sql.

    Args:
        engine: The SQLAlchemy engine.
        table_name (str): The name of the table to write to.
        data_iterator (Iterator[pd.DataFrame]): An iterator that yields batches of the DataFrame.
    """
    # first_batch = True
    for batch in data_iterator:
        batch = batch.astype({col: 'string' for col in batch.select_dtypes(include=['float64', 'int64']).columns})
        logging.info(f"ingesting batch of size {len(batch)} to table {table_name}")
        batch['created_at'] = pd.Timestamp.now()
        batch['updated_at'] = pd.Timestamp.now()
        batch.to_sql(table_name, engine, if_exists="append", index=False)
        break


def main(params):
    """
    Main function to orchestrate the data ingestion process.

    Args:
        params (argparse.Namespace): The command-line arguments.
    """
    logging.info("starting ingestion")
    for i, url in enumerate(get_urls(params.url)):
        if i >= params.ntables:
            break
        logging.info(f"ingesting data from {url} starting")
        file_path = download_file(url)
        engine = create_db_engine(
            params.user, params.password, params.host, params.port, params.db
        )
        df = read_data(file_path)
        data_iterator = get_data_iterator(df, params.batch_size)
        table_name = "raw_" + "_".join(url.split("/")[-1].split("_")[:2])
        ingest_data(engine, table_name, data_iterator)
        logging.info(f"ingesting data from {url} complete")
    logging.info("ingestion complete")
    logging.info(f"{i} tables ingested")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data to Oracle DB")

    parser.add_argument("--user", default="PDAVE", help="user name for oracle")
    parser.add_argument("--password", default="root", help="password for oracle")
    parser.add_argument("--host", default="localhost", help="host for oracle")
    parser.add_argument("--port", default="1521", help="port for oracle")
    parser.add_argument("--db", default="D7WOB1D1", help="database name for oracle")
    parser.add_argument(
        "--ntables", default=1, type=int, help="max number of tables to create"
    )
    parser.add_argument(
        "--url",
        default="https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        help="base url to scrape data from",
    )
    parser.add_argument(
        "--batch_size",
        required=False,
        default=100_000,
        type=int,
        help="size of the batch to insert data",
    )
    parser.add_argument(
        "--log_level",
        required=False,
        default="INFO",
        help="logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    args = parser.parse_args()

    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.log_level}")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    main(args)
