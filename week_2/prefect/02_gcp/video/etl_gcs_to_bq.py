import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials  

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    '''Download parquet file from GCS'''
    gcs_path = f'ny_taxi/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("datazoomcamp-prefect-gsc")
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(f'./{gcs_path}')

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    '''Read parquet file into pandas dataframe'''
    df = pd.read_parquet(path)
    print(f'pre: missing values: {df["passenger_count"].isna().sum()}')
    df['passenger_count'].fillna(0, inplace=True)
    print(f'post: missing values: {df["passenger_count"].isna().sum()}')
    return df

@task()
def writ_bq(df: pd.DataFrame, color: str, year: int, month: int) -> None:
    '''Write DataFrame to BigQuery'''
    table = f'ny_taxi.{color}_{year}_{month:02}'
    gcp_creds_block = GcpCredentials.load("datazoomcamp-gcp-cred")
    df.to_gbq(
    destination_table = table,
    project_id = 'datazoomcamp-33300',
    credentials = gcp_creds_block.get_credentials_from_service_account(),
    chunksize = 500_000,
    if_exists = 'append',
    )
    return

@flow(log_prints=True)
def etl_gcs_to_bq() -> None:
    '''ETL from GCS to BigQuery'''
    color = 'yellow'
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    writ_bq(df, color, year, month)

if __name__ == '__main__':
    etl_gcs_to_bq()
