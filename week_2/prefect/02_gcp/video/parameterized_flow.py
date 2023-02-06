import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read taxi dataset from web into pandas dataframe'''
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    '''Fix dytpe'''
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f'Columns: {df.dtypes}')
    print(f'Rows: {len(df)}')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write DataFrame to parquet file'''
    path = Path(f'./ny_taxi/{color}/{dataset_file}.parquet') 
    df.to_parquet (path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    '''Upload parquet file to GCS'''
    gcp_block = GcsBucket.load("datazoomcamp-prefect-gsc")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    '''ETL from web to GCS'''
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    color: str = 'yellow',
    year: int = 2021,
    months: list[int] = [1, 2]
):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [1,2,3]
    etl_parent_flow(color, year, months)
