import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read taxi dataset from web into pandas dataframe'''
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    '''Fix dytpe'''
    if 'lpep_pickup_datetime' in df.columns:
        df = df.rename(columns={'lpep_pickup_datetime': 'tpep_pickup_datetime'})
    if 'lpep_dropoff_datetime' in df.columns:
        df = df.rename(columns={'lpep_dropoff_datetime': 'tpep_dropoff_datetime'})
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
def etl_web_to_gcs() -> None:
    '''ETL from web to GCS'''
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
