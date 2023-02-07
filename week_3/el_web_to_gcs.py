import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def write_local(url: str, file_name: str) -> Path:
    '''Write file to local disk'''
    path = Path(f'./ny_taxi/fhv/{file_name}.csv.gz')
    os.system(f'wget {url} -O {path}')
    return path

@task()
def write_gcs(path: Path) -> Path:
    '''Upload parquet file to GCS'''
    gcp_block = GcsBucket.load("datazoomcamp-prefect-gsc")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return path

@task()
def remove_local(path: Path) -> None:
    '''Delete file from local disk'''
    os.remove(path)
    return

@flow()
def el_web_to_gcs(year: int, month: int) -> None:
    '''ETL from web to GCS'''
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz'
    file_name = f'fhv_tripdata_{year}_{month:02}'

    path = write_local(url, file_name)
    path = write_gcs(path)
    remove_local(path)

@flow
def el_multi_web_to_gcs(years: list[int]):
    for year in years:
        for month in range(12):
            el_web_to_gcs(year, month + 1)

if __name__ == '__main__':
    years = [2019]
    el_multi_web_to_gcs(years)

