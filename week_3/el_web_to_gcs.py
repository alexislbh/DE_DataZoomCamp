import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def write_local(url: str, color: str, file_name: str) -> Path:
    '''Write file to local disk'''
    path = Path(f'./week_3/ny_taxi/{color}/{file_name}.csv.gz')
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
def el_web_to_gcs(color: str, year: int, month: int) -> None:
    '''ETL from web to GCS'''
    #url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz'
    #file_name = f'fhv_tripdata_{year}_{month:02}'
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz'
    file_name = f'{color}_tripdata_{year}_{month:02}'

    path = write_local(url, color, file_name)
    path = write_gcs(path)
    remove_local(path)

@flow
def el_multi_web_to_gcs(colors: list[str], years: list[int]):
    for color in colors:
        for year in years:
            for month in range(12):
                el_web_to_gcs(color, year, month + 1)

if __name__ == '__main__':
    colors = ['green', 'yellow']
    years = [2019, 2020]
    el_multi_web_to_gcs(colors, years)

