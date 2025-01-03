import os
import pathlib

from modules.GCStorage import GStorage, get_gclient

from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/raw')
treated_files_folder = working_dir.joinpath('data/treated')

#Gettig files, transforming them into Dataframe, saving
file_path_list = []

for file in files_folder.iterdir():
    if file.is_file():
        file_path_list.append(file)

df_read_csv = (
    pd.read_csv(files, encoding='utf8') 
    for files in file_path_list
)

df = (
    pd.concat(df_read_csv, ignore_index=True) 
    if len(file_path_list) > 1 else file_path_list[0]
)

def treat_data(df):
    try:
        print('Treating data...')
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9_]", "_", regex=True)

        )
        df = df.fillna({
            col: "N/A" if df[col].dtype == "object" else 0
            for col in df.columns
        })
    except Exception as e:
        print(e)

    return df

def save_to_parquet(df):
    try:
        print('saving df to parquet on folder')
        df.to_parquet(
            treated_files_folder.joinpath('table.parquet'),
            compression = None
        )
        return print(f'File saved on {treated_files_folder} with success')
    
    except Exception as e:
        print(e)

treat_data(df)
save_to_parquet(df)

#up to gcloud
storage_client = get_gclient()
gcs = GStorage(storage_client)
bucket_name = 'blackstone-churn'

try:
    if not bucket_name in gcs.list_buckets():
        bucket_gcs = gcs.create_bucket('blackstone-churn', storage_class='STANDARD')
    else:
        bucket_gcs = gcs.get_bucket(bucket_name)

    for file_path in treated_files_folder.glob('*.*'):
        gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    
    print(f'upload of {file_path.name} was done!')

except Exception as e:
    print(e)