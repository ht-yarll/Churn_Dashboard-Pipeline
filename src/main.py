import io
import os
import pathlib

from modules.DataProcess import DataProcessor
from modules.GCStorage import GCStorage, get_gclient
from modules.GBQuery import GBigQuery, get_bqclient

import pandas as pd

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/raw')
output_folder = working_dir.joinpath('data/treated')
processor = DataProcessor(output_path=output_folder)


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

treated_df = processor.treat_data(df)
print(treated_df.head())


# processor.save_to_parquet(treated_df)


# #up to gcloud
# storage_client = get_gclient()
# gcs = GCStorage(storage_client)
# bucket_name = 'blackstone-churn'

# try:
#     if not bucket_name in gcs.list_buckets():
#         bucket_gcs = gcs.create_bucket('blackstone-churn', storage_class='STANDARD')
#     else:
#         bucket_gcs = gcs.get_bucket(bucket_name)

#     for file_path in output_folder.glob('*.*'):
#         gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    
#     print(f'upload of {file_path.name} was done!')

# except Exception as e:
#     print(e)

# #downloading from gcloud
# gcs_demo_blobs = gcs.list_blobs(bucket_gcs)
# downloads_folder = working_dir.joinpath('data/downloads')

# for blob in gcs_demo_blobs:
#     path_download = downloads_folder.joinpath(blob.name)
#     if not path_download.parent.exists():
#         path_download.parent.mkdir(parents=True)

#     try:    
#         blob.download_to_filename(str(path_download))
#         print(f'Download of {blob.name} was done!')
#         blob.delete()
#         print(f'Disposing of {blob.name}')
#     except Exception as e:
#         print(e)

# #up to bquery
# bq_client = get_bqclient()
# bqc = GBigQuery(bq_client)
# project_id = 'blackstone-446301'
# dataset_id = 'user_data'
# table_name = 'churn_data'
# destination_table = f'{project_id}.{dataset_id}.{table_name}'

# for file in downloads_folder.iterdir():
#     try:
#         bqc.up_to_bigquery(file, destination_table=destination_table)
#         print(f'Upload of {file.name} to BigQuery was done!')
#     except Exception as e:
#         print(f'Error in upload:{e}')