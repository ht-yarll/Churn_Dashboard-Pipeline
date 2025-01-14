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

df_read_csv =  list(df_read_csv)
result = df_read_csv[0]

for df in df_read_csv[1:]:
    result = pd.merge(result, df, on='customer_id')
#debug
# print("Files in folder:", file_path_list)

# df = (
#     pd.concat(df_read_csv, axis=1, verify_integrity=True) 
#     if len(file_path_list) > 1 
#     else pd.read_csv(file_path_list[0], encoding='utf8')
# )
#debug
# print("Initial DataFrame shape:", df.shape)
# print("Initial DataFrame null values:", df.isnull().sum())

treated_df = processor.treat_data(result)

#debug
# print("Treated DataFrame shape:", treated_df.shape)
# print("Treated DataFrame null values:", treated_df.isnull().sum())

df_parquet = processor.save_to_parquet(treated_df)

#debug
# print(f"Parquet saved at: {processor.output_folder.joinpath('table.parquet')}")

df = pd.read_parquet('data/treated/table.parquet')

#debug
# print("Read Parquet DataFrame shape:", df.shape)
# print("Read Parquet DataFrame null values:", df.isnull().sum())


#up to gcloud
storage_client = get_gclient()
gcs = GCStorage(storage_client)
bucket_name = 'blackstone-churn'

try:
    if not bucket_name in gcs.list_buckets():
        bucket_gcs = gcs.create_bucket('blackstone-churn', storage_class='STANDARD')
    else:
        bucket_gcs = gcs.get_bucket(bucket_name)

    for file_path in output_folder.glob('*.*'):
        gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    
    print(f'upload of {file_path.name} was done!')

except Exception as e:
    print(e)

#downloading from gcloud
gcs_demo_blobs = gcs.list_blobs(bucket_gcs)
downloads_folder = working_dir.joinpath('data/downloads')

for blob in gcs_demo_blobs:
    path_download = downloads_folder.joinpath(blob.name)
    if not path_download.parent.exists():
        path_download.parent.mkdir(parents=True)

    try:    
        blob.download_to_filename(str(path_download))
        print(f'Download of {blob.name} was done!')
        blob.delete()
        print(f'Disposing of {blob.name}')
    except Exception as e:
        print(e)

#up to bquery
bq_client = get_bqclient()
bqc = GBigQuery(bq_client)
project_id = 'blackstone-446301'
dataset_id = 'user_data'
table_name = 'churn_data'
destination_table = f'{project_id}.{dataset_id}.{table_name}'

for file in downloads_folder.iterdir():
    try:
        bqc.up_to_bigquery(file, destination_table=destination_table)
        print(f'Upload of {file.name} to BigQuery was done!')
    except Exception as e:
        print(f'Error in upload:{e}')

#query

excluded_columns = [
    'paperless_billing', 'payment_method', 'avg_monthly_long_distance_charges',
    'total_refunds', 'total_extra_data_charges', 'total_long_distance_charges',
    'gender', 'age', 'cltv', 'churn_category', 'churn_reason', 'tenure', 'internet_service_y', 
    'phone_service_y', 'multiple_lines', 'offer', 'country', 'state', 'zip_code', 
    'total_population'
    ]
table_id = 'blackstone-446301.user_data.churn_data'
table = bq_client.get_table(table_id)
selected_columns = [
    field.name for field in table.schema 
    if field.name not in excluded_columns
    ]

query_job_select = f"""
SELECT {', '.join(selected_columns)}
FROM   `{table_id}`
WHERE churn_score > 1
"""

new_table_name = 'churn_data_to_analyze'

if not new_table_name in bqc.list_tables(dataset_id):
    print(f'Creating table {new_table_name} on {dataset_id}...')
    bqc.query(
        query_job_select, 
        destination_table=f'{project_id}.{dataset_id}.{new_table_name}'
            ) 
else:
    print(f'Table {new_table_name} already exists on {dataset_id}')
    bqc.query(
        query_job_select, 
        destination_table = f'{project_id}.{dataset_id}.{new_table_name}',
        ) 
print('Process finished')