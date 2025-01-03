import os
import pathlib

from modules.GCStorage import GStorage, get_gclient

from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import pyarrow


#variables
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
        print(f'saving df to parquet on {treated_files_folder}')
        df.to_parquet(
            treated_files_folder.joinpath('table.parquet'),
            compression = None
        )
        return f'file saved on {treated_files_folder} with succes'
    
    except Exception as e:
        print(e)

treat_data(df)
save_to_parquet(df)