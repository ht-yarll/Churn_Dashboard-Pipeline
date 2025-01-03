import os
import pathlib

from modules.GCStorage import GStorage, get_gclient

from google.cloud import bigquery
from google.cloud import storage
import pandas as pd


working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/raw')
treated_files_folder = working_dir.joinpath('data/treated')


#Gettig files and transforming them into Dataframe
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