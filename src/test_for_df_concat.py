import pathlib

from modules.DataProcess import DataProcessor
from modules.GCStorage import GCStorage, get_gclient
from modules.GBQuery import GBigQuery, get_bqclient


import pandas as pd

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/raw')
output_folder = working_dir.joinpath('data/treated')
processor = DataProcessor(output_path=output_folder)


# df = pd.read_csv(files_folder.joinpath('Location_Data.csv'))

# df['city'] = df['city'].str.strip()

# # Check for leading or trailing spaces in the 'city' column
# print(df['city'].head())  # Print sample values
# print(df['city'].apply(lambda x: f"'{x}'"))  # Highlight spaces in the values


file_path_list = []

for file in files_folder.iterdir():
    if file.is_file():
        file_path_list.append(file)

df_read_csv = (
    pd.read_csv(files, header=0, encoding='utf8') 
    for files in file_path_list
)

df_read_csv = list(df_read_csv)
for df in df_read_csv:
    print(df.columns)


df = (
    pd.concat(df_read_csv, axis=1) 
    if len(file_path_list) > 1 
    else pd.read_csv(file_path_list[0], encoding='utf8')
)

print(df.columns)