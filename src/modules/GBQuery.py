import os
import pathlib

from google.cloud import bigquery
import pandas as pd

def get_bqclient():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
        '/home/ht-yarll/Documents/keys/ht-churn-bstone.json'
        )
    return bigquery.Client()

working_dir = pathlib.Path.cwd()

class GBigQuery:
    def __init__(self, bigquery_client):
        self.client = bigquery_client

    def up_to_bigquery(self, file, destination_table, if_exists='replace'):

        job_config = bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.PARQUET
                                            , write_disposition = 'WRITE_TRUNCATE')
        if isinstance(file, pd.DataFrame):
            data = self.client.load_table_from_dataframe(
                file, destination_table, job_config = job_config
            )
        else:
            with open(file, 'rb') as file_obj:
                data = self.client.load_table_from_file(
                    file_obj, destination_table, job_config = job_config
                )
        data.result()
        return data
    
    def get_table(self, table_id):
        return self.client.get_table(table_id)

    def list_tables(self, data_set_id):
        tables = self.client.list_tables(data_set_id)
        return [t.table_id for t in tables]
    
    def query(self, query_string: str, 
              destination_table: str = None, 
              ):
        try:
            job_config = bigquery.QueryJobConfig(write_disposition = 'WRITE_TRUNCATE')
            if destination_table:
                print(f'Setting destination table to: {destination_table}')
                job_config.destination = destination_table

            query_job = self.client.query(query_string, job_config = job_config)
            query_job.result()
            
            print(f'Query was done!')

            if destination_table:
                print(f'Results saved to table: {destination_table}')

            return query_job
        
        except Exception as e:
            print(f'Error during query: {e}')