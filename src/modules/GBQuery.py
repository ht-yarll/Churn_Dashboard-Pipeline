import os

from google.cloud import bigquery
import pandas as pd

def get_bqclient():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
         '/home/ht-yarll/Documents/keys/ht-churn-bstone.json'
        )
    return bigquery.Client()

class GBigQuery:
    def __init__(self, bigquery_client):
        self.client = bigquery_client

    def up_to_bigquery(self, file, destination_table, project_id, schema='none', if_exists='replace'):
        job_config = bigquery.LoadJobConfig()

        if schema:
             job_config.schema = [
                  bigquery.SchemaField(field['name'], 
                                       field['type'], 
                                       mode=field.get('mode', 'NULLABLE')
                                       )
                  for field in schema
             ]

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
    
    def make_query(self, query, destination_table):
            query_job = self.client.query(query)
            query_result = query_job.result()
            return query_result