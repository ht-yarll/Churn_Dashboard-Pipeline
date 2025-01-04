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

  