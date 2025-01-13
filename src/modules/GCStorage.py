import os
import mimetypes

from google.cloud import storage

def get_gclient():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
        '/home/ht-yarll/Documents/keys/ht-churn-bstone.json'
        )
    return storage.Client()

STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')

class GCStorage:

    def __init__(self, storage_client):
        self.client = storage_client

    def create_bucket(self, bucket_name, storage_class, bucket_location='US'):
        bucket = self.client.bucket(bucket_name)
        bucket.storage_class = storage_class
        return self.client.create_bucket(bucket, bucket_location)
    
    def get_bucket(self, bucket_name):
        return self.client.get_bucket(bucket_name)
    
    def list_buckets(self):
        buckets = self.client.list_buckets()
        return [bucket.name for bucket in buckets]
    
    def upload_file(self, bucket, blob_destination, file_path):
        file_type = file_path.split('.')[-1]
        if file_type == 'csv':
            content_type = 'text/csv'
        else:
            content_type = mimetypes.guess_type(file_path)[0]
        blob = bucket.blob(blob_destination)
        blob.upload_from_filename(file_path, content_type=content_type)
        return blob

    def list_blobs(self, bucket_name):
        return self.client.list_blobs(bucket_name)