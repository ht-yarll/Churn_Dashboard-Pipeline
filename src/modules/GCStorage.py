import os
import pathlib
import mimetypes

from google.cloud import storage

def get_gclient():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
        '/home/ht-yarll/Documents/keys/ht-churn-bstone.json'
        )
    return storage.Client()

STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')

class GStorage:

    STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')

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

# workin_dir = pathlib.Path.cwd()
# files_folder = workin_dir.joinpath('data/raw')
# downloads_folder = workin_dir.joinpath('data/download')
# bucket_name = 'demo_bucket_ht'

# storage_client = get_gclient()
# gcs = GStorage(storage_client)

# if not bucket_name in gcs.list_buckets():
#     bucket_gcs = gcs.create_bucket('demo_bucket_ht', STORAGE_CLASSES[0])
# else:
#     bucket_gcs = gcs.get_bucket(bucket_name)

# for file_path in files_folder.glob('*.*'):
#     gcs.upload_file(bucket_gcs, 'without extensiion/' + file_path.stem, str(file_path))

#     gcs.upload_file(bucket_gcs, file_path.name, str(file_path))

# gcs_demo_blobs = gcs.list_blobs('demo_bucket_ht')

# for blob in gcs_demo_blobs:
#     path_download = downloads_folder.joinpath(blob.name)
#     if not path_download.parent.exists():
#         path_download.parent.mkdir(parents=True)
#     blob.download_to_filename(str(path_download))
#     blob.delete()