'''
Author: Vitor Abdo

file to get raw data from google storage and do 
some transformations to save in bigquery
'''

# import necessay packages
import logging
import os
import pandas as pd
from google.cloud import storage
from decouple import config

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
BUCKET_NAME = config('BUCKET_NAME')
DESTINATION_BLOB_PATH = config('DESTINATION_BLOB_PATH')
DOWNLOAD_FILE = config('DOWNLOAD_FILE')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config('JSON_KEY')

def transform_raw_data():
    '''
    '''
    # download the data to be transformed 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB_PATH)
    blob.download_to_filename(DOWNLOAD_FILE)
    raw_df = pd.read_parquet(DOWNLOAD_FILE)