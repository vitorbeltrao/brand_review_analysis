'''
Author: Vitor Abdo

etl run file
'''
import logging
import os
from decouple import config
from bring_own_tweets import bring_own_tweets
from upload_to_storage import upload_to_storage

logging.basicConfig(
    filename='C:/Users/4YouSee/Desktop/personal_work/brand_review_analysis/brand_review_project/logs_own_tweets_etl.log',
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
BEARER_TOKEN = config('BEARER_TOKEN')
USER_ID = config('USER_ID')
BUCKET_NAME = config('BUCKET_NAME')
DESTINATION_BLOB_PATH = config('DESTINATION_BLOB_PATH')
DOWNLOAD_FILE = config('DOWNLOAD_FILE')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config('JSON_KEY')

if __name__ == "__main__":
    logging.info('About to start the etl step of the system')
    df_result = bring_own_tweets(BEARER_TOKEN, USER_ID)
    upload_to_storage(
        BUCKET_NAME,
        df_result,
        DESTINATION_BLOB_PATH,
        DOWNLOAD_FILE)
    logging.info('Done executing the etl step')
