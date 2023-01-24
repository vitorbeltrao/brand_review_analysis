'''
Author: Vitor Abdo

File to upload .parquet files to a google storage bucket
'''

# import necessay packages
import logging
import pandas as pd
from google.cloud import storage

def upload_to_storage(
        bucket_name: str,
        data: pd.DataFrame,
        destination_blob_path: str,
        download_file: str) -> None:
    '''Function that uploads a dataframe into a google storage bucket

    :param bucket_name: (str)
    Name of the respective bucket

    :param data: (dataframe)
    Dataframe you want to upload

    :param destination_blob_path: (str)
    destination path of the file you want to upload to the google storage bucket

    :param download_file: (str)
    Name of the file you want to download from your google storage bucket
    '''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_path)

        if blob.exists() is False:
            blob.upload_from_string(data.to_parquet())
            logging.info(
                'Loading the NEW PARQUET FILE into the bucket: SUCCESS')
            return None

        else:
            blob.download_to_filename(download_file)
            df_temp = pd.read_parquet(download_file)
            df_final = pd.concat([data, df_temp])
            df_final.drop_duplicates(
                subset=['tweet_id'],
                keep='first',
                inplace=True,
                ignore_index=True)
            blob.upload_from_string(data.to_parquet())
            logging.info('Loading the NEW ROWS into the bucket: SUCCESS')
            return None

    except TypeError:
        logging.error('Your file type does not look correct')
        return None

    except ValueError:
        logging.error(
            'You are performing an input or output operations on the file that is already closed')
        return None