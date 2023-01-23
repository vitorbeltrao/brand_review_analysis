'''
'''
# import necessay packages
import logging
import os
import pandas as pd
import tweepy as tw
from decouple import config
from google.cloud import storage


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
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config('JSON_KEY')


# connect twitter API
client = tw.Client(bearer_token=BEARER_TOKEN)


def bring_own_tweets() -> pd.DataFrame:
    '''
    '''
    # empty lists are created that will store the data extracted from the API call
    id = []
    date = []
    text = []
    retweets = []
    answers = []
    likes = []
    mentions = []

    # API call script with loop and pagination
    for tweet in tw.Paginator(
        client.get_users_tweets, id=USER_ID, exclude=['retweets', 'replies'], #start_time = '2023-01-01T00:00:00Z',
        tweet_fields=['id', 'text', 'created_at', 'public_metrics'], max_results=100).flatten(limit=3200):

        # inserting the results in the lists
        id.append(tweet['id'])
        date.append(tweet['created_at'])
        text.append(tweet['text'])
        retweets.append(tweet['public_metrics']['retweet_count'])
        answers.append(tweet['public_metrics']['reply_count'])
        likes.append(tweet['public_metrics']['like_count'])
        mentions.append(tweet['public_metrics']['quote_count'])

    # create dataframe
        results = {
        'tweet_id':id, 'date':date, 'text':text, 'likes':likes, 
        'retweets':retweets, 'answers':answers, 'mentions':mentions
        }

    df_result = pd.DataFrame(results)
    df_result['tweet_id'] = df_result['tweet_id'].astype('str')
    logging.info('Dataframe creation: SUCCESS')
    return df_result


def upload_to_storage(
    bucket_name: str, data: pd.DataFrame, destination_blob_path: str) -> None:
    '''
    '''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_path)

        if blob.exists() is False:
            blob.upload_from_string(data.to_parquet())
            logging.info('Loading the NEW PARQUET FILE into the bucket: SUCCESS')
            return None

        else:
            blob.download_to_filename('atletico_own_tweets.parquet')
            df_temp = pd.read_parquet('atletico_own_tweets.parquet')
            df_final = pd.concat([data, df_temp])
            df_final.drop_duplicates(subset=['tweet_id'], keep='first', inplace=True, ignore_index=True)
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


if __name__ == "__main__":
    logging.info('About to start the etl step of the system')
    df_result = bring_own_tweets()
    upload_to_storage(BUCKET_NAME, df_result, DESTINATION_BLOB_PATH)
    logging.info('Done executing the etl step')