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
    filename='./brand_review_analysis/brand_review_project/logs_own_tweets_etl.log',
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
# CONSUMER_KEY = config('CONSUMER_KEY')
# CONSUMER_SECRET = config('CONSUMER_SECRET')
# ACCESS_TOKEN = config('ACCESS_TOKEN')
# ACCESS_TOKEN_SECRET = config('ACCESS_TOKEN_SECRET')
BEARER_TOKEN = config('BEARER_TOKEN')
USER_ID = config('USER_ID')

# connect API
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
    return df_result


if __name__ == "__main__":
    logging.info('About to start the etl step of the system')
    df_result = bring_own_tweets()

    logging.info('Done executing the etl step')