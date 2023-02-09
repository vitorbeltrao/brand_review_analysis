'''
Author: Vitor Abdo

File to call the twitter API and fetch the data
'''

# import necessay packages
import logging
import pandas as pd
import tweepy as tw
from datetime import timedelta

def bring_own_tweets(bearer_token: str, user_id: str) -> pd.DataFrame:
    '''Function to make a request on the twitter API and bring
    the necessary data related to a specific user's page

    :param bearer_token: (str)
    The bearer token of your twitter development account

    :param user_id: (str)
    The user id of the page you want to extract twitter data from

    :return: (dataframe)
    Pandas dataframe transformed
    '''
    # connect twitter API
    client = tw.Client(bearer_token=bearer_token)

    # empty lists are created that will store the data extracted from the API
    id = []
    date = []
    text = []
    retweets = []
    answers = []
    likes = []
    mentions = []

    # API call script with loop and pagination
    for tweet in tw.Paginator(
            client.get_users_tweets, id=user_id, exclude=[
                'retweets', 'replies'],  # start_time = '2023-01-01T00:00:00Z',
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
            'tweet_id': id, 'date': date, 'text': text, 'likes': likes,
            'retweets': retweets, 'answers': answers, 'mentions': mentions
        }

    df_result = pd.DataFrame(results)

    # some transformations
    df_result['tweet_id'] = df_result['tweet_id'].astype('str')
    df_result['date'] = pd.to_datetime(df_result['date']) - timedelta(hours=3)

    logging.info('Dataframe creation: SUCCESS')
    return df_result
