import tweepy as tw
import pandas as pd
import json
import os
from pandas.io.json import json_normalize
from google.cloud import bigquery
from google.cloud import bigquery_storage
from datetime import datetime
import requests
import logging
from io import StringIO

def hello_world(self):
# Connecting to Twitter Developer APIs
    try: 
# Authenticating the APIs
        auth = tw.OAuthHandler(os.environ.get('api_key'), os.environ.get('api_secret'))
        auth.set_access_token(os.environ.get('access_token'),os.environ.get('access_token_secret'))
        api = tw.API(auth, wait_on_rate_limit=True)
    except tw.TweepError as e:
        print("Error authenticating Twitter API: ", e)


    try:
# Entering search keyword to pick the tweets 
        search_words = "ikea"
        date_since = "2023-01-19"

        tweets = tw.Cursor(api.search,
                       q=search_words,
                      lang="en",
                         since=date_since, exclude='retweets').items(200)

# create an empty DataFrame
        df = pd.DataFrame(columns=['tweet_id', 'text', 'created_at', 'favorite_count', 'place'])

# Iterate over tweets and add the data to the DataFrame
        for tweet in tweets:
            df = df.append({'tweet_id': str(tweet.id), 'text': str(tweet.text), 'created_at': str(tweet.created_at), 'place': str(tweet.place),
                    'favorite_count': str(tweet.favorite_count)}, ignore_index=True)

# print the DataFrame
        
        print(df)
        df = df.to_json(orient='records',date_format='iso').encode('utf-8')
        json_data = json.loads(df)
        

    
    # Loading dataframe to Bigquery
        client = bigquery.Client() #credentials=JSON_credentials
        table_id = os.environ.get('bq_table')
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("tweet_id", "STRING"),
                bigquery.SchemaField("text", "STRING"),
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("favorite_count","STRING"),
                bigquery.SchemaField("place","STRING")
                ],
                autodetect=False,
                
        )
        job = client.load_table_from_json(json_data, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
             table.num_rows, len(table.schema), table_id
            )
        )
    except Exception as e:
        print("Error loading Bq: ", e)

    return 'success'
