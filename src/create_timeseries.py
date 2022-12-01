import requests
import os
import time
import pathlib
import pandas as pd
import numpy as np


def auth():
    return os.getenv('TWITTERTOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(tweet_ids):
    tweet_fields = "tweet.fields=id,created_at"
    ids = f"ids={tweet_ids}"
    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
    return url


def connect_to_endpoint(url, headers):
    try:
        response = requests.request("GET", url, headers = headers)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        
    except Exception as e:
        x = 5*60
        print(e)
        print(f"request limit exceeded, pausing for {x} seconds")
        time.sleep(x)
        connect_to_endpoint(url, headers)
    return response.json()


def get_df(json_response):
    for tweet in json_response['data']:
        tweet_id = tweet['id']
        created_at = tweet['created_at']
        new_data = pd.DataFrame({
            "id":tweet_id,
            "created_at": created_at}, index=[0])
        all_data = pd.concat([new_data], ignore_index=True)
    return all_data

def get_movie_dirs(movies_path):

    list_of_movies = [f.path for f in os.scandir(movies_path) if f.is_dir()]

    return list_of_movies

def get_sentiment_file(directory):

    sentiment_file = ""

    for file in os.listdir(directory):

        if file.endswith("sentiment.csv") and not "subset" in file and not "date" in file:
            sentiment_file = os.path.join(directory, file)

    return sentiment_file

def get_json(url, headers):
    try:
        json_response = connect_to_endpoint(url, headers)
        if "status" in json_response:
            print(json_response)
            if json_response.status == 429:
                x = 5*60
                print(f"request limit exceeded, pausing for {x} seconds")
                time.sleep(x)
                json_response = connect_to_endpoint(url, headers)
    except Exception as e:
        x = 5*60
        print(e)
        print(f"request limit exceeded, pausing for {x} seconds")
        time.sleep(x)
        json_response = connect_to_endpoint(url, headers)
    return json_response



def main():
    bearer_token = auth()
    headers = create_headers(bearer_token)
    
    cwd = pathlib.Path().resolve()
    movies_path = os.path.abspath(
        os.path.join(cwd, "data/twitter")
    )
    # get list of directories for movies
    movies_list = get_movie_dirs(movies_path)

    counter = 0
    for movie in movies_list:
        if counter >= 299:
            x = 15*60
            print(f"Pausing for {x} seconds after 300 requests")
            time.sleep(x)
            counter = 0
        print(f"running for {movie}")

        # get sentiment csv file
        sentiment_file = get_sentiment_file(movie)
        sentiment_df = pd.read_csv(sentiment_file, sep="\t")
        
        length = len(sentiment_df.index)
        # adding column "date"
        sentiment_df = sentiment_df.assign(date=pd.Series(np.zeros(length)).values)

        # somehow we have na values?
        sentiment_df = sentiment_df[sentiment_df['id'].notna()]

        # These ids cannot be found anymore and break the search for all 100 in their respective query, probably the tweets got deleted
        failing_ids = [1514335562576052224, 1534314350307069952]
        sentiment_df = sentiment_df[~sentiment_df['id'].isin(failing_ids)]

        # looping over df in steps of 100, getting 100 ids and sending those to api
        total = 0
        for i in range(0, length, 99):
            j = min(length, i+99)
            df = sentiment_df.loc[i:j, :]

            ids = df['id']
            ids_list = ids.to_list()
            # print(f"Looking for {len(ids_list)} ids (should be 100)")
            total += len(ids_list)
            if len(ids_list) == 0:
                print(f"no ids left for {sentiment_file} df[{i}:{j}]")
                break   
            ids_string = ','.join(map(str, map(int, ids_list)))
            url = create_url(ids_string)

            # Catching only exception for too many requests in connect_to_endpoint
            json_response = get_json(url, headers)


            # using try except block here in case no data is returned, maybe better way to handle it?
            try:
                for tweet in json_response['data']:
                    tweet_id = tweet['id']
                    date = tweet['created_at'].split('T')[0]
                    sentiment_df.loc[sentiment_df['id'].astype(float) == float(tweet_id), 'date'] = date
            except KeyError:
                print("no tweets returned")
                print(json_response)
            except Exception as e:
                print(e)
            
            counter += 100

        print(f"Looked for {total} tweets")
        
        file_name = pathlib.Path(sentiment_file).stem
        out_file_name = f"{file_name}_date.csv"
        out_file_path = os.path.join(movie, out_file_name)
        sentiment_df.to_csv(out_file_path, sep="\t", encoding="utf-8", index=False)
        print(f"saved to {out_file_path}")

if __name__ == "__main__":
    main()
    