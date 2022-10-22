import requests
import os
import json
import time
import pandas as pd


def auth():
    return os.getenv('TWITTERTOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results = 10):
    search_url = "https://api.twitter.com/2/tweets/search/all"
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token
    response = requests.request("GET", url, headers = headers, params = params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def append_to_df(all_data, json_response):
    for tweet in json_response['data']:
        tweet_id = tweet['id']
        author_id = tweet['author_id']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']
        reply_count = tweet['public_metrics']['reply_count']
        retweet_count = tweet['public_metrics']['retweet_count']
        text = tweet['text']
        referenced_tweets_present = False
        if 'referenced_tweets' in tweet:
            referenced_tweets_present = True
            referenced_tweet_id = tweet['referenced_tweets'][0]['id']
            referenced_tweet_type = tweet['referenced_tweets'][0]['type']

        new_data = pd.DataFrame({
            "id":tweet_id,
            "author_id":author_id,
            "like_count":like_count,
            "quote_count":quote_count,
            "reply_count":reply_count,
            "retweet_count":retweet_count,
            "referenced_tweet_id":referenced_tweet_id if referenced_tweets_present else None,
            "referenced_tweet_type":referenced_tweet_type if referenced_tweets_present else None,
            "text":text,
        }, index=[0])
        all_data = pd.concat([all_data, new_data], ignore_index=True)
    return all_data


def main():
    bearer_token = auth()
    headers = create_headers(bearer_token)
    keyword = "#TheLittleMermaid lang:en"
    start_time = "2022-01-01T00:00:00.000Z"
    end_time = "2022-10-20T00:00:00.000Z"
    max_results = 10

    count = 0
    max_count = 30
    flag = True
    next_token = None

    all_data = pd.DataFrame(columns=['id','author_id','like_count','quote_count','reply_count','retweet_count','referenced_tweet_id', 'referenced_tweet_type', 'text'])

    while flag:
        if count >= max_count:
            break
        print(f"Tweets fetched: {count}")
        url = create_url(keyword, start_time, end_time, max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            next_token = json_response['meta']['next_token']
            if result_count is not None and result_count > 0 and next_token is not None:
                all_data = append_to_df(all_data, json_response)
                count += result_count
                time.sleep(5)                
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                all_data = append_to_df(all_data, json_response)
                count += result_count
                time.sleep(5)
            
            #Since this is the final request, turn flag to false
            flag = False
            next_token = None

    #result = all_data.to_json(orient="split")
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=3, sort_keys=True))
    print(all_data)


if __name__ == "__main__":
    main()
    