import requests
import os
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
    max_results = 500

    months = [
        #("2019-06-01T00:00:00.000Z", "2019-07-01T00:00:00.000Z"),
        #("2019-07-01T00:00:00.000Z", "2019-08-01T00:00:00.000Z"),
        ("2019-08-01T00:00:00.000Z", "2019-09-01T00:00:00.000Z"),
        ("2019-09-01T00:00:00.000Z", "2019-10-01T00:00:00.000Z"),
        ("2019-10-01T00:00:00.000Z", "2019-11-01T00:00:00.000Z"),
        ("2019-11-01T00:00:00.000Z", "2019-12-01T00:00:00.000Z"),
        ("2019-12-01T00:00:00.000Z", "2020-01-01T00:00:00.000Z"),
        ("2020-01-01T00:00:00.000Z", "2020-02-01T00:00:00.000Z"),
        ("2020-02-01T00:00:00.000Z", "2020-03-01T00:00:00.000Z"),
        ("2020-03-01T00:00:00.000Z", "2020-04-01T00:00:00.000Z"),
        ("2020-04-01T00:00:00.000Z", "2020-05-01T00:00:00.000Z"),
        ("2020-05-01T00:00:00.000Z", "2020-06-01T00:00:00.000Z"),
        ("2020-06-01T00:00:00.000Z", "2020-07-01T00:00:00.000Z"),
        ("2020-07-01T00:00:00.000Z", "2020-08-01T00:00:00.000Z"),
        ("2020-08-01T00:00:00.000Z", "2020-09-01T00:00:00.000Z"),
        ("2020-09-01T00:00:00.000Z", "2020-10-01T00:00:00.000Z"),
        ("2020-10-01T00:00:00.000Z", "2020-11-01T00:00:00.000Z"),
        ("2020-11-01T00:00:00.000Z", "2020-12-01T00:00:00.000Z"),
        ("2020-12-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z"),
        ("2021-01-01T00:00:00.000Z", "2021-02-01T00:00:00.000Z"),
        ("2021-02-01T00:00:00.000Z", "2021-03-01T00:00:00.000Z"),
        ("2021-03-01T00:00:00.000Z", "2021-04-01T00:00:00.000Z"),
        ("2021-04-01T00:00:00.000Z", "2021-05-01T00:00:00.000Z"),
        ("2021-05-01T00:00:00.000Z", "2021-06-01T00:00:00.000Z"),
        ("2021-06-01T00:00:00.000Z", "2021-07-01T00:00:00.000Z"),
        ("2021-07-01T00:00:00.000Z", "2021-08-01T00:00:00.000Z"),
        ("2021-08-01T00:00:00.000Z", "2021-09-01T00:00:00.000Z"),
        ("2021-09-01T00:00:00.000Z", "2021-10-01T00:00:00.000Z"),
        ("2021-10-01T00:00:00.000Z", "2021-11-01T00:00:00.000Z"),
        ("2021-11-01T00:00:00.000Z", "2021-12-01T00:00:00.000Z"),
        ("2021-12-01T00:00:00.000Z", "2022-01-01T00:00:00.000Z"),
        ("2022-01-01T00:00:00.000Z", "2022-02-01T00:00:00.000Z"),
        ("2022-02-01T00:00:00.000Z", "2022-03-01T00:00:00.000Z"),
        ("2022-03-01T00:00:00.000Z", "2022-04-01T00:00:00.000Z"),
        ("2022-04-01T00:00:00.000Z", "2022-05-01T00:00:00.000Z"),
        ("2022-05-01T00:00:00.000Z", "2022-06-01T00:00:00.000Z"),
        ("2022-06-01T00:00:00.000Z", "2022-07-01T00:00:00.000Z"),
        ("2022-07-01T00:00:00.000Z", "2022-08-01T00:00:00.000Z"),
        ("2022-08-01T00:00:00.000Z", "2022-09-01T00:00:00.000Z"),
        ("2022-09-01T00:00:00.000Z", "2022-10-01T00:00:00.000Z")
    ]

    for start_time, end_time in months:
        month = start_time.split("T")[0]
        count = 0
        max_count = 125000
        flag = True
        next_token = None

        all_data = pd.DataFrame(columns=['id','author_id','like_count','quote_count','reply_count','retweet_count','referenced_tweet_id', 'referenced_tweet_type', 'text'])

        while flag:
            if count >= max_count:
                break
            print(f"Tweets fetched ({month}): {count}")
            url = create_url(keyword, start_time, end_time, max_results)
            json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
            result_count = json_response['meta']['result_count']

            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
                if result_count is not None and result_count > 0 and next_token is not None:
                    all_data = append_to_df(all_data, json_response)
                    count += result_count
                    time.sleep(1.1)                
            # If no next token exists
            else:
                if result_count is not None and result_count > 0:
                    all_data = append_to_df(all_data, json_response)
                    count += result_count
                    time.sleep(1.1)
                
                #Since this is the final request, turn flag to false
                flag = False
                next_token = None

        start_time_str = start_time.split("T")[0]
        end_time_str = end_time.split("T")[0]
        all_data.to_csv(f"data/twitter/little_mermaid/start:{start_time_str},end:{end_time_str}_hashtag:{keyword}.csv")


if __name__ == "__main__":
    main()
    