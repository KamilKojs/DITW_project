import pandas as pd
import json
import os
import numpy as np
import glob


thisfile_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.abspath(os.path.join(thisfile_path, os.pardir))

# Mappings of movie id with its folder name of tweets
map_folder2movieid_dict = {
    4: 'bridgerton',
    2: 'ghostintheshell',
    5: 'inception',
    1: 'little_mermaid',
    7: 'mad_max_fury_road',
    3: 'thewitcher',
    6: 'wolf_of_wall_street'
}

# TO DO:
# - Compute sentiment score
# Change fixed filepaths to variables once known the format
# save output in csv with scores for each film
# Create another python notebook


def locate_sentimentcsv(movie_id, map_folder2movieid_dict):

    folder_name = map_folder2movieid_dict[movie_id]
    csvfile_path = glob.glob(os.path.join(project_path, 'data', 'twitter',
                                          folder_name, '*_sentiment.csv'))[0]

    return csvfile_path


def calculate_sentiment_score(movie_id, map_folder2movieid_dict, sentiment_cols=['negative', 'neutral', 'positive']):

    # Get csv file for each movie
    csv_filepath = locate_sentimentcsv(movie_id,  map_folder2movieid_dict)

    sentiment_data = pd.read_csv(
        csv_filepath, usecols=sentiment_cols, sep="\t")

    # Calculation of sentiment score
    n_tweets = sentiment_data.shape[0]
    sentiment_score = (sentiment_data.sum()).round(decimals=2)

    sentiment_score['total_score'] = (sentiment_score['negative']*-1 +
                                      sentiment_score['neutral']*0 +
                                      sentiment_score['positive']*1) / n_tweets

    sentiment_score['n_tweets'] = int(n_tweets)

    return sentiment_score[['total_score', 'n_tweets']]


def add_diversityscore(movies, actors):
    '''
    Adds column related to diversity score to movies dataframe based on the presence of the presence
    of non-white actors in their cast, given by a boolean column in actors dataframe

    '''

    scores = (actors
              .groupby('movie_title').agg(
                  n_nonwhite=pd.NamedAgg(
                      column='not_white', aggfunc=sum),
                  n_actors=pd.NamedAgg(
                      column='not_white', aggfunc=len),
                  diversity_score=pd.NamedAgg(column='not_white', aggfunc=(
                      lambda x: round(sum(x) / len(x), ndigits=3)))
              )
              )
    movies_withscore = (movies
                        .merge(scores, left_on=['film_name'], right_on=['movie_title']))

    return movies_withscore


def add_sentimentscore(movies, map_folder2movieid_dict):

    # Calculate sentiment score for each
    movies_scores = movies.apply(
        lambda row: calculate_sentiment_score(row['id'],  map_folder2movieid_dict), axis=1, result_type='expand')
    movies = pd.concat([movies, movies_scores], axis='columns')
    return movies


def main():

    # ------------------ Reading data ----------------------

    # read list of films
    movies = pd.read_csv(
        # file used as starting point is movies_minimalversion.csv
        os.path.join(project_path, 'data', 'movies.csv'),
        delimiter=';',
        header=0)

    # read metadata from scraping
    scraped_data = (pd.read_json(
        os.path.join(project_path, 'data', 'metadata', 'movies_metadata.json'), encoding='latin-1')
        .T
    )

    # read actors' etnicity data
    actors = pd.read_csv(
        # file used as starting point is movies_minimalversion.csv
        os.path.join(project_path, 'data', 'actor_race.csv'),
        delimiter=';',
        header=0)

    # --------------------- Processing ---------------------

    movies_with_scores = (
        movies
        .pipe(add_diversityscore, actors)
        .pipe(add_sentimentscore, map_folder2movieid_dict)
    )

    # -------------------- Saving --------------------------
    movies_with_scores.to_csv(os.path.join(
        project_path, 'data/movies_with_scores.csv'), sep=';', index=False)
    print(movies_with_scores)


if __name__ == '__main__':
    main()
