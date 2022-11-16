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


def locate_sentimentcsv(movie_id, map_folder2movieid_dict):
    '''
    Helper function to get csv filepath of a certain movie
    '''
    # Get folder name from mapping dictionary by id
    folder_name = map_folder2movieid_dict[movie_id]
    folder_path = os.path.join(project_path, 'data', 'twitter',
                               folder_name)
    # Get the csv filepath by pattern matching inside folder

    for file in os.listdir(folder_path):

        if file.endswith("_sentiment.csv") and "subset" not in file:
            return os.path.join(folder_path, file)

    return None


def calculate_sentiment_score(movie_id, map_folder2movieid_dict,
                              sentiment_cols=['negative', 'neutral', 'positive']):
    '''
    For each movie, calculate certain metrics related to sentiment based on model output.
    Returns a list of values ot be added as columns
    '''

    # Get csv file for movie and read
    csv_filepath = locate_sentimentcsv(movie_id,  map_folder2movieid_dict)

    sentiment_data = pd.read_csv(
        csv_filepath, usecols=sentiment_cols, sep="\t")

    # ----------- Calculation of different sentiment metrics -------------------

    n_tweets = int(sentiment_data.shape[0])

    # 1-Get how many tweets there are per cattegory in percentage
    data_percent = sentiment_data.copy(deep=True).rename(columns={
        'negative': 'perc_negative',
        'neutral': 'perc_neutral',
        'positive': 'perc_positive'
    })

    data_percent['overall_sentiment'] = data_percent.apply(
        (lambda row: row.index[np.argmax(row)]),
        axis=1)  # select most probable one as the choice of the model

    data_percent = (data_percent.groupby(
        'overall_sentiment').size()*100/n_tweets).round(decimals=2)

    # 2-Get the average probability for each cattegory
    data_scores_mean = sentiment_data.copy(deep=True).rename(columns={
        'negative': 'neg_score_avg',
        'neutral': 'neutr_score_avg',
        'positive': 'pos_score_avg'
    }).mean().round(decimals=2)

    # 3-Get a combined score weighting the different scores of the model
    weighted_score_data = sentiment_data.copy(deep=True).sum()

    sentiment_score = ((weighted_score_data['negative']*-1 +
                        weighted_score_data['neutral']*0 +
                        weighted_score_data['positive']*1)/n_tweets).round(decimals=4)

    data_out = pd.concat([data_percent, data_scores_mean,
                          pd.Series([sentiment_score, n_tweets], index=['sentiment_score', 'n_tweets'])])

    return data_out


def add_diversityscore(movies, actors):
    '''
    Adds columns related to diversity score to movies dataframe based on the presence of the presence
    of non-white actors in their cast, given by a boolean column 'not_white' in actors dataframe
    '''
    # Calculate diversity score as proportion of non-white actor in the main cast
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
                        .merge(scores, left_on=['film_name'],
                               right_on=['movie_title']))

    return movies_withscore


def add_sentimentscore(movies, map_folder2movieid_dict):
    '''
    Adds columns related to sentiment score to movies dataframe based on the output
    of the sentiment analysis model applied to tweets
    '''

    # Calculate sentiment score for each
    movies_scores = movies.apply(
        lambda row: calculate_sentiment_score(
            row['id'],  map_folder2movieid_dict),
        axis=1, result_type='expand')
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
        os.path.join(project_path, 'data', 'metadata', 'movies_metadata.json'),
        encoding='latin-1')
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
