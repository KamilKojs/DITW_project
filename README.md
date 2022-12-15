# DITW_project
Data_in_the_wild_project

Create venv with:
    python3 -m venv /path/to/new/virtual/environment

Activate venv and install dependencies:
    pip3 install -r requirements.txt

File descriptions:
The following repository contains data gathered during project for Data In The Wild course.
There are 2 main directories in this repository:
 - data
 - src

Data directory structure:
 - annotation_guide - annotation guide for labeling of our data
 - img - all plots produced for the article
 - metadata - cinematic productions metadata scraped from wikipedia
 - twitter - twitter data gathered from twitter API 2.0 divided into movies directories

Each movie directory under twitter dir holds the following files:
 - movie_title_annotations_annotator_initials.csv - first round of annotation of our data.
 - movie_title_annotations_annotator_initials.csv - second round of annotation of our data.
 - movie_title_annotations_final.csv - third round of annotation of our data.
 - movie_title_sentiment.csv - sentiment of tweets generated with the model.
 - movie_title_sentiment.csv - sentiment of tweets generated with the model with tweet date appended.
 - subset_movie_title.csv - 500 tweet sample of tweets used for data annotation and analysis.
 - subset_movie_title_sentiment.csv - 500 tweet sample of tweets used for data annotation and analysis with sentiment output appended from the model.
 - start_date_end_date_hashtag_hashtagvalue_dehydr.csv - all twitter data fetched for specific movie from twitter API 2.0. It consists only tweet id for privacy reasons but the rest of the tweet data can be fetched using the provided id from twitter API 2.0 endpoints.

 Src directory structure:
  - actor_df.ipynb - Notebook used for creation of actors dataframes used in later analysis.
  - calculate_metrics_sentiment.ipynb - Notebook used for calculation of metrics regarding sentiment of tweets on 500 tweet subset of the data. The metrics are our annotations compared to model output.
  - calculate_moviescores.py - Script used for calculation of moviescores.
  - construct_subsets.py - Script used for construction of 500 tweet subsets of all the data that were later used for future analysis and annotation process.
  - correlation_analysis.ipynb - Notebook used for correlation analysis between racial diversity of the main cast of a film and the overall sentiment observed in tweets associated.
  - count_tweets.py - Script used for calculating how many tweets are in desired directory with tweets data.
  - create_time_series.ipynb - Notebook used for creation of plots regrding how sentiment changed over time for each movie.
  - create_timeseries_twitter_api.py - Script used for appending the date to the twitter data. Created files were later used in notebook for plot creation reagrding time series data.
  - dehydrate_tweets.py - Script used for dehydration process of twitter data.
  - plot_analysis.ipynb - Notebook used for analysis of correlation.
  - rename_files.py - Script used for renaming files to desired convention.
  - sentiment_analysis.py - Script used for generation of sentiment using the model.
  - twitter_api.py - Script used for data gathering from twitter API 2.0.
  - wikipedia_scraper.py - Script used for scraping data from wikipedia.
