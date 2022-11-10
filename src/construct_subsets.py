from os import listdir
from os.path import isfile, join
import os
import pandas as pd


def main():
    movie_title = "wolf_of_wall_street"
    dir_path = "/Users/kamil/Documents/Dokumenty/ITU/Sem1/Data_in_the_wild/project/DITW_project/data/twitter/" + movie_title
    all_files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    all_files = sorted(all_files)
    tweets_per_file = 28
    subset_all = pd.DataFrame()

    for file in all_files:
        df = pd.read_csv(f"{dir_path}/{file}")
        subset_all = pd.concat([subset_all, df.head(tweets_per_file)], ignore_index=True)

    subset_all.to_csv(f"{dir_path}/subset_{movie_title}.csv")


if __name__ == "__main__":
    main()