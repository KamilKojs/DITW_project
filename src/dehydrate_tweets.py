from os import listdir
from os.path import isfile, join
import os
import pandas as pd

def main():
    dir_path = "/Users/kamil/Documents/Dokumenty/ITU/Sem1/Data_in_the_wild/project/DITW_project/data/twitter/wolf_of_wall_street"
    all_files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    all_files = sorted(all_files)

    for file in all_files:
        if file.endswith('en.csv'):
            index = file.find('.csv')
            new_file_name = file[:index] + '_dehydr' + file[index:]

            df = pd.read_csv(f"{dir_path}/{file}")
            df = df["id"]
            df.to_csv(f"{dir_path}/{new_file_name}")


if __name__ == "__main__":
    main()