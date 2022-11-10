from os import listdir
from os.path import isfile, join
import os
import pandas as pd


def main():
    dir_path = "/Users/kamil/Documents/Dokumenty/ITU/Sem1/Data_in_the_wild/project/DITW_project/data/twitter/wolf_of_wall_street"
    all_files = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    all_files = sorted(all_files)
    total = 0

    for file in all_files:
        file_name = os.path.basename(file)
        df = pd.read_csv(f"{dir_path}/{file}")
        length = len(df.index)
        total += length
        print(f"Tweet count ({file_name}): {length}")

    print(f"Total tweet count: {total}")


if __name__ == "__main__":
    main()