import pandas as pd


def main():
    movies = pd.read_csv("data/movies.csv", sep=';', header=None)
    movies = movies.rename(columns={"0": "title", "1": "year"})
    print(movies)


if __name__ == "__main__":
    main()