import os
import pathlib
import pandas as pd
import datetime as dt
import numpy as np

from tqdm import tqdm
from scipy.special import softmax

from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig

MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"


class Roberta_Sentiment:

    """
    Class for applying the latest pretrained roberta sentiment analysis model
    https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
    """

    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL, device_map="auto")
        self.config = AutoConfig.from_pretrained(MODEL, device_map="auto")
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL, device_map="auto")

    # Preprocess text (username and link placeholders)
    def preprocess(self, text):

        text = str(text)
        new_text = []

        for t in text.split(" "):

            t = "@user" if t.startswith("@") and len(t) > 1 else t
            t = "http" if t.startswith("http") else t
            new_text.append(t)

        return " ".join(new_text)

    # get sentiment for text
    def get_sentiment(self, text, print_sentiment_score=False):

        text = self.preprocess(text)

        encoded_input = self.tokenizer(text, return_tensors="pt")

        output = self.model(**encoded_input)

        scores = output[0][0].detach().numpy()
        scores = softmax(scores)

        scores_rounded = [np.round(float(score), 4) for score in scores]

        if print_sentiment_score:

            print("------")
            print(text)
            self.print_sentiment(scores)
            print("------")

        return scores_rounded

    def print_sentiment(self, scores):

        ranking = np.argsort(scores)
        ranking = ranking[::-1]

        for i in range(scores.shape[0]):

            l = self.config.id2label[ranking[i]]
            s = scores[ranking[i]]
            print(f"{i+1}) {l} {np.round(float(s), 4)}")


class CSV_reader:
    """
    Reading all csv files from each movie directory and running sentiment analysis on it
    """

    def __init__(self, csv_file=None):

        if csv_file is not None:

            self.csv_file = os.path.normpath(csv_file)

        else:

            self.csv_file = None
        self.cwd = pathlib.Path().resolve()
        self.movies_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data/twitter")
        )
        self.roberta = Roberta_Sentiment()

    def get_movie_dirs(self):

        path = os.path.join(self.cwd, self.movies_path)

        list_of_movies = [f.path for f in os.scandir(path) if f.is_dir()]

        return list_of_movies

    def get_csv_files(self, directory):

        csv_files = []

        for file in os.listdir(directory):

            if file.endswith(".csv") and file.startswith("start_"):
                csv_files.append(os.path.join(directory, file))

        return csv_files

    def read_tweets_from_files(self, files):

        all_tweets = pd.DataFrame()

        for file in files:

            df = pd.read_csv(file)
            all_tweets = pd.concat([all_tweets, df], ignore_index=True)

        return all_tweets

    def run_analysis_on_file(self, file, out_file_path, size=None):

        df = pd.read_csv(file)
        length = len(df.index)
        df = df.assign(negative=pd.Series(np.zeros(length)).values)
        df = df.assign(neutral=pd.Series(np.zeros(length)).values)
        df = df.assign(positive=pd.Series(np.zeros(length)).values)

        if size is not None:

            size = max(size, length)
            print(f"using sample size {size}")
            print(f"df sample size is {len(df.index)}")
            df = df.sample(size)

        negative = [0]*size
        neutral = [0]*size
        positive = [0]*size

        df.reset_index(inplace=True)

        for index, row in tqdm(df.iterrows()):

            text = row["text"]
            scores = self.roberta.get_sentiment(text)

            negative[index]=scores[0]
            neutral[index]=scores[1]
            positive[index]=scores[2]

        df["negative"] = negative
        df["neutral"] = neutral
        df["positive"] = positive

        df.to_csv(out_file_path, sep="\t", encoding="utf-8", index=False)

    def run_analysis_on_df(self, df, out_file_path, size=None):

        length = len(df.index)
        df = df.assign(negative=pd.Series(np.zeros(length)).values)
        df = df.assign(neutral=pd.Series(np.zeros(length)).values)
        df = df.assign(positive=pd.Series(np.zeros(length)).values)

        if size is not None:

            size = min(size, length)
            print(f"using sample size {size}")
            df = df.sample(size)
            print(f"df sample size is {len(df.index)}")

        negative = [0]*size
        neutral = [0]*size
        positive = [0]*size
        
        df.reset_index(inplace=True)
            
        for index, row in tqdm(df.iterrows()):

            text = row["text"]
            scores = self.roberta.get_sentiment(text)

            negative[index]=scores[0]
            neutral[index]=scores[1]
            positive[index]=scores[2]

        df["negative"] = negative
        df["neutral"] = neutral
        df["positive"] = positive

        df.to_csv(out_file_path, sep="\t", encoding="utf-8", index=False)

    def run_sentiment_analysis(self, sample_size=None):

        if self.csv_file is None:

            movie_dirs = self.get_movie_dirs()

            for movie_dir in movie_dirs:

                csv_files = self.get_csv_files(movie_dir)

                if len(csv_files) >= 0:

                    df = self.read_tweets_from_files(csv_files)

                    movie_name = os.path.basename(os.path.normpath(movie_dir))
                    out_file_name = f"{movie_name}_sentiment.csv"
                    out_file_path = os.path.join(movie_dir, out_file_name)

                    print(
                        f"starting sentiment analysis on {movie_name} at {dt.datetime.now()}"
                    )
                    self.run_analysis_on_df(df, out_file_path, size=sample_size)
                    print(
                        f"finished sentiment analysis on {movie_name} at {dt.datetime.now()}"
                    )
                    print(
                        f"saved sentiment analysis on {movie_name} to {out_file_path}"
                    )

                else:
                    print(f"No csv files found in {movie_dir}")

        else:

            file = self.csv_file
            movie_dir = os.path.split(file)[0]
            file_name = pathlib.Path(file).stem
            out_file_name = f"{file_name}_sentiment.csv"

            out_file_path = os.path.join(movie_dir, out_file_name)

            print(f"starting sentiment analysis on {file_name} at {dt.datetime.now()}")
            self.run_analysis_on_file(file, out_file_path)
            print(f"finished sentiment analysis on {file_name} at {dt.datetime.now()}")


if __name__ == "__main__":

    csv_reader = CSV_reader()
    csv_reader.run_sentiment_analysis(sample_size=10000)
