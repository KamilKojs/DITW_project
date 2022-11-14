import os
import pathlib
import pandas as pd


from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax

MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"


class Roberta_Sentiment:

    """
    Class for applying the latest pretrained roberta sentiment analysis model
    https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
    """

    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.config = AutoConfig.from_pretrained(MODEL)
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL)

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
    def get_sentiment(self, text):

        text = self.preprocess(text)

        encoded_input = self.tokenizer(text, return_tensors="pt")

        output = self.model(**encoded_input)

        scores = output[0][0].detach().numpy()
        scores = softmax(scores)
        

        scores_rounded = [np.round(float(score), 4) for score in scores]

        # print("------")
        # print(text)
        # self.print_sentiment(scores)
        # print("------")

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

    # TODO:
    def __init__(self, csv_file=None):
        self.csv_file = os.path.normpath(csv_file)
        self.cwd = pathlib.Path().resolve()
        self.movies_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'data/twitter'))
        self.roberta = Roberta_Sentiment()

    def get_movie_dirs(self):

        path = os.path.join(self.cwd, self.movies_path)

        list_of_movies = [f.path for f in os.scandir(path) if f.is_dir()]

        return list_of_movies

    def get_csv_files(self, directory):

        csv_files = []

        for file in os.listdir(directory):
            if file.endswith(".csv"):
                csv_files.append(os.path.join(directory, file))

        return csv_files

    def read_tweets_from_files(self, files):
        all_tweets = pd.DataFrame()
        
        for file in files:
            df = pd.read_csv(file)
            all_tweets = pd.concat([all_tweets, df], ignore_index=True)

        return all_tweets
    
    def run_analysis(self, file, out_file_path):
        df = pd.read_csv(file)
        length = len(df.index)
        df = df.assign(negative=pd.Series(np.zeros(length)).values)
        df = df.assign(neutral=pd.Series(np.zeros(length)).values)
        df = df.assign(positive=pd.Series(np.zeros(length)).values)

        for index, row in df.iterrows():
            text = row['text']
            scores = self.roberta.get_sentiment(text)
            df.at[index, 'negative'] = scores[0]
            df.at[index, 'neutral'] = scores[1]
            df.at[index, 'positive'] = scores[2]
            
        df.to_csv(out_file_path, sep='\t', encoding='utf-8', index=False)

    def run_sentiment_analysis(self):

        if self.csv_file is None:
            movie_dirs = self.get_movie_dirs()
            for movie_dir in movie_dirs:
                csv_files = self.get_csv_files(movie_dir)

                if len(csv_files) >= 0:

                    for file in csv_files:
                        
                        file_name = pathlib.Path(file).stem
                        out_file_name = f"{file_name}_sentiment.csv"

                        out_file_path = os.path.join(movie_dir, out_file_name)

                        self.run_analysis(file, out_file_path)
                        

                else:
                    print(f"No csv files found in {movie_dir}")

        else:
            file = self.csv_file
            movie_dir = os.path.split(file)[0]
            file_name = pathlib.Path(file).stem
            out_file_name = f"{file_name}_sentiment.csv"

            out_file_path = os.path.join(movie_dir, out_file_name)

            self.run_analysis(file, out_file_path)


if __name__ == "__main__":

    csv_reader = CSV_reader(csv_file="C:\\Users\\Juliu\\OneDrive\\ITU\\1st Semester\\DataInTheWild\\Project\\DITW_project\\data\\twitter\\inception\\subset_inception.csv")
    csv_reader.run_sentiment_analysis()
