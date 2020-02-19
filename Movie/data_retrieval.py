import pandas as pd

FILE_PATH_MOVIE = "/home/amald/airflow/files/movies.csv"
FILE_PATH_TAGS = "/home/amald/airflow/files/tags.csv"
FILE_PATH_RATINGS = "/home/amald/airflow/files/ratings.csv"


def get_data():
    df_movies = pd.read_csv(FILE_PATH_MOVIE)
    df_ratings = pd.read_csv(FILE_PATH_RATINGS)
    df_tags = pd.read_csv(FILE_PATH_TAGS)

    return df_movies,df_ratings,df_tags


def merge_data(df_movies,df_ratings,df_tags):
    df_movie_raings = df_movies.merge(df_ratings,how="inner",on="movieId")
    print(df_movie_raings.columns)
    df_movie_tags = df_movies.merge(df_tags,how="inner",on="movieId")
    print(df_movie_tags.columns)
    df_movies_tags_ratings = df_movie_raings.merge(df_movie_tags,how="inner",on="movieId")
    return df_movie_raings,df_movie_tags,df_movies_tags_ratings