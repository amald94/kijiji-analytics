from Movie.connect import create_engine_pgsql, insert_to_db
from Movie.data_retrieval import merge_data, get_data
from Movie.aggregation import agg_rating_movie, agg_rating_tag


def core_get_data(**kwargs):
    task_instance = kwargs['ti']
    df_movies, df_ratings, df_tags = get_data()
    df_movie_raings,df_movie_tags,df_movies_tags_ratings = merge_data(df_movies,df_ratings,df_tags)
    task_instance.xcom_push(key='df_movie_ratings', value=df_movie_raings)
    task_instance.xcom_push(key='df_movie_tags_ratings', value=df_movies_tags_ratings)


def core_aggregation(**kwargs):
    task_instance = kwargs['ti']
    df_movie_ratings = task_instance.xcom_pull(key='df_movie_ratings', task_ids='get_data')
    df_movie_tags_ratings = task_instance.xcom_pull(key='df_movie_tags_ratings', task_ids='get_data')
    df_agg_rating = agg_rating_movie(df_movie_ratings)
    df_agg_rating_genre =agg_rating_tag(df_movie_tags_ratings)
    task_instance.xcom_push(key='df_agg_ratings', value=df_agg_rating)
    task_instance.xcom_push(key='df_agg_rating_genre', value=df_agg_rating_genre)
    
def core_db_insert_to_db(DB_CONN, **kwargs):
    task_instance = kwargs['ti']
    engine = create_engine_pgsql(DB_CONN)
    df_agg_rating = task_instance.xcom_pull(key='df_agg_ratings', task_ids='aggregation')
    df_agg_rating_genre = task_instance.xcom_pull(key='df_agg_rating_genre', task_ids='aggregation')
    insert_to_db(engine, df_agg_rating, 'agg_movie_ratings')
    insert_to_db(engine, df_agg_rating_genre, 'agg_rating_genere')