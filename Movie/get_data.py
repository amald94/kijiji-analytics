from data_retrieval import get_data,merge_data
from aggregation import agg_rating_movie,agg_rating_tag
from connect import create_engine_pgsql, insert_to_db

df_movies,df_ratings,df_tags = get_data()

#print(df_movies,df_ratings,df_tags)

df_movie_raings,df_movie_tags,df_movies_tags_ratings = merge_data(df_movies,df_ratings,df_tags)

# print(df_movies_tags_ratings.drop_duplicates())

df_agg_rating = agg_rating_movie(df_movie_raings)
print(df_agg_rating)
df_agg_rating_genere = agg_rating_tag(df_movies_tags_ratings)
print(df_agg_rating_genere) 

engine = create_engine_pgsql()
insert_to_db(engine, df_agg_rating, 'agg_movie_ratings')
insert_to_db(engine, df_agg_rating_genere, 'agg_rating_genere')