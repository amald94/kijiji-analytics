import pandas as pd 
from sqlalchemy import create_engine

def create_engine_pgsql(DB_CONN):

    engine = create_engine(DB_CONN)
    #engine = create_engine('postgresql+psycopg2://amal:amaldas@localhost/movies', echo=False)
    return engine


def insert_to_db(engine, df, table_name):
    df.to_sql(table_name, con=engine, if_exists="append")