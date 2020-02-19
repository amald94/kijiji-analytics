from sqlalchemy import create_engine

def create_engine_pgsql(DB_CONN):

    engine = create_engine(DB_CONN)
    #engine = create_engine('postgresql+psycopg2://amal:amaldas@localhost/movies', echo=False)
    return engine