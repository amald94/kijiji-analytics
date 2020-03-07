from src.core.connect import create_engine_pgsql


def create_db(DB_CONN,**kwargs):
    task_instance = kwargs['ti']
    engine = create_engine_pgsql(DB_CONN)
    engine.execute('CREATE TABLE IF NOT EXISTS kijiji_tmp(id text PRIMARY KEY, url text, status text, \
                    created_at TIMESTAMPTZ DEFAULT Now())')
    engine.execute('CREATE TABLE IF NOT EXISTS kijiji(id text PRIMARY KEY, url text,status text, \
                    created_at TIMESTAMPTZ DEFAULT Now())')
    engine.execute('CREATE TABLE IF NOT EXISTS kijiji_rentals(id text PRIMARY KEY, title text,price text, \
                    description text, location text, posted_on text, unit_type text, bedrooms text, \
                    bathrooms text, parking text, lease text, movein text, pet_friendly text, \
                    furnished text, created_at TIMESTAMPTZ DEFAULT Now())')