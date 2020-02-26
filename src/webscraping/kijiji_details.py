import requests
from bs4 import BeautifulSoup
from src.core.connect import create_engine_pgsql
import time


def scrape_innerdata(DB_CONN,**kwargs):
    task_instance = kwargs['ti']
    engine = create_engine_pgsql(DB_CONN)

    q = engine.execute("""select * from kijiji where status not in ('expired','finished')""")
    for row in q:
        if not is_expired(row[1]):
            #update_db(engine,row[0],False)
            pass
        else:
            pass
            update_db(engine,row[0],True)


def is_expired(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    try:
        expired = soup.find_all('div', attrs={'class': 'expired-ad-container'})
        if expired:
            print('expired')
            return True
        else:
            print('not expired')
            return False
    except Exception as e:
        print(e)

def update_db(engine,id,status):

    query = """UPDATE kijiji 
               SET status = %s
               WHERE id = %s"""
    if status:
        engine.execute(query,('expired',id))