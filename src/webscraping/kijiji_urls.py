import requests
from bs4 import BeautifulSoup
from src.core.connect import create_engine_pgsql
import time

url = 'https://www.kijiji.ca/b-for-rent/ontario'
baseurl = 'https://www.kijiji.ca'
baseForOntario = '/c30349001l9004'
pageNos = '/page-'
apartment = 'v-apartments-condos'
roomRent = 'room rent'
adurls = []
ad_ids = []

def getUrls(noPages, **kwargs):
    task_instance = kwargs['ti']
    for i in range(noPages):
        print('current page : '+str(i))
        url_final = url+pageNos+str(i)+baseForOntario
        response = requests.get(url_final)
        soup = BeautifulSoup(response.text, "html.parser")
        advtTitles = soup.findAll('div', attrs={'class' : 'title'})
        try:
            for link in advtTitles:
                adlink = baseurl+link.find('a')['href']
                adurls.append(adlink)
                ad_id = adlink.split('/')[6]
                ad_ids.append(ad_id)
                #print(adurl)
        except(Exception):
            print(Exception)
        #time.sleep(1)
    print("Total number of ads found : "+str(len(adurls)))
    adurls_set = set(adurls)
    ad_ids_set = set(ad_ids)
    print("Total number of ads found : "+str(len(adurls_set)))
    print("Total number of ads found : "+str(len(ad_ids_set)))
    task_instance.xcom_push(key='url_ids', value=ad_ids_set)
    print(ad_ids)
    print(adurls)
    task_instance.xcom_push(key='ad_urls', value=adurls_set)


def insert_adurls(DB_CONN,**kwargs):
    task_instance = kwargs['ti']
    url_ids = list(task_instance.xcom_pull(key='url_ids', task_ids='get_url_kijiji'))
    ad_urls = list(task_instance.xcom_pull(key='ad_urls', task_ids='get_url_kijiji'))
    engine = create_engine_pgsql(DB_CONN)
    print(ad_urls)

    #create a temp table to insert scrapted urls
    engine.execute('CREATE TABLE IF NOT EXISTS kijiji_tmp(id text PRIMARY KEY, url text, status text, \
                    created_at TIMESTAMPTZ DEFAULT Now())')
    engine.execute('CREATE TABLE IF NOT EXISTS kijiji(id text PRIMARY KEY, url text,status text, \
                    created_at TIMESTAMPTZ DEFAULT Now())')
    q2 = 'INSERT INTO kijiji (id, url, status) \
    SELECT kijiji_tmp.id, kijiji_tmp.url, kijiji_tmp.status\
    FROM kijiji_tmp \
    ON CONFLICT DO NOTHING;'

    for i in range(len(url_ids)):
        engine.execute("INSERT INTO kijiji_tmp (id, url,status) VALUES (%s, %s, %s)",url_ids[i],ad_urls[i],'pending')

    engine.execute(q2)
    #delete datas from temp table
    engine.execute('TRUNCATE TABLE kijiji_tmp')


