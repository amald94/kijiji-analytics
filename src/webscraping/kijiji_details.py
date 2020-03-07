import requests
from bs4 import BeautifulSoup
from src.core.connect import create_engine_pgsql
import time

apartment = 'v-apartments-condos'
roomRent = 'room rent'

default_properties = {'title': 'Rental', 'price': '0', 'description': 'single occupancy', 'location': 'toronto',
                      'date added': 'just now', 'Unit Type': 'Room Rent', 'Bedrooms': '1', 'Bathrooms': '1',
                      'Parking Included': '1','Agreement Type': 'Month-to-month', 'Move-In Date': 'ASAP',
                      'Pet Friendly': 'Yes','Furnished' : 'Yes'}

def parse_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    return soup

def is_expired(url):
    soup = parse_url(url)
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

def update_db(url,engine,id,status):

    query = """UPDATE kijiji 
               SET status = %s
               WHERE id = %s"""
    try:
        if status:
            engine.execute(query,('expired',id))
        else:
            get_properties(id,url,engine)
            engine.execute(query, ('processed', id))

    except Exception as e:
        print(e)

def get_properties(id,url,engine):
    soup = parse_url(url)
    try:
        default_properties['title'] = soup.select_one("h1[class*=title-2323565163]").text
        default_properties['price'] = soup.select_one("span[class*=currentPrice-2842943473]").text
        # default_properties['description'] = soup.find_all('div', attrs={'class': 'descriptionContainer-3544745383'})
        default_properties['location'] = soup.find('span', attrs={'class': 'address-3617944557'}).text
        default_properties['date added'] = soup.find('time')['title']
        print(url)
        desc = soup.find_all('div', attrs={'class': 'descriptionContainer-3544745383'})
        for tag in desc:
            # print(tag.text.strip())
            default_properties['description'] = tag.text.strip()

        print(apartment)
        if apartment in url:
            addetails = soup.find('ul', attrs={'class': 'list-1757374920 disablePadding-1318173106'})
            print(addetails)
            print(type(addetails))
            adfts = addetails.find_all('li', attrs={'class': 'twoLinesAttribute-1157856205'})
            default_properties['Unit Type'] = 'Apartment'
            for ft in adfts:
                # dd = ft.find('div').text
                dt = ft.find_all('dt', attrs={'twoLinesLabel-3766429502'})
                dd = ft.find_all('dd', attrs={'twoLinesValue-2815147826'})
                for tag,val in zip(dt,dd):
                    print(tag.text.strip())
                    print(val.text.strip())
                    default_properties[tag.text.strip()] = val.text.strip()
        else:
            for tag in desc:
                description_advt = tag.text.strip()
                if 'basement' in description_advt.lower():
                    default_properties['Unit Type'] = 'Basement'
            adfts = soup.find_all('dl', attrs={'class': 'itemAttribute-983037059'})
            for ft in adfts:
                dt = ft.find_all('dt', attrs={'attributeLabel-240934283'})
                dd = ft.find_all('dd', attrs={'attributeValue-2574930263'})
                for tag,val in zip(dt,dd):
                    print(tag.text.strip())
                    print(val.text.strip())
                    default_properties[tag.text.strip()] = val.text.strip()

        print(default_properties)
        insert_to_tables(engine,id,default_properties)
    except Exception as e:
        print(e)
        pass

def insert_to_tables(engine,id,data):

    print('inside insertion')
    query = """INSERT INTO kijiji_rentals (id, title, price, description, location, posted_on, unit_type, bedrooms,
                bathrooms, parking, lease, movein, pet_friendly, furnished) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    try:
        engine.execute(query,(id, data['title'], data['price'], data['description'], data['location'],
                              data['date added'], data['Unit Type'], data['Bedrooms'], data['Bathrooms'],
                              data['Parking Included'], data['Agreement Type'], data['Move-In Date'],
                              data['Pet Friendly'], data['Furnished']))

        print('inserted to table')
    except Exception as e:
        print(e)

def scrape_innerdata(DB_CONN,**kwargs):
    task_instance = kwargs['ti']
    engine = create_engine_pgsql(DB_CONN)
    q = engine.execute("""select * from kijiji where status not in ('expired','processed')""")
    for row in q:
        if not is_expired(row[1]):
            update_db(row[1],engine,row[0],False)
        else:
            update_db(row[1],engine,row[0],True)
