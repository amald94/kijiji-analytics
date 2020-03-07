import re
from src.core.connect import create_engine_pgsql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd


def cleanPrice(price):
    try:
        rent = re.sub('[^0-9.]+', '', price)
        return(int(float(rent)))
    except:
        return 0

def cleanAddress(address):
    regex = '[A^a-zA-Z]{1}[0-9]{1}[A^a-zA-Z]{1}[0-9]{1}[A^a-zA-Z]{1}[0-9]{1}'
    try:
        postalAddress = address.replace(" ", "")
        postal = re.search(regex, postalAddress)
        return postal.group(0)
    except:
        return 'unknown'

def getRegion(region):
    url = region.split("/")
    location = url[4]
    return location.replace("-"," ").replace("region","").rstrip()

def removeSymbols(text):
    return re.sub('[^A-Za-z0-9]+', ' ', text)

#a function to clean the new generated features
def convertToBedFloat(value):
    if 'Den' in str(value):
        rooms = value.split(" ")
        rooms = float(rooms[0])
        rooms += 0.5
    elif 'Bachelor/Studio' in str(value):
        rooms = 1.0
    else:
        value = removeSymbols(str(value))
        rooms = float(value)
    return rooms

def convertToBathFloat(value):
    return float(value)

def clean_parking(value):
    if 'Not Available' in value:
        parking = 0
    else:
        parking = removeSymbols(value)

    return parking

def get_dataframe(engine):

    session = Session(engine)
    # sqlalchemy: Reflect the tables
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    # Mapped classes are now created with names by default matching that of the table name.
    k1 = Base.classes.kijiji
    k2 = Base.classes.kijiji_rentals
    # Example query with filtering
    query1 = session.query(k1).filter(k1.status != 'expired')
    query2 = session.query(k2)
    # Convert to DataFrame
    kijiji = pd.read_sql(query1.statement, engine)
    kijiji_details = pd.read_sql(query2.statement, engine)

    return kijiji, kijiji_details
