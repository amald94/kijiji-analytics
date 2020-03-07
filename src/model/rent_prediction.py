import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix, hstack
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.preprocessing import LabelBinarizer
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.linear_model import Ridge
from sklearn import preprocessing
from src.core.connect import create_engine_pgsql
from src.utils.text_processing import get_dataframe
from tabulate import tabulate
from src.utils.text_processing import getRegion,cleanPrice, convertToBedFloat, clean_parking

def getData_fromDB(DB_CONN,**kwargs):
    task_instance = kwargs['ti']
    engine = create_engine_pgsql(DB_CONN)
    kijiji , kijiji_details = get_dataframe(engine)
    merged_df = pd.merge(kijiji, kijiji_details, on='id')
    merged_df['region'] = merged_df['url'].apply(getRegion)
    merged_df['price'] = merged_df['price'].apply(cleanPrice)
    merged_df['bedrooms'] = merged_df['bedrooms'].apply(convertToBedFloat)
    merged_df['parking'] = merged_df['parking'].apply(clean_parking)

    print(tabulate(merged_df.head(), headers='keys', tablefmt='psql'))
    #remove unwanted columns for now
    data_initial = merged_df.copy()
    data_initial.drop(['id', 'url', 'status', 'created_at_x', 'title', 'description', 'location', 'posted_on', 'created_at_y', 'movein'], axis=1, inplace=True)
    print(tabulate(data_initial.head(), headers='keys', tablefmt='psql'))
    print(data_initial.bedrooms.unique())
    print(data_initial.bathrooms.unique())
    le = preprocessing.LabelEncoder()
    data_initial['unit_type'] = le.fit_transform(data_initial['unit_type'])
    data_initial['lease'] = le.fit_transform(data_initial['lease'])
    data_initial['pet_friendly'] = le.fit_transform(data_initial['pet_friendly'])
    data_initial['furnished'] = le.fit_transform(data_initial['furnished'])
    data_initial['region'] = le.fit_transform(data_initial['region'])
    print(tabulate(data_initial.head(), headers='keys', tablefmt='psql'))

