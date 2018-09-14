# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from random import *
from datacleaner import autoclean
# Imports the Google Cloud client library
from google.cloud import bigquery

import os


def upload():
    client = bigquery.Client(project='seraphic-fire-151618')

    dataset_ref = client.dataset('crime_main')
    table_ref = dataset_ref.table('gun_violence')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open('cleaned_gun.csv', 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.


def load():
    df = pd.read_csv('gun.csv', dtype={
        'incident_id': str,
        'congressional_district': str,
        'state_house_district': str,
        'state_senate_district': str
    })
    return df


def add_missing_row(df):
    # Fix a row in the dataset
    # According to the author of this dataset, one particular incident is missing from the dataset
    # I have manually added this incident
    # Related Thread: https://www.kaggle.com/jameslko/gun-violence-data/discussion/55307

    missing_row = ['123456', '2017-10-01', 'Nevada', 'Las Vegas', 'Mandalay Bay 3950 Blvd S', 59, 489, 'https://en.wikipedia.org/wiki/2017_Las_Vegas_shooting', 'https://en.wikipedia.org/wiki/2017_Las_Vegas_shooting', '', '', '', '', '', '36.095', 'Hotel',
                   '-115.171667', 47, 'Route 91 Harvest Festiva; concert, open fire from 32nd floor. 47 guns seized; TOTAL:59 kill, 489 inj, number shot TBD,girlfriend Marilou Danley POI', '', '', '', '', '', '', '', '', '', '']
    df.loc[len(df)] = missing_row
    return df


def add_additional_feature(df):
    # Create some additional features
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['monthday'] = df['date'].dt.day
    df['weekday'] = df['date'].dt.weekday
    df['loss'] = df['n_killed'] + df['n_injured']
    return df


def change_data_test(df):
    pub = df['incident_id']
    isNum = pub.str.isdigit()
    df['incident_id'] = np.where(
        isNum, df['incident_id'], randint(100000, 999999))
    return df


def drop_row(df):
    to_drop = [
        'sources',
        'incident_url',
        'source_url',
        'incident_url_fields_missing'
    ]

    df.drop(to_drop, inplace=True, axis=1)
    return df


def to_numeric(df):
    df['latitude'] = df['latitude'].apply(pd.to_numeric)
    df['longitude'] = df['longitude'].apply(pd.to_numeric)
    return df


def remove_char(df):
    df = df.replace('\n', '', regex=True)
    df = df.replace('"', '', regex=True)
    return df


def __main__():

    df = load()
    df = add_missing_row(df)
    df = add_additional_feature(df)
    # df = change_data_test(df)
    df = drop_row(df)
    df = to_numeric(df)
    df = remove_char(df)
    # df = autoclean(df)

    df.to_csv("cleaned_gun.csv")
    print(df.loc[15153])


__main__()
