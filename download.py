import requests
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime as dt, date, timedelta
import pandas as pd
from dotenv import load_dotenv
import json
load_dotenv()

def request_with_retry(url, params={}, headers={}):
    '''Retry a request at 1s time interval.'''

    try:
        with requests.get(url, params=params, headers=headers) as page:
            return page
    except: # explicit errors ?
        raise

def convert_to_datetime(text):
    return dt.strptime(text,'%d.%m.%Y %H:%M:%S')
def convert_temp(text):
    return text.replace(',', '.').replace(' C','')
def convert_humidity(text):
    return text.replace(',', '.').replace('%','')
def get_rows(page):
    rows = page.find('table', {'class': 'table table-striped'})
    if rows ==None:
        return
    rows = rows.find_all('tr')
    return rows
def get_page(url, params):
    page = request_with_retry(url = url, params = params)
    return BeautifulSoup(page.text, 'html.parser')
def transform_cols(df):
    df.Zeitpunkt = df.Zeitpunkt.apply(convert_to_datetime)
    df['Temperatur Innen'] = df['Temperatur Innen'].apply(convert_temp)
    df['Temperatur Außen'] =  df['Temperatur Außen'].apply(convert_temp)
    df['Luftfeuchte Innen'] =  df['Luftfeuchte Innen'].apply(convert_humidity)
    df['Luftfeuchte Außen'] =  df['Luftfeuchte Außen'].apply(convert_humidity)
    df = df.set_index('Zeitpunkt')
    return df

min_date = date( 1970, 1, 1)
url = 'https://measurements.mobile-alerts.eu/Home/MeasurementDetails?'
now = dt.today()
end = (now.date() - min_date).days*60*60*24 + now.hour*60*60 + now.minute*60
start = (now.date() - timedelta(days=90) - min_date).days*60*60*24
device_info = json.loads(os.getenv('STATION_ID'))
params = {**device_info,
    'appbundle': 'eu.mobile_alerts.mobilealerts',
    'fromepoch':start,
    'toepoch': end
}

lines = []
print('Start downloading')
page = get_page(url, params)
rows = get_rows(page)
header = [i.text for i in rows[0].find_all('th')]
for row in rows[1:]:
    row = [i.text for i in row.find_all('td')]
    lines.append(row)
df = pd.DataFrame(lines, columns=header)
df = transform_cols(df)

filename = f'wetterstation_{now.day}-{now.month}-{now.year}.csv'    
df.to_csv(filename)
print('Finally done')