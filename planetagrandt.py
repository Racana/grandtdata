import requests
import locale
import csv
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

locale.setlocale(locale.LC_ALL, 'es_ES.utf8')

### Goes to Planetagrandt and check if there are new statistics available
### Then includes the link in our (csv), proximately, database

def obtain_links(url, links, dates, validate):
    while True:
        validate_links = validate.links.tolist()
        validate_dates = validate.dates.tolist()

        r = requests.get(url)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, 'html.parser') 
        estadisticas = soup.find_all('a', text='Ver Estadísticas')
        date_header = soup.find_all('h2', {'class':'date-header'})

        [links.append(x['href']) for x in estadisticas] 
        [dates.append(parse_date(date.text)) for date in date_header]
        
        if any(l in validate_links for l in links):
            break #if any of the links is already stored we stop iterating

        if any(t < datetime(2016, 2, 1) for t in dates):
            break # we want to stop at 2015 season

        url = soup.find('a', {'class':'blog-pager-older-link'})['href']

    if len(links) != len(dates):
        print('something happen, link and dates dont have the same lenght')

    df = pd.DataFrame({'dates':dates, 'links': links})
    df.sort_values(by=['dates'], inplace=True)

    df = df.merge(validate, how='left', indicator=True).query('_merge == "left_only"').drop(['_merge'], axis=1)

    return df

def parse_date(date):
    parsed_date = datetime.strptime(date, '%A, %d de %B de %Y')
    return parsed_date

def run():
    url = 'https://www.planetagrandt.com.ar/search/label/Estad%C3%ADsticas'
    validate = pd.read_csv('~/testingfolder/grandtlinks.csv')
    validate.dates = validate.dates.apply(pd.to_datetime)

    links = []
    dates = []
    
    df = obtain_links(url, links, dates, validate)

    df.to_csv('~/testingfolder/grandtlinks.csv', index=False, mode='a', header=False)

    print('job completed')

if __name__ == "__main__":
    run()