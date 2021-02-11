import pandas as pd
import re
import math

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

### Lets get General data
def data_downloading(url):
    global fecha, torneo
    dfs = pd.read_html(url, encoding='UTF-8')
    
    df = dfs[1]
    match_information = df.loc[0, 'Unnamed: 1']

    torneo = re.search('(\d+)[\/\d+]*', match_information).group(0).replace("/", "-")
    try:
        fecha = int(re.search('(\d+|FINAL[ES]?)$', match_information).group(1))
    except ValueError:
        date_finder = df.iloc[7:-2] 
        date_finder.columns = date_finder.iloc[0]
        date_finder = date_finder.drop(7)
        date_finder.dropna(axis=1, how='all', inplace=True)
        dates_played = date_finder.filter(regex='F\d+').columns.tolist()
        dates_played.sort(key=natural_keys)
        fecha = int(dates_played[-1].split('F')[1])

    df.drop('Unnamed: 0', axis=1, inplace=True)

    df = df.iloc[7:, :]
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df = df.reset_index(drop=True)

    df['fecha'] = fecha
    df['torneo'] = torneo

    return df

data = pd.read_csv('~/testingfolder/grandtlinks.csv')
data['filename'] = data['filename'].astype(str)

for index, row in data.iterrows():
    if row['filename'] != 'nan' : #is there a more pythonic way to do this?
        filename = row['filename']
        print(f'File already downloaded at {filename}')
        continue
    url = row['links']
    print(url)
    df = data_downloading(url)
    print(f'saving file planetagrandt_torneo_{torneo}_fecha_{fecha}.csv')
    df.to_csv(f'~/testingfolder/data/planetagrandt_torneo_{torneo}_fecha_{fecha}.csv', index=False)
    data.at[index, 'filename'] = f'planetagrandt_torneo_{torneo}_fecha_{fecha}.csv'

data.to_csv('~/testingfolder/grandtlinks.csv', index=False)