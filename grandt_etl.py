import pandas as pd

df = pd.DataFrame()

files = pd.read_csv('~/testingfolder/grandtlinks.csv')
files['status'] = files['status'].astype(str)

for index, row in files.iterrows():
    if row['status'] == 'parsed':
        continue
    filename = row['filename']
    data = pd.read_csv(f'~/testingfolder/data/{filename}')
    data = data.iloc[:-2]
    fecha = data.fecha[0]
    data = data.rename(columns={f'F{fecha}':'Puntaje'})

    data.drop(data.filter(regex='F\d+').columns, axis=1, inplace=True)

    df = df.append(data)
    files.at[index, 'status'] = 'parsed'


df.to_csv('~/testingfolder/grandtdata.csv', mode='a', header=False, index=False)
files.to_csv('~/testingfolder/grandtlinks.csv', index=False)
