import pandas as pd

df = pd.DataFrame()

files = pd.read_csv('grandtlinks.csv')
try:
    files['status'] = files['status'].astype(str)
    header=False
except KeyError:
    files['status'] = ''
    header=True

for index, row in files.iterrows():
    if row['status'] == 'parsed':
        continue
    filename = row['filename']
    data = pd.read_csv(f'data/{filename}')
    data = data.iloc[:-2]
    fecha = data.fecha[0]
    data = data.rename(columns={f'F{fecha}':'Puntaje'})

    data['Cotizacion'] = data['Cotización'].str.replace(r'\.', '')

    if 'GC.1' in data.columns:
        data.rename(columns={'GC':'G', 'POS':'Puesto', 'GC.1':'GC'}, inplace=True)

    data.drop(data.filter(regex=r'F\d+').columns, axis=1, inplace=True)
    data.drop(columns=['PcG', 'PCG', 'AcG', 'PrG', 'PCT', 'CG', 'CT', 'PcG', 'AcT', 'Cotización'], axis=1, inplace=True, errors='ignore')

    df = df.append(data)
    files.at[index, 'status'] = 'parsed'


df.to_csv('grandtdata.csv', mode='a', header=header, index=False)
files.to_csv('grandtlinks.csv', index=False)
