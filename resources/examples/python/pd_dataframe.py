import pandas as pd

df = vizierdb.get_dataset('nyc311')
print(df.head())

data = list()
for _, values in df.iterrows():
    values = list(values)
    values[0] = values[0].upper()
    data.append(values)

df = pd.DataFrame(data=data, index=df.index, columns=df.columns)

vizierdb.update_dataset(name='nyc311', dataset=df)

boroughs = df['borough'].value_counts()
data = list()
for i in range(len(boroughs)):
    data.append([boroughs.index[i], boroughs[i]])
df = pd.DataFrame(data=data)

vizierdb.create_dataset(name='boroughs', dataset=df)
