import pandas as pd

df = pd.DataFrame({'A': [1,2,1], 'B': [3,4,3], 'Size': ['Ma2','kb3','3l Varies with device po']})
for i, v in enumerate(df['Size'].values):
    v = v.replace('M', '')
    v = v.replace('k', '')
    v = v.replace('Varies with device', '')
    df['Size'].values[i] = v


