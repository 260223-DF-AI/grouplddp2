import pandas as pd
import os

directory_path = '../data/'
df = pd.DataFrame()

with os.scandir(directory_path) as batches:
    for batch in batches:
        #print(len(pd.read_csv(batch)))
        if batch.name.endswith('.csv'):
            if df.empty:
                df = pd.read_csv(batch)
            else:
                df = pd.concat([df,pd.read_csv(batch)])
        else:
            print(f"{batch.name} is not a csv")
            break

df.to_parquet('../parquet/dummy_sales_batch.parquet')

#print(len(df))
