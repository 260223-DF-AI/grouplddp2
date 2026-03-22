import pandas as pd
import os
import gcsfs
from dotenv import load_dotenv

load_dotenv()

#uri to the bucket location to put file
gcs_uri = os.getenv("GCS_URI")

gc_auth = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_auth

directory_path = '../data/'
df = pd.DataFrame()
#
with os.scandir(directory_path) as batches:
    for batch in batches:
        if batch.name.endswith('.csv'):
            if df.empty:
                df = pd.read_csv(batch)
            else:
                df = pd.concat([df,pd.read_csv(batch)])
        else:
            print(f"{batch.name} is not a csv")
            break

#upload to cloud storage
try:
    df.to_parquet(gcs_uri, engine='pyarrow')
    print(f"Parquet saved to {gcs_uri}")
except Exception as e:
    print(f"An error occurred: {e}")

