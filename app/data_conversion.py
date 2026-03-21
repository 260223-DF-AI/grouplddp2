"""
Script to  transform .csv to .parquet and then pipe data to Google Cloud Storage   
"""
from models.sales_data import SalesData
from dotenv import load_dotenv
import pandas as pd
import logging
import csv
import os

load_dotenv() # get environment variables

# uri to the GCS bucket location
gcs_uri = os.getenv("GCS_URI")
# path to the file with your GCS credentials
gc_auth = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_auth

directory_path = '../data/'
df = pd.DataFrame()
# combine multiple .csv files
with os.scandir(directory_path) as batches:
    for batch in batches:
        if batch.name.endswith('.csv'):
            if df.empty:
                df = pd.read_csv(batch)
            else:
                df = pd.concat([df, pd.read_csv(batch)])
        else:
            print(f"{batch.name} is not a csv")
            break

# upload .parquet to Google Cloud Storage
try:
    df.to_parquet(gcs_uri, engine='pyarrow')
    print(f"Parquet saved to {gcs_uri}")
except Exception as e:
    print(f"An error occurred: {e}")

def validate_csv(input_path, output_path, error_log):
    with open(input_path, newline='') as infile, \
        open(output_path, 'w', newline='') as outfile:
            
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        for row in reader:
            try:
                record = SalesData.convert_csv_types(row)
                writer.writerow(record.to_row())

            except Exception as e:
                # log row + str(e)
                
