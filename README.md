# GroupLDD - P2
## Set-up Guide:
In terminal:
    python -m venv venv
    ./Venv/Scripts/activate

    pip install -r requirements.txt

## Setting up Google Cloud:
1. Create a Google Cloud account
2. Create a project
3. Go to IAM in the left sidebar. Give yourself the roles of Organization Administrator
4. Search the 'Buckets' page and create a new bucket
5. Search the 'Keys' page and add a new key
6. In Python, create a .env file in the app directory
7. Set GSC_URI='gs://<bucket name>/data/'
8. Set GOOGLE_APPLICATION_CREDENTIALS='<path>/<downloaded project key>.json'

## Setting up BigQuery

1. Create a new query in the form of:
CREATE OR REPLACE EXTERNAL TABLE `project-<extension>.analytics_lab.dummy_sales_batch_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<bucket name>/data/dummy_sales_batch_*.parquet']
);


## Dev Tips:
Show API in Browser:localhost/{port_num}/docs
