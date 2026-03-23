# GroupLDD - Ramen Ratings Data Analysis
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
7. Set GSC_URI='gs://<bucket name>/<chosen file name>.parquet'
8. Set GOOGLE_APPLICATION_CREDENTIALS='<path>/downloaded project key.json'


## Dev Tips:
Show API in Browser:localhost/{port_num}/docs
