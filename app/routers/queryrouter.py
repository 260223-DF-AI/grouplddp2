import os

from fastapi import APIRouter, HTTPException
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
# "/"
# "/example"
router = APIRouter(
    prefix="/query",
    tags=["query"],
    responses={404: {"description": "Not found"}}
)

#print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
#gc_auth = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client

BQClient = get_bq_client()

client = bigquery.Client()


# localhost:{port_num}/example/
@router.get("/")
def get_query_root():
    return {"message": "Hello from example"}

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client


@router.get("/{transactionID}")
async def get_transaction(transactionID: int):
    """
    Queries BigQuery for an item's price and returns the result.
    """
    query = f"""
    SELECT *
    FROM `project-888cbb02-b71f-41c5-a44.sales_data.sales_data`
    WHERE transactionID = @transactionID
    """

    # Use query parameters to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Define a scalar query parameter, specifying name, type, and value
            bigquery.ScalarQueryParameter("transactionID", "INT64", transactionID)
        ]
    )

    try:
        # Execute the query
        query_job = client.query(query, job_config=job_config)

        # Fetch results
        results = [dict(row.items()) for row in query_job]

        if not results:
            raise HTTPException(status_code=404, detail="Item not found")

        return {"item": results[0]}
    except Exception as e:
        # Log the error and return a generic server error
        print(f"Error querying BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

router.get("/exception")
def get_exception():
    raise HTTPException(status_code=404, detail="Not found")