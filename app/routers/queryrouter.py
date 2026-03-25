import os
from typing import List, Dict, Any, Literal

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from google.cloud import bigquery
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()
# "/"
# "/example"
router = APIRouter(
    prefix="/query",
    tags=["query"],
    responses={404: {"description": "Not found"}}
)

#allowed columns to select
AllowedColumns = Literal["TransactionID", "Category", "StoreID", "all"]
class DataQueryParams(BaseModel):
    category: str # Category name filter
    select_column: AllowedColumns = Field("all", description="Column to select, or 'all' for all columns")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client

BQClient = get_bq_client()

client = bigquery.Client()


# localhost:{port_num}/example/
#@router.get("/")
#def get_query_root():
#    return {"message": "Hello from example"}

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client


@router.get("/", response_model=List[Dict[str, Any]])
async def get_transaction(params: DataQueryParams = Depends()):
    """
    Queries BigQuery for select columns of a specific category
    """
    if params.select_column == "all":
        columns = "*"
    else:
        columns = params.select_column

    query = f"""
    SELECT {columns}
    FROM `project-888cbb02-b71f-41c5-a44.sales_data.sales_data`
    WHERE Category = @category
    LIMIT 100
    """

    # Use query parameters to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Define a scalar query parameter, specifying name, type, and value
            #bigquery.ScalarQueryParameter("select", "STRING", params.select),
            bigquery.ScalarQueryParameter("category", "STRING", params.where),
        ]
    )

    try:
        # Execute the query
        query_job = client.query(query, job_config=job_config)

        # Fetch results
        results = [dict(row.items()) for row in query_job]

        if not results:
            raise HTTPException(status_code=404, detail="Item not found")

        #return results
        return [dict(row) for row in results]
    except Exception as e:
        # Log the error and return a generic server error
        print(f"Error querying BigQuery: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

router.get("/exception")
def get_exception():
    raise HTTPException(status_code=404, detail="Not found")