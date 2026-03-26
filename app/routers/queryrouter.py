import os
from typing import List, Dict, Any, Literal

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from google.cloud import bigquery
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv('app/.env')


dataset = 'project-3caeb50a-50d4-4448-ad1.analytics_lab.dummy_sales_batch_ext'

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

client = bigquery.Client()

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client

BQClient = get_bq_client()

@router.get("/segments",response_model=List[Dict[str, Any]])
async def get_segments():

    query = """
    SELECT segment, SUM(total_amount) AS TotalAmount
    FROM `project-3caeb50a-50d4-4448-ad1.analytics_lab.dummy_sales_batch_ext`
    GROUP BY segment
    ORDER BY SUM(total_amount) DESC
    """

    # Use query parameters to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Define a scalar query parameter, specifying name, type, and value
            #bigquery.ScalarQueryParameter("select", "STRING", params.select),
           # bigquery.ScalarQueryParameter("category", "STRING", product_name),
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


@router.get("/topproducts", response_model=List[Dict[str, Any]])
async def get_products():

    query = f"""
    SELECT product_name, SUM(quantity) AS items_sold, SUM(total_amount) AS TotalAmount
    FROM `project-3caeb50a-50d4-4448-ad1.analytics_lab.dummy_sales_batch_ext`
    GROUP BY product_name
    ORDER BY SUM(total_amount) DESC
    """

    # Use query parameters to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Define a scalar query parameter, specifying name, type, and value
            #bigquery.ScalarQueryParameter("select", "STRING", params.select),
            #bigquery.ScalarQueryParameter("category", "STRING", product_name),
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


#Make a pytest to test this fastapi http request function:
@router.get("/category", response_model=List[Dict[str, Any]])
async def get_category(category_name: str):
    """
    Queries BigQuery for select columns of a specific category
    """
    query = """
    SELECT category, AVG(total_amount) AS avg_amount
    FROM `project-3caeb50a-50d4-4448-ad1.analytics_lab.dummy_sales_batch_ext`
    WHERE category = @category
    GROUP BY category
    """

    # Use query parameters to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Define a scalar query parameter, specifying name, type, and value
            #bigquery.ScalarQueryParameter("select", "STRING", params.select),
            bigquery.ScalarQueryParameter("category", "STRING", category_name),
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
        print ([dict(row) for row in results])
        return [dict(row) for row in results]
    except Exception as e:
        # Log the error and return a generic server error
        print(f"Error querying BigQuery: {e}")
        raise HTTPException(status_code=404, detail="Item not found")

router.get("/exception")
def get_exception():
    raise HTTPException(status_code=404, detail="Not found")