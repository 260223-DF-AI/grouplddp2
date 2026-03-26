from fastapi import FastAPI
from pydantic import BaseModel
from app.routers import queryrouter
from app.models.instances import reporter
from app.services.gcs import GCS

def get_bq_client():
    """Provides a BigQuery client instance."""
    # The client automatically handles authentication if ADC is set up
    with bigquery.Client() as client:
        yield client


app = FastAPI(
    title= "BigQuery API",
    description="API for sales data",
    version="0.0.1"
)

app.include_router(queryrouter.router)

@app.get("/")
def get_root():
    return {"message": "Hello"}

class GCSPathRequest(BaseModel):
    gcs_path: str

@app.post("/process-gcs-file")
async def process_gcs_file(request: GCSPathRequest):
    # The GCS path is now accessible via request.gcs_path
    GCS_URI = request.gcs_path

    # Typically, you would pass this path to the
    # google-cloud-storage library here.

    print(f"Received GCS path: {GCS_URI}")

    return {
        "status": "success",
        "message": f"Processing file at {GCS_URI}"
    }

def main():
    """Creates and uploads .parquet files to GCS
    """
    gcs = GCS()
    gcs.upload_csvs_as_parquet()

    reporter.create_audit_log()

if __name__ == "__main__":
    main()