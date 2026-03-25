## example code from Rich's lecture
from fastapi import FastAPI
from pydantic import BaseModel
from routers import queryrouter
from instances import reporter
from data_conversion import DataConversion

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
    path = request.gcs_path

    # Typically, you would pass this path to the
    # google-cloud-storage library here.

    print(f"Received GCS path: {path}")

    return {
        "status": "success",
        "message": f"Processing file at {path}"
    }

def main():
    """Creates and uploads .parquet files to GCS
    """
    dc = DataConversion()
    dc.upload_csvs_as_parquet()
    
    reporter.create_audit_log()

if __name__ == "__main__":
    main()