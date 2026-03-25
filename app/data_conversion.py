"""
Script to  transform .csv to .parquet and then pipe data to Google Cloud Storage (GCS)
"""
from models.sales_data import SalesData
from models.benchmarking_report import get_function_duration
from models.logger import get_logger
from instances import reporter
from dotenv import load_dotenv
import pandas as pd
import csv
import os
import fsspec
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

class DataConversion:
    logger = get_logger(__name__, 'error.log')

    def __init__(self):
        self.gcs_uri_prefix = None
        self.gc_auth = None
        self.set_up_environment()

    def set_up_environment(self):
        """ Sets up the environment"""
        load_dotenv()
        
        # uri to the GCS bucket location
        self.gcs_uri_prefix = os.getenv("GCS_URI")
        # path to the file with your GCS credentials
        self.gc_auth = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if not self.gcs_uri_prefix:
            self.logger.error("GCS_URI environment variable not found. Check .env file and README.md for setup help.")
        if not self.gc_auth:
            self.logger.error(
                "GOOGLE_APPLICATION_CREDENTIALS environment variable not found. Check .env file and README.md for setup help.")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.gc_auth

    def _dataframe_hash(self, df: pd.DataFrame) -> str:
        """Generate deterministic MD5 hash for a DataFrame"""
        table = pa.Table.from_pandas(df)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)  # write in-memory
        buf = sink.getvalue().to_pybytes()
        return hashlib.md5(buf).hexdigest()

    def _gcs_file_has_same_content(self, fs, gcs_file_uri: str, df_hash: str) -> bool:
        """Check if a Parquet file in GCS already has the same content hash"""
        try:
            info = fs.info(gcs_file_uri)
            remote_hash = info.get("metadata", {}).get("dataframe_hash")
            return remote_hash == df_hash
        except FileNotFoundError:
            return False

    def update_gcs_metadata(self, gcs_file_uri, metadata):
        """
        Update the metadata of a GCS object.
        gcs_file_uri example: gs://bucket_name/path/to/file.parquet
        """
        # Extract bucket and blob
        if not gcs_file_uri.startswith("gs://"):
            raise ValueError("GCS URI must start with gs://")
        parts = gcs_file_uri[5:].split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1]

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Update metadata
        blob.metadata = metadata
        blob.patch()

    @get_function_duration(reporter, 'u') # captures upload speed for reporting
    def upload_csvs_as_parquet(self):
        """Stream CSVs to GCS as Parquet files with content validation"""
        fs = fsspec.filesystem("gcs")
        directory_path = '../data/'

        with os.scandir(directory_path) as batches:
            for idx, batch in enumerate(batches, start=1):
                if batch.name.endswith('.csv'):
                    reporter.add_to_csv_size(os.path.getsize(batch)) # captures file size for reporting
                    self.logger.info(f"Processing CSV: {batch.path}")
                    
                    df = self._validate_csv(batch.path)
                    if df.empty:
                        self.logger.info(f"No valid rows found in {batch.name}, skipping.")
                        continue

                    # Compute hash
                    df_hash = self._dataframe_hash(df)

                    # Construct GCS file URI
                    gcs_file_uri = os.path.join(self.gcs_uri_prefix, f"dummy_sales_batch_{idx:02d}.parquet")
                    
                    # Check if file already exists with same content
                    if self._gcs_file_has_same_content(fs, gcs_file_uri, df_hash):
                        self.logger.info(f"Skipping upload; content unchanged: {gcs_file_uri}")
                        reporter.add_to_parquet_size(gcs_file_uri) # captures file size for reporting
                        continue

                    # Upload DataFrame to Parquet in-memory
                    with fs.open(gcs_file_uri, 'wb') as f:
                        df.to_parquet(f, engine='pyarrow', index=False)
                        
                    reporter.add_to_parquet_size(gcs_file_uri) # captures file size for reporting
                        
                    # Update metadata with hash
                    fs.invalidate_cache()
                    self.update_gcs_metadata(gcs_file_uri, {"dataframe_hash": df_hash})

                    self.logger.info(f"Uploaded {gcs_file_uri}")
                else:
                    self.logger.info(f"Skipping non-CSV file: {batch.name}")
                    
    def _validate_csv(self, input_path: str) -> pd.DataFrame:
        """ Helper function to validate a .csv file row by row. Returned valid records as a DataFrame and logs invalid records to error log

        Args:
            input_path (str): .csv file path

        Returns:
            pd.DataFrame: DataFrame of valid records
        """
        with open(input_path, newline='') as infile:
            reader = csv.reader(infile)
            next(reader)  # skip header row
            rows = []

            for row in reader:
                try:
                    record = SalesData.convert_csv_types(row) # convert fields to types of SalesData model
                    rows.append(record.to_row()) # adds valid record to list

                except Exception as e:
                    self.logger.error(f"Error: {e}\n Record: {row}") # adds invalid records to log
        return pd.DataFrame(rows, columns=SalesData.columns)
