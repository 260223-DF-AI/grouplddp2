"""
Script to  transform .csv to .parquet and then pipe data to Google Cloud Storage (GCS)
"""
from models.sales_data import SalesData
from models.logger import get_logger
from models.benchmarking_report import BenchmarkData
from dotenv import load_dotenv
import pandas as pd
import csv
import os
import fsspec


logger = get_logger(__name__, 'error.log')
reporter = BenchmarkData()

class DataConversion:
    logger = get_logger(__name__)

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

    def upload_to_gcs(self):
        """ Uploads .csv file to GCS as .parquet"""
        try:
            data = self._read_and_clean_data()
            reporter.get_upload_speed(data.to_parquet)(self.gcs_uri, engine="pyarrow") # uploads file and measures time of operation
            self.logger.info(f"Parquet saved to {self.gcs_uri}")
            reporter.set_parquet_size(self.gcs_uri) # fetches file size using pyarrow, and sets to reporter obj
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

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

    def upload_csvs_as_parquet(self):
        """Processes all CSVs individually and writes them as separate Parquet files to GCS"""
        directory_path = '../data/'
        fs = fsspec.filesystem("gcs")  # requires gcsfs

        with os.scandir(directory_path) as batches:
            for idx, batch in enumerate(batches, start=1):
                if batch.name.endswith('.csv'):
                    self.logger.info(f"Processing CSV: {batch.path}")
                    cleaned_df = self._validate_csv(batch.path)

                    # Construct GCS URI for each Parquet file
                    gcs_file_uri = os.path.join(self.gcs_uri_prefix, f"dummy_sales_batch_{idx:02d}.parquet")

                    # Write directly to GCS
                    with fs.open(gcs_file_uri, 'wb') as f:
                        cleaned_df.to_parquet(f, engine='pyarrow', index=False)

                    self.logger.info(f"Uploaded {gcs_file_uri}")
                else:
                    self.logger.info(f"Skipping non-CSV file: {batch.name}")
