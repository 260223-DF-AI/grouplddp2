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

logger = get_logger(__name__, 'error.log')
reporter = BenchmarkData()

class DataConversion:
    logger = get_logger(__name__)
    gcs_uri = None
    gc_auth = None


    def set_up_environment(self):
        """ Sets up the environment"""
        load_dotenv()

    # upload .parquet to Google Cloud Storage
    try:
        reporter.get_upload_speed(data.to_parquet)(gcs_uri, engine="pyarrow") # uploads file and measures time of operation
        logger.info(f"Parquet saved to {gcs_uri}")
        reporter.set_parquet_size(gcs_uri) # fetches file size using pyarrow, and sets to reporter obj
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        
        # uri to the GCS bucket location
        gcs_uri = os.getenv("GCS_URI")
        # path to the file with your GCS credentials
        gc_auth = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if gcs_uri is None:
            self.logger.error("GCS_URI environment variable not found. Check .env file and README.md for setup help.")
        if gc_auth is None:
            self.logger.error(
                "GOOGLE_APPLICATION_CREDENTIALS environment variable not found. Check .env file and README.md for setup help.")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_auth

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

    def _read_and_clean_data(self) -> pd.DataFrame:
        """ Helper function to read .csv files from ../data/, cleans the data and return as a DataFrame

        Returns:
            pd.DataFrame: .csv data cleaned and validated
        """
        directory_path = '../data/'
        df = pd.DataFrame()
        # combine multiple .csv files
        with os.scandir(directory_path) as batches:
            for batch in batches:
                if batch.name.endswith('.csv'):
                    self.logger.info(f"Validating file: {batch.path}")
                    cleaned_batch = self._validate_csv(batch.path)

                    if df.empty:
                        df = cleaned_batch
                    else:
                        df = pd.concat([df, cleaned_batch])
                else:
                    print(f"{batch.name} is not a csv")
                    break
        return df
