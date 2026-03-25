import base64
import hashlib
import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def calculate_local_md5(file_path):
    """Calculates the MD5 hash of a local file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return base64.b64encode(hash_md5.digest()).decode('utf-8')

def check_if_file_unique(bucket_name, source_file_name, destination_file_name):
    """Checks if a file exists in the bucket. returns true if it is unique and false otherwise."""

    #change to where the local parquet file is
    local_md5_hash = calculate_local_md5(source_file_name)
    print(f"Local file MD5 hash (Base64): {local_md5_hash}")

    storage_client = storage.Client()
    #bucket = storage_client.bucket('project_2_groupldd')
    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob('dummy_sales.parquet')
    blob = bucket.blob(destination_file_name)

    # 2. Retrieve GCS object metadata to get its hash
    if blob.exists():
        # Reload metadata to ensure we have the latest info including etag/md5_hash
        blob.reload()
        gcs_md5_hash = blob.md5_hash
        print(f"GCS object MD5 hash (Base64): {gcs_md5_hash}")

        # 3. Compare hashes
        if local_md5_hash == gcs_md5_hash:
            print("Content is identical. Skipping processing/upload.")
            return False
        else:
            print("Content is different. Proceeding with upload/processing.")
            return True
    else:
        print("Object does not exist in GCS. Proceeding with upload/processing.")
        return True