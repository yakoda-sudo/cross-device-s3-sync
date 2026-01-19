import boto3
import botocore
from concurrent.futures import ThreadPoolExecutor
import logging

# --- CONFIGURATION SECTION ---

# SOURCE Configuration
SRC_ACCESS_KEY = 'YOUR_SOURCE_ACCESS_KEY'
SRC_SECRET_KEY = 'YOUR_SOURCE_SECRET_KEY'
SRC_ENDPOINT_URL = 'https://source-endpoint.com' # e.g., https://s3.us-east-1.amazonaws.com or MinIO URL
SRC_BUCKET_NAME = 'source-bucket-name'

# TARGET Configuration
DST_ACCESS_KEY = 'YOUR_TARGET_ACCESS_KEY'
DST_SECRET_KEY = 'YOUR_TARGET_SECRET_KEY'
DST_ENDPOINT_URL = 'https://target-endpoint.com'
DST_BUCKET_NAME = 'target-bucket-name'

# PERFORMANCE Configuration
MAX_THREADS = 10  # Increase this for higher parallelism on large bandwidth connections

# -----------------------------

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_s3_client(access_key, secret_key, endpoint_url):
    """Creates an S3 client with the specific configuration."""
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
    )

def sync_object(obj_summary):
    """
    Worker function to sync a single object.
    Checks if object exists and is identical in destination. If not, copies it.
    """
    key = obj_summary['Key']
    size = obj_summary['Size']
    # ETag is usually the MD5 hash of the file (for standard puts), useful for comparison
    src_etag = obj_summary.get('ETag', '').strip('"') 

    # Create independent clients for threads to avoid race conditions
    src_client = get_s3_client(SRC_ACCESS_KEY, SRC_SECRET_KEY, SRC_ENDPOINT_URL)
    dst_client = get_s3_client(DST_ACCESS_KEY, DST_SECRET_KEY, DST_ENDPOINT_URL)

    try:
        # Check if object exists in destination
        try:
            head = dst_client.head_object(Bucket=DST_BUCKET_NAME, Key=key)
            dst_etag = head.get('ETag', '').strip('"')
            dst_size = head['ContentLength']

            # If Size and ETag match, skip copy
            if size == dst_size and src_etag == dst_etag:
                logging.info(f"SKIPPING: {key} (Already exists and matches)")
                return
        except botocore.exceptions.ClientError as e:
            # If 404, object doesn't exist, so we proceed to copy
            if e.response['Error']['Code'] != "404":
                logging.error(f"Error checking {key}: {e}")
                return

        # Perform the Copy
        logging.info(f"COPYING:  {key} ({size} bytes)")
        
        # We assume direct copying via getting the object from source and putting to dest
        # This is more compatible across different providers than copy_object which requires shared trust
        
        # 1. Get object and metadata from source
        source_response = src_client.get_object(Bucket=SRC_BUCKET_NAME, Key=key)
        body = source_response['Body']
        metadata = source_response.get('Metadata', {})
        content_type = source_response.get('ContentType', 'application/octet-stream')
        
        # 2. Upload to destination with Metadata
        dst_client.put_object(
            Bucket=DST_BUCKET_NAME,
            Key=key,
            Body=body,
            Metadata=metadata, # Explicitly setting custom metadata
            ContentType=content_type,
            MetadataDirective='REPLACE' # We are replacing generic auto-generated metadata with Source metadata
        )
        
    except Exception as e:
        logging.error(f"FAILED:   {key} - {str(e)}")

def main():
    logging.info("Starting Sync Process...")
    
    src_client = get_s3_client(SRC_ACCESS_KEY, SRC_SECRET_KEY, SRC_ENDPOINT_URL)
    
    # List all objects in source bucket
    # Using paginator to handle buckets with >1000 objects
    paginator = src_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=SRC_BUCKET_NAME)

    objects_to_sync = []
    
    logging.info("Listing objects in source bucket...")
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                objects_to_sync.append(obj)

    logging.info(f"Found {len(objects_to_sync)} objects. Starting transfer with {MAX_THREADS} threads.")

    # Multi-threaded execution
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(sync_object, objects_to_sync)

    logging.info("Sync Process Complete.")

if __name__ == "__main__":
    main()