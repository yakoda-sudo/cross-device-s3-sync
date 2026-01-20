import boto3
import botocore
from concurrent.futures import ThreadPoolExecutor
import logging

# ================= CONFIGURATION =================
# SOURCE
SRC_ACCESS_KEY   = 'YOUR_SOURCE_ACCESS_KEY'
SRC_SECRET_KEY   = 'YOUR_SOURCE_SECRET_KEY'
SRC_ENDPOINT     = 'https://s3.source-region.amazonaws.com'
SRC_BUCKET       = 'source-bucket-name'

# TARGET
DST_ACCESS_KEY   = 'YOUR_TARGET_ACCESS_KEY'
DST_SECRET_KEY   = 'YOUR_TARGET_SECRET_KEY'
DST_ENDPOINT     = 'https://s3.target-region.amazonaws.com'
DST_BUCKET       = 'target-bucket-name'

# SETTINGS
MAX_THREADS      = 4   # Adjust based on your bandwidth
# =================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_client(access, secret, endpoint):
    return boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret, endpoint_url=endpoint)

def sync_file(obj):
    key = obj['Key']
    size = obj['Size']
    src_etag = obj.get('ETag', '').replace('"', '')

    # Create clients inside thread
    s_cli = get_client(SRC_ACCESS_KEY, SRC_SECRET_KEY, SRC_ENDPOINT)
    d_cli = get_client(DST_ACCESS_KEY, DST_SECRET_KEY, DST_ENDPOINT)

    try:
        # 1. Check if file exists in destination
        try:
            head = d_cli.head_object(Bucket=DST_BUCKET, Key=key)
            if head['ContentLength'] == size and head.get('ETag', '').replace('"', '') == src_etag:
                logging.info(f"SKIP: {key} (Match)")
                return
        except botocore.exceptions.ClientError:
            pass # File not found, proceed to copy

        # 2. Copy File
        logging.info(f"COPY: {key}")
        resp = s_cli.get_object(Bucket=SRC_BUCKET, Key=key)
        
        d_cli.put_object(
            Bucket=DST_BUCKET, 
            Key=key, 
            Body=resp['Body'].read(),
            ContentType=resp.get('ContentType', 'application/octet-stream')
        )
        
    except Exception as e:
        logging.error(f"FAIL: {key} -> {e}")

def main():
    s_cli = get_client(SRC_ACCESS_KEY, SRC_SECRET_KEY, SRC_ENDPOINT)
    
    logging.info("Listing objects...")
    paginator = s_cli.get_paginator('list_objects_v2')
    
    files = []
    for page in paginator.paginate(Bucket=SRC_BUCKET):
        if 'Contents' in page:
            files.extend(page['Contents'])

    logging.info(f"Found {len(files)} files. Syncing with {MAX_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        pool.map(sync_file, files)

    logging.info("Done.")

if __name__ == "__main__":
    main()
