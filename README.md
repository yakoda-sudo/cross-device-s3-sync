# S3 Compatible Object Sync Tool

A high-performance Python utility to synchronize objects between two S3-compatible storage services (e.g., AWS S3, MinIO, Wasabi, DigitalOcean Spaces).

## Key Features

* **Universal Compatibility:** Works with any S3-compliant endpoint.
* **Full Metadata Preservation:** Copies custom user metadata and content-type headers exactly as they are on the source.
* **Multi-Threaded:** Uses concurrent threading to maximize bandwidth usage for large transfers.
* **Smart Sync:** Checks file existence, size, and ETag (hash) to avoid re-uploading identical files.
* **Standalone:** No complex config files; all parameters are contained within the script logic.

## Prerequisites

* Python 3.6 or higher
* `boto3` library

## Installation

1.  Clone this repository:
    ```bash
    git clone [https://github.com/yourusername/s3-sync-tool.git](https://github.com/yourusername/s3-sync-tool.git)
    cd s3-sync-tool
    ```

2.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Open `s3_sync.py` and edit the **CONFIGURATION SECTION** at the top of the file. 

**⚠️ SECURITY WARNING ⚠️**
> This script is designed to hold credentials internally. **DO NOT commit your `s3_sync.py` file to GitHub after you have added your real Access Keys and Secret Keys.** > If you are tracking this project with Git, ensure you revert the keys to placeholders before pushing, or add the script to your `.gitignore`.

```python
# SOURCE Configuration
SRC_ACCESS_KEY = 'YOUR_SOURCE_ACCESS_KEY'
SRC_SECRET_KEY = 'YOUR_SOURCE_SECRET_KEY'
SRC_ENDPOINT_URL = '[https://s3.us-east-1.amazonaws.com](https://s3.us-east-1.amazonaws.com)'
SRC_BUCKET_NAME = 'source-bucket'

# TARGET Configuration
DST_ACCESS_KEY = 'YOUR_TARGET_ACCESS_KEY'
DST_SECRET_KEY = 'YOUR_TARGET_SECRET_KEY'
DST_ENDPOINT_URL = '[https://s3.us-west-1.amazonaws.com](https://s3.us-west-1.amazonaws.com)'
DST_BUCKET_NAME = 'target-bucket'

# PERFORMANCE
MAX_THREADS = 10