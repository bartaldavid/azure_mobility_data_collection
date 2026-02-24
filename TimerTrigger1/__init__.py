import logging
import asyncio
import os
import datetime
import aiohttp
from azure.functions import TimerRequest
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

# 1. Setup - Move clients outside the main function for "warm start" performance
credential = DefaultAzureCredential()
ACCOUNT_NAME = os.getenv("ADLS2_STORAGE_ACCOUNT", "mobilitydatabase2")
FILE_SYSTEM = os.getenv("ADLS2_FILE_SYSTEM", "raw-data")
BASE_URL = f"https://{ACCOUNT_NAME}.dfs.core.windows.net"

service_client = DataLakeServiceClient(account_url=BASE_URL, credential=credential)
fs_client = service_client.get_file_system_client(file_system=FILE_SYSTEM)


async def fetch_and_upload(session, url, timestamp):
    """Fetches a URL and uploads directly to ADLS2."""
    try:
        async with session.get(url, timeout=10) as response:
            data = await response.text()

            # Generate path: dott/feed_name/feed_name_timestamp.json
            feed_name = url.split("/")[-1].replace(".json", "")
            file_path = f"dott/{feed_name}/{feed_name}_{timestamp}.json"

            # Upload using the pre-warmed client
            file_client = fs_client.get_file_client(file_path)
            file_client.upload_data(data, overwrite=True)
            return file_path
    except Exception as e:
        logging.error(f"Failed {url}: {e}")
        return None


async def run_orchestrator():
    urls = [
        "https://gbfs.api.ridedott.com/public/v2/budapest/free_bike_status.json",
        "https://gbfs.api.ridedott.com/public/v2/budapest/station_status.json",
    ]
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_upload(session, url, timestamp) for url in urls]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]


def main(mytimer: TimerRequest) -> None:
    # Azure Functions (Python) supports top-level asyncio.run
    uploaded_files = asyncio.run(run_orchestrator())
    logging.info(f"Successfully uploaded {len(uploaded_files)} files.")
