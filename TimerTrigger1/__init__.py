import datetime
import logging
import aiohttp
import azure.functions as func
import asyncio
import os
from azure.storage.filedatalake import DataLakeServiceClient
import json


def _upload_file(
    service_client: DataLakeServiceClient,
    file_system_name: str,
    adls_directory: str,
    destination_path: str,
    content: str,
) -> str:
    full_path = (
        f"{adls_directory.rstrip('/')}/{destination_path}"
        if adls_directory
        else destination_path
    )

    file_system_client = service_client.get_file_system_client(
        file_system=file_system_name
    )
    file_client = file_system_client.get_file_client(full_path)

    file_client.upload_data(content, overwrite=True)

    return full_path


feeds_dict = {
    "gbfs_versions": "https://gbfs.api.ridedott.com/public/v2/gbfs_versions.json",
    "free_bike_status": "https://gbfs.api.ridedott.com/public/v2/budapest/free_bike_status.json",
    "geofencing_zones": "https://gbfs.api.ridedott.com/public/v2/budapest/geofencing_zones.json",
    "system_information": "https://gbfs.api.ridedott.com/public/v2/budapest/system_information.json",
    "system_pricing_plans": "https://gbfs.api.ridedott.com/public/v2/budapest/system_pricing_plans.json",
    "vehicle_types": "https://gbfs.api.ridedott.com/public/v2/budapest/vehicle_types.json",
    "station_information": "https://gbfs.api.ridedott.com/public/v2/budapest/station_information.json",
    "station_status": "https://gbfs.api.ridedott.com/public/v2/budapest/station_status.json",
}


async def fetch_url(session, url):
    """Fetch a single URL and return its status/content."""
    try:
        async with session.get(url, timeout=10) as response:
            # You can use .json() or .text() depending on the API
            data = await response.text()
            print(f"Finished {url}: {len(data)} chars")
            return data
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


async def fetch_all(urls):
    """Manager to handle the session and concurrent tasks."""
    async with aiohttp.ClientSession() as session:
        # TaskGroup is the modern (Python 3.11+) way to run tasks concurrently
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(fetch_url(session, url)) for url in urls]

        # Once the TaskGroup block exits, all tasks are complete
        return [task.result() for task in tasks]


async def save_to_blob_storage(data, filename):
    # Implement your logic to save data to Azure Blob Storage
    pass


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)

    connection_string = os.getenv("ADLS2_CONNECTION_STRING") or os.getenv(
        "AzureWebJobsStorage"
    )
    file_system_name = os.getenv("ADLS2_FILE_SYSTEM")
    adls_directory = os.getenv("ADLS2_DIRECTORY", "")

    if not connection_string:
        logging.error("Missing ADLS2_CONNECTION_STRING (or AzureWebJobsStorage).")
        return

    if not file_system_name:
        logging.error("Missing ADLS2_FILE_SYSTEM.")
        return

    urls = [
        feeds_dict["free_bike_status"],
        feeds_dict["station_status"],
        feeds_dict["station_information"],
    ]
    results = asyncio.run(fetch_all(urls))

    try:
        service_client = DataLakeServiceClient.from_connection_string(connection_string)

        uploaded = 0
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        for index, url in enumerate(urls):
            payload = results[index]
            if payload is None:
                logging.warning("Skipping upload because fetch failed: %s", url)
                continue

            feed_name = url.rstrip("/").split("/")[-1].replace(".json", "")
            file_name = f"{feed_name}_{timestamp}.json"
            destination_path = f"dott/{feed_name}/{file_name}"

            normalized_payload = payload
            try:
                normalized_payload = json.dumps(json.loads(payload), ensure_ascii=False)
            except Exception:
                pass

            uploaded_path = _upload_file(
                service_client=service_client,
                file_system_name=file_system_name,
                adls_directory=adls_directory,
                destination_path=destination_path,
                content=normalized_payload,
            )
            uploaded += 1
            logging.info("Uploaded %s -> %s", url, uploaded_path)

        logging.info(
            "Uploaded %s file(s) to ADLS2 filesystem '%s'.", uploaded, file_system_name
        )
    except Exception as exc:
        logging.exception("ADLS2 upload failed: %s", exc)
