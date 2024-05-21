"""

AWS Lambda function in Python that is triggered by an S3 event when a new file is uploaded to a source bucket. The function reads the file, converts it to JSON format, and uploads it to a destination bucket.

"""

import json
import boto3
import csv
import os

# Define source and destination bucket names
SOURCE_BUCKET = "source-bucket"
DESTINATION_BUCKET = "destination-bucket"

# Initialize S3 client
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    AWS Lambda handler function. Triggered by S3 event when a new file is uploaded.
    Reads the file from the source bucket, converts it to JSON format, and uploads it
    to the destination bucket.

    :param event: The event data that triggered the function.
    :param context: Runtime information of the Lambda function.
    """
    # Get the bucket name and file key from the event
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    # Ensure the function is only triggered by the source bucket
    if source_bucket != SOURCE_BUCKET:
        print(f"Event from unexpected bucket: {source_bucket}")
        return

    # Download the file from the source bucket
    download_path = f"/tmp/{os.path.basename(file_key)}"
    s3_client.download_file(source_bucket, file_key, download_path)

    # Determine the file type and read the file
    file_extension = os.path.splitext(file_key)[1].lower()
    if file_extension in [".csv", ".tsv", ".txt"]:
        data = read_delimited_file(download_path, file_extension)
    else:
        print(f"Unsupported file type: {file_extension}")
        return

    # Convert data to JSON
    json_data = json.dumps(data, indent=4)

    # Upload the JSON file to the destination bucket
    destination_key = f"{os.path.splitext(file_key)[0]}.json"
    upload_path = f"/tmp/{os.path.basename(destination_key)}"
    with open(upload_path, "w") as json_file:
        json_file.write(json_data)

    s3_client.upload_file(upload_path, DESTINATION_BUCKET, destination_key)
    print(
        f"File {file_key} successfully converted to JSON and uploaded to {DESTINATION_BUCKET}/{destination_key}"
    )


def read_delimited_file(file_path, extension):
    """
    Reads a delimited file (CSV, TSV, TXT) and converts it to a list of dictionaries.

    :param file_path: The path to the file to be read.
    :param extension: The file extension to determine the delimiter.
    :return: A list of dictionaries representing the file data.
    """
    delimiter = ","
    if extension == ".tsv":
        delimiter = "\t"
    elif extension == ".txt":
        delimiter = None  # Auto-detect delimiter for .txt files

    with open(file_path, mode="r", newline="") as file:
        if delimiter:
            reader = csv.DictReader(file, delimiter=delimiter)
        else:
            # If delimiter is None, use csv.Sniffer to detect it
            sample = file.read(1024)
            file.seek(0)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            reader = csv.DictReader(file, dialect=dialect)

        data = [row for row in reader]
    return data


# Local testing version

import json
import os
import time
import csv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

# Define source and destination directories
SOURCE_DIR = "./source"
DESTINATION_DIR = "./destination"


def process_file(file_path):
    """
    Reads a file from the source directory, converts it to JSON format, and saves it
    to the destination directory with a timestamp in the filename.

    :param file_path: The path to the file to be processed.
    """
    file_name = os.path.basename(file_path)
    file_extension = os.path.splitext(file_name)[1].lower()

    # Determine the file type and read the file
    if file_extension in [".csv", ".tsv", ".txt"]:
        data = read_delimited_file(file_path, file_extension)
    else:
        LOGGER.warning(f"Unsupported file type: {file_extension}")
        return

    # Convert data to JSON
    json_data = json.dumps(data, indent=4)

    # Create destination file path with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    destination_file_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.json"
    destination_file_path = os.path.join(DESTINATION_DIR, destination_file_name)

    # Write JSON data to the destination file
    with open(destination_file_path, "w") as json_file:
        json_file.write(json_data)

    LOGGER.info(
        f"File {file_name} successfully converted to JSON and saved to {destination_file_path}"
    )


def read_delimited_file(file_path, extension):
    """
    Reads a delimited file (CSV, TSV, TXT) and converts it to a list of dictionaries.

    :param file_path: The path to the file to be read.
    :param extension: The file extension to determine the delimiter.
    :return: A list of dictionaries representing the file data.
    """
    delimiter = ","
    if extension == ".tsv":
        delimiter = "\t"
    elif extension == ".txt":
        delimiter = None  # Auto-detect delimiter for .txt files

    with open(file_path, mode="r", newline="") as file:
        if delimiter:
            reader = csv.DictReader(file, delimiter=delimiter)
        else:
            # If delimiter is None, use csv.Sniffer to detect it
            sample = file.read(1024)
            file.seek(0)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            reader = csv.DictReader(file, dialect=dialect)

        data = [row for row in reader]
    return data


def scan_directory(directory):
    """
    Scans the specified directory for files and processes new files.

    :param directory: The path to the directory to be scanned.
    """
    processed_files = set()

    while True:
        current_files = set(os.listdir(directory))
        new_files = current_files - processed_files

        for file_name in new_files:
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                LOGGER.info(f"New file detected: {file_name}")
                process_file(file_path)

        processed_files.update(new_files)
        time.sleep(5)


if __name__ == "__main__":
    if not os.path.exists(DESTINATION_DIR):
        os.makedirs(DESTINATION_DIR)

    LOGGER.info("Starting directory scan...")
    scan_directory(SOURCE_DIR)
