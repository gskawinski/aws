""" 

Amazon Simple Storage Service (Amazon S3) is a scalable object storage service that provides industry-leading performance, data availability, security, and scalability. It is designed to store and retrieve any amount of data from anywhere on the web.

S3 Bucket Attributes

1. Bucket Name
A unique name to identify your bucket. It must follow DNS-compliant naming conventions.

2. Region
The AWS region where the bucket is created. Choosing the right region can help minimize latency and costs.

3. Access Control
Control who can access your bucket and its objects using:

- Bucket Policies: JSON-based policies that define what actions are allowed or denied.
- Access Control Lists (ACLs): Define access permissions for objects.
- IAM Policies: Control access using IAM roles and users.

4. Versioning
Enables multiple versions of objects in the same bucket, providing protection against accidental overwrites and deletions.

5. Object Lock
Protects objects from being deleted or overwritten for a fixed amount of time or indefinitely.

6. Logging
Enables server access logging to track requests for access to your bucket.

7. Encryption
S3 supports several encryption options:

- Server-Side Encryption (SSE): Encrypts data at rest using Amazon S3-managed keys (SSE-S3), AWS KMS keys (SSE-KMS), or customer-provided keys (SSE-C).
- Client-Side Encryption: Encrypt data client-side before uploading it to S3.

8. Storage Classes
S3 offers different storage classes to optimize cost:

- S3 Standard: High durability, availability, and performance for frequently accessed data.
- S3 Intelligent-Tiering: Moves objects between two access tiers when access patterns change.
- S3 Standard-IA (Infrequent Access): For data accessed less frequently but requires rapid access.
- S3 One Zone-IA: For infrequently accessed data stored in a single Availability Zone.
- S3 Glacier: Low-cost storage designed for data archiving, retrieval times range from minutes to hours.
- S3 Glacier Deep Archive: Lowest-cost storage, designed for data that can be retained for years and accessed infrequently.

9. Lifecycle Policies
Automate the transition of objects to different storage classes or the deletion of objects after a specified period.

10. Cross-Region Replication (CRR)
Automatically replicates objects from one bucket to another bucket in a different AWS region.

11. Transfer Acceleration
Enables faster upload and download of objects using Amazon CloudFront’s globally distributed edge locations.

12. Event Notifications
Set up notifications to trigger workflows, such as invoking an AWS Lambda function, sending a message to an Amazon SQS queue, or publishing to an SNS topic when specific events occur.

13. Object Metadata
Key-value pairs associated with objects to store additional information.

14. Tags
Assign key-value pairs to your S3 bucket for easier management and cost allocation.


DOCS
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

"""

# This set of functions provides basic functionalities like creating buckets, listing buckets, uploading and downloading files, deleting buckets, and listing objects within a bucket.

import boto3
import botocore.exceptions


def create_bucket(bucket_name, region="us-east-1"):
    """
    Create an S3 bucket with the specified name in the specified region.

    Parameters:
    - bucket_name: The name of the bucket to create.
    - region: The AWS region where the bucket will be created. Default is 'us-east-1'.

    Returns:
    - True if the bucket is created successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3", region_name=region)
        s3.create_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        print(f"Error creating bucket: {e}")
        return False


def list_buckets():
    """
    List all S3 buckets in the AWS account.

    Returns:
    - A list of bucket names.
    """
    try:
        s3 = boto3.client("s3")
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        return buckets
    except botocore.exceptions.ClientError as e:
        print(f"Error listing buckets: {e}")
        return []


def upload_file(bucket_name, file_path, object_key):
    """
    Upload a file to an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket where the file will be uploaded.
    - file_path: The local path of the file to upload.
    - object_key: The key (name) of the object in the bucket.

    Returns:
    - True if the file is uploaded successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        s3.upload_file(file_path, bucket_name, object_key)
        return True
    except botocore.exceptions.ClientError as e:
        print(f"Error uploading file: {e}")
        return False


def download_file(bucket_name, object_key, file_path):
    """
    Download a file from an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket where the file is stored.
    - object_key: The key (name) of the object in the bucket.
    - file_path: The local path where the file will be downloaded.

    Returns:
    - True if the file is downloaded successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        s3.download_file(bucket_name, object_key, file_path)
        return True
    except botocore.exceptions.ClientError as e:
        print(f"Error downloading file: {e}")
        return False


def delete_bucket(bucket_name):
    """
    Delete an S3 bucket and all its contents.

    Parameters:
    - bucket_name: The name of the bucket to delete.

    Returns:
    - True if the bucket is deleted successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        s3.delete_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        print(f"Error deleting bucket: {e}")
        return False


def list_objects(bucket_name):
    """
    List all objects in an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket.

    Returns:
    - A list of object keys (names).
    """
    try:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            objects = [obj["Key"] for obj in response["Contents"]]
            return objects
        else:
            return []
    except botocore.exceptions.ClientError as e:
        print(f"Error listing objects: {e}")
        return []


# You can implement other functions for additional S3 features such as object versioning, lifecycle policies, etc.


# In Amazon S3, there are no "folders" in the traditional sense, but rather, you can mimic folder-like structures by using object keys with slashes ("/") in their names. These slashes are interpreted as delimiters, allowing you to organize objects hierarchically. When you use the AWS Management Console or SDKs like Boto3 to interact with S3, you'll see these objects displayed as if they were in folders.

# To create a "folder" (or a prefix) within a bucket, you simply need to upload an object with a key that ends with a slash ("/").


def create_folder(bucket_name, folder_name):
    """
    Create a folder (prefix) within an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket where the folder will be created.
    - folder_name: The name of the folder (prefix) to create.

    Returns:
    - True if the folder is created successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        # Add a trailing slash to mimic a folder structure
        folder_key = f"{folder_name}/"
        s3.put_object(Bucket=bucket_name, Key=folder_key)
        return True
    except Exception as e:
        print(f"Error creating folder: {e}")
        return False


# Example usage:
bucket_name = "your-bucket-name"
folder_name = "folder/subfolder/"
if create_folder(bucket_name, folder_name):
    print(f"Folder '{folder_name}' created successfully in bucket '{bucket_name}'.")
else:
    print(f"Failed to create folder '{folder_name}' in bucket '{bucket_name}'.")


def enable_versioning(bucket_name):
    """
    Enable versioning for an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket for which to enable versioning.

    Returns:
    - True if versioning is enabled successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        s3.put_bucket_versioning(
            Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"}
        )
        return True
    except Exception as e:
        print(f"Error enabling versioning: {e}")
        return False


# Example usage:
bucket_name = "your-bucket-name"
if enable_versioning(bucket_name):
    print(f"Versioning enabled for bucket '{bucket_name}'.")
else:
    print(f"Failed to enable versioning for bucket '{bucket_name}'.")


def set_lifecycle_policy(bucket_name, prefix, days_to_expire):
    """
    Set a lifecycle policy for objects in an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket for which to set the lifecycle policy.
    - prefix: The prefix (folder) to which the lifecycle policy applies.
    - days_to_expire: The number of days after which objects with the specified prefix will expire.

    Returns:
    - True if the lifecycle policy is set successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        lifecycle_config = {
            "Rules": [
                {
                    "Prefix": prefix,
                    "Status": "Enabled",
                    "Expiration": {"Days": days_to_expire},
                }
            ]
        }
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
        )
        return True
    except Exception as e:
        print(f"Error setting lifecycle policy: {e}")
        return False


# Example usage:
bucket_name = "your-bucket-name"
prefix = "folder/"
days_to_expire = 30
if set_lifecycle_policy(bucket_name, prefix, days_to_expire):
    print(f"Lifecycle policy set for prefix '{prefix}' in bucket '{bucket_name}'.")
else:
    print(
        f"Failed to set lifecycle policy for prefix '{prefix}' in bucket '{bucket_name}'."
    )


def set_lifecycle_policy(bucket_name, prefix, days_to_expire):
    """
    Set a lifecycle policy for objects in an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket for which to set the lifecycle policy.
    - prefix: The prefix (folder) to which the lifecycle policy applies.
    - days_to_expire: The number of days after which objects with the specified prefix will expire.

    Returns:
    - True if the lifecycle policy is set successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        lifecycle_config = {
            "Rules": [
                {
                    "Prefix": prefix,
                    "Status": "Enabled",
                    "Expiration": {"Days": days_to_expire},
                }
            ]
        }
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
        )
        return True
    except Exception as e:
        print(f"Error setting lifecycle policy: {e}")
        return False


# Example usage:
bucket_name = "your-bucket-name"
prefix = "folder/"
days_to_expire = 30
if set_lifecycle_policy(bucket_name, prefix, days_to_expire):
    print(f"Lifecycle policy set for prefix '{prefix}' in bucket '{bucket_name}'.")
else:
    print(
        f"Failed to set lifecycle policy for prefix '{prefix}' in bucket '{bucket_name}'."
    )


def enable_server_side_encryption(bucket_name):
    """
    Enable server-side encryption for an S3 bucket.

    Parameters:
    - bucket_name: The name of the bucket for which to enable server-side encryption.

    Returns:
    - True if server-side encryption is enabled successfully, False otherwise.
    """
    try:
        s3 = boto3.client("s3")
        s3.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
        )
        return True
    except Exception as e:
        print(f"Error enabling server-side encryption: {e}")
        return False


# Example usage:
bucket_name = "your-bucket-name"
if enable_server_side_encryption(bucket_name):
    print(f"Server-side encryption enabled for bucket '{bucket_name}'.")
else:
    print(f"Failed to enable server-side encryption for bucket '{bucket_name}'.")


bucket_name = "unique-bucket-name"
region = "us-east-1"

if create_bucket(bucket_name, region):
    print(f"Bucket '{bucket_name}' created successfully.")
else:
    print(f"Failed to create bucket '{bucket_name}'.")


buckets = list_buckets()
if buckets:
    print("Buckets in your account:")
    for bucket in buckets:
        print(f" - {bucket}")
else:
    print("No buckets found or error listing buckets.")


bucket_name = "my-unique-bucket-name"
file_path = "/path/to/your/local/file.txt"
object_key = "uploads/file.txt"

if upload_file(bucket_name, file_path, object_key):
    print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_key}'.")
else:
    print(f"Failed to upload file '{file_path}' to bucket '{bucket_name}'.")


bucket_name = "my-unique-bucket-name"
object_key = "uploads/file.txt"
download_path = "/path/to/your/local/downloaded_file.txt"

if download_file(bucket_name, object_key, download_path):
    print(
        f"File '{object_key}' downloaded from bucket '{bucket_name}' to '{download_path}'."
    )
else:
    print(f"Failed to download file '{object_key}' from bucket '{bucket_name}'.")


bucket_name = "my-unique-bucket-name"

objects = list_objects(bucket_name)
if objects:
    print(f"Objects in bucket '{bucket_name}':")
    for obj in objects:
        print(f" - {obj}")
else:
    print(f"No objects found in bucket '{bucket_name}' or error listing objects.")


bucket_name = "my-unique-bucket-name"
folder_name = "raw-data/"

if create_folder(bucket_name, folder_name):
    print(f"Folder '{folder_name}' created successfully in bucket '{bucket_name}'.")
else:
    print(f"Failed to create folder '{folder_name}' in bucket '{bucket_name}'.")


# Create a bucket
bucket_name = "my-data-bucket"
create_bucket(bucket_name)

# Create folders for raw data and processed data
create_folder(bucket_name, "raw-data")
create_folder(bucket_name, "processed-data")

import json
import csv
import os
from io import StringIO
import boto3


def process_json_to_csv(bucket_name, json_key, csv_key):
    """
    Process a JSON file from the 'raw-data' folder, convert it to CSV, and save it in the 'processed-data' folder.

    Parameters:
    - bucket_name: The name of the bucket.
    - json_key: The key (name) of the JSON object in the 'raw-data' folder.
    - csv_key: The key (name) of the CSV object to save in the 'processed-data' folder.

    Returns:
    - True if the processing and saving are successful, False otherwise.
    """
    try:
        s3 = boto3.client("s3")

        # Download the JSON file
        json_obj = s3.get_object(Bucket=bucket_name, Key=json_key)
        json_content = json_obj["Body"].read().decode("utf-8")
        json_data = json.loads(json_content)

        # Process JSON data and convert to CSV
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Assuming JSON data is a list of dictionaries
        if json_data:
            header = json_data[0].keys()
            csv_writer.writerow(header)
            for row in json_data:
                csv_writer.writerow(row.values())

        # Upload the CSV file to S3
        s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
        return True

    except Exception as e:
        print(f"Error processing JSON to CSV: {e}")
        return False


# Example usage:
bucket_name = "my-data-bucket"
json_key = "raw-data/example.json"
csv_key = "processed-data/example.csv"

if process_json_to_csv(bucket_name, json_key, csv_key):
    print(f"JSON file '{json_key}' processed and saved as CSV '{csv_key}'.")
else:
    print(f"Failed to process JSON file '{json_key}'.")


# Upload a JSON file to the raw-data folder
bucket_name = "my-data-bucket"
local_json_file = "/path/to/your/local/example.json"
json_key = "raw-data/example.json"
upload_file(bucket_name, local_json_file, json_key)

# Process the JSON file and save it as a CSV in the processed-data folder
csv_key = "processed-data/example.csv"
process_json_to_csv(bucket_name, json_key, csv_key)
