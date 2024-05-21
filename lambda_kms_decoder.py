"""
Lambda function designed to decrypt an encrypted value stored in an environment variable using AWS Key Management Service (KMS).

Modules used:

- logging: This module is used to log information, errors, and debug messages.
- os: This module is used to interact with the operating system, in this case, to access environment variables.
- boto3: This is the AWS SDK for Python, used here to interact with AWS services, specifically KMS.
- b64decode: This function from the base64 module is used to decode base64 encoded data.


Variable encryption and AWS Key Management Service (KMS) are critical in securing sensitive data. 

- Protecting Secrets and Credentials: Encrypting API keys, database credentials, and other sensitive configuration settings to ensure they are not exposed in plaintext in code or configuration files.
- Data Encryption: Encrypting sensitive data such as personal information, payment information, or other confidential data stored in databases or files.


"""

from base64 import b64decode, b64encode
import logging
import os
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Loading function")


def lambda_handler(event, context):
    try:
        # Validate environment variable
        encrypted = os.environ.get("ENCRYPTED_VALUE")
        if not encrypted:
            raise ValueError("ENCRYPTED_VALUE environment variable not set")

        # Initialize KMS client
        kms_client = boto3.client("kms")

        # Decode and decrypt the value
        decrypted_response = kms_client.decrypt(CiphertextBlob=b64decode(encrypted))
        decrypted = decrypted_response["Plaintext"].decode()

        # Log success without sensitive information
        logger.info("Decryption successful")

        return {"statusCode": 200, "body": "Decryption successful"}

    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error decrypting value: {str(e)}")
        return {"statusCode": 500, "body": "Error decrypting value"}
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return {"statusCode": 400, "body": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}


# ================================
# Secret rotation mechanism to securely rotate the encrypted value.
# Secret rotation ensures that sensitive data is kept secure over time

# Trigger Rotation: Use a CloudWatch event or manual trigger to rotate the secret.
# Encrypt New Secret: Encrypt a new secret value using KMS.
# Update Environment Variable: Update the Lambda environment variable with the new encrypted value.
# Use New Secret: Ensure the Lambda function uses the new secret in subsequent invocations


def lambda_rotate_secret(event, context):
    try:
        # Define new secret value (in practice, this should be generated securely)
        new_secret = "new-secret-value"

        # Initialize KMS client
        kms_client = boto3.client("kms")

        # Encrypt the new secret
        encrypted_response = kms_client.encrypt(
            KeyId=os.environ["KMS_KEY_ID"], Plaintext=new_secret.encode()
        )
        new_encrypted_value = b64encode(encrypted_response["CiphertextBlob"]).decode()

        # Update the Lambda environment variable (typically via AWS SDK)
        client = boto3.client("lambda")
        response = client.update_function_configuration(
            FunctionName=context.function_name,
            Environment={"Variables": {"ENCRYPTED_VALUE": new_encrypted_value}},
        )
        logger.info("Secret rotation successful")

        return {"statusCode": 200, "body": "Secret rotation successful"}

    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error rotating secret: {str(e)}")
        return {"statusCode": 500, "body": "Error rotating secret"}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}


# ================================
# Role-based decryption, where different IAM roles can decrypt different parts of the encrypted data based on their permissions.

# Multiple Encrypted Values: Store multiple encrypted values, each accessible by different roles.
# Role Identification: Identify the role of the Lambda function at runtime.
# Conditional Decryption: Decrypt the value based on the role of the Lambda function.


# Lambda Function with Role-Based Decryption
def lambda_handler_role_decrypt(event, context):
    try:
        # Identify the Lambda execution role
        execution_role = context.invoked_function_arn.split(":")[-1]

        # Map roles to environment variables
        role_to_env_var = {
            "role1": "ENCRYPTED_VALUE_ROLE1",
            "role2": "ENCRYPTED_VALUE_ROLE2",
        }

        if execution_role not in role_to_env_var:
            raise ValueError("Unauthorized role")

        encrypted = os.environ.get(role_to_env_var[execution_role])
        if not encrypted:
            raise ValueError("Encrypted value not found for this role")

        # Initialize KMS client
        kms_client = boto3.client("kms")

        # Decode and decrypt the value
        decrypted_response = kms_client.decrypt(CiphertextBlob=b64decode(encrypted))
        decrypted = decrypted_response["Plaintext"].decode()

        # Log success without sensitive information
        logger.info("Decryption successful")

        return {"statusCode": 200, "body": "Decryption successful"}

    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error decrypting value: {str(e)}")
        return {"statusCode": 500, "body": "Error decrypting value"}
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return {"statusCode": 400, "body": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}


# ======================================
# A Lambda function needs to connect to a database, and the database credentials are stored in an encrypted format using AWS KMS.


def lambda_handler_decrypt_db(event, context):
    """
    Lambda function to decrypt database credentials and connect to the database.
    """
    try:
        # Retrieve and validate the encrypted database credentials from environment variables
        encrypted_db_user = os.environ.get("ENCRYPTED_DB_USER")
        encrypted_db_password = os.environ.get("ENCRYPTED_DB_PASSWORD")
        if not encrypted_db_user or not encrypted_db_password:
            raise ValueError("Encrypted database credentials are not set")

        # Initialize AWS KMS client
        kms_client = boto3.client("kms")

        # Decrypt the database username
        decrypted_db_user_response = kms_client.decrypt(
            CiphertextBlob=b64decode(encrypted_db_user)
        )
        decrypted_db_user = decrypted_db_user_response["Plaintext"].decode()

        # Decrypt the database password
        decrypted_db_password_response = kms_client.decrypt(
            CiphertextBlob=b64decode(encrypted_db_password)
        )
        decrypted_db_password = decrypted_db_password_response["Plaintext"].decode()

        # Log successful decryption without sensitive information
        logger.info("Database credentials decrypted successfully")

        # Placeholder for database connection logic
        # connect_to_database(username=decrypted_db_user, password=decrypted_db_password)

        return {
            "statusCode": 200,
            "body": "Database credentials decrypted successfully",
        }

    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error decrypting database credentials: {str(e)}")
        return {"statusCode": 500, "body": "Error decrypting database credentials"}
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        return {"statusCode": 400, "body": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}


# ======================
# A Lambda function periodically rotates an API key used by an application, ensuring the new key is encrypted and stored securely.


def lambda_handler(event, context):
    """
    Lambda function to rotate an API key, encrypt it, and update the environment variable.
    """
    try:
        # Generate a new API key
        new_api_key = "API123456"

        # Initialize AWS KMS client
        kms_client = boto3.client("kms")

        # Encrypt the new API key
        encrypted_response = kms_client.encrypt(
            KeyId=os.environ["KMS_KEY_ID"],  # Ensure this environment variable is set
            Plaintext=new_api_key.encode(),
        )
        new_encrypted_api_key = b64encode(encrypted_response["CiphertextBlob"]).decode()

        # Update the Lambda environment variable with the new encrypted API key
        client = boto3.client("lambda")
        response = client.update_function_configuration(
            FunctionName=context.function_name,
            Environment={"Variables": {"ENCRYPTED_API_KEY": new_encrypted_api_key}},
        )
        logger.info("API key rotation successful")

        return {"statusCode": 200, "body": "API key rotation successful"}

    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error rotating API key: {str(e)}")
        return {"statusCode": 500, "body": "Error rotating API key"}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}
