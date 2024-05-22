import boto3
import json


def lambda_handler(event, context):
    """
    AWS Lambda function that invokes another Lambda function.

    Parameters:
    event (dict): A dictionary containing event data. The structure can be customized as needed.
    context (LambdaContext): A context object that provides methods and properties that provide information about the invocation, function, and runtime environment.

    Returns:
    dict: Response from the invoked Lambda function or an error message.
    """

    # Initialize the boto3 client for Lambda
    lambda_client = boto3.client("lambda")

    # The name of the Lambda function to invoke
    target_lambda_function_name = "target-lambda-function-name-ARN"
    # target_lambda_function_name="arn:aws:lambda:us-east-1:1236547899871:function:my_function_to_Invoke",

    # Payload to send to the target Lambda function
    payload = {"key1": "value1", "key2": "value2"}

    try:
        # Invoke the target Lambda function
        response = lambda_client.invoke(
            FunctionName=target_lambda_function_name,
            InvocationType="RequestResponse",  # Can be 'Event' for async invocation,, RequestResponse = sync invocation
            Payload=json.dumps(payload),
        )

        # Read the response payload
        response_payload = json.load(response["Payload"])

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Invocation successful", "response": response_payload}
            ),
        }

    except Exception as e:
        # Log the exception and return an error response
        print(f"Error invoking Lambda function: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Invocation failed", "error": str(e)}),
        }


# =================
# ETL using lambda invoke lambda
# An ETL (Extract, Transform, Load) pipeline can leverage AWS Lambda functions to perform various steps in the data pipeline.

import boto3
import json


def lambda_handler_extract(event, context):
    """
    Extract data from a source and invoke the Transform function.
    """
    lambda_client = boto3.client("lambda")
    transform_function_name = (
        "arn:aws:lambda:us-east-1:1236547899871:function:TransformFunctionName_ARN"
    )

    # Simulate data extraction
    extracted_data = {"data": [{"id": 1, "value": 10}, {"id": 2, "value": 20}]}

    try:
        response = lambda_client.invoke(
            FunctionName=transform_function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(extracted_data),
        )

        response_payload = json.load(response["Payload"])

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Extracted data sent to transform function",
                    "response": response_payload,
                }
            ),
        }

    except Exception as e:
        print(f"Error invoking transform function: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": "Failed to invoke transform function", "error": str(e)}
            ),
        }


import boto3
import json


def lambda_handler_transform(event, context):
    """
    Transform the extracted data and invoke the Load function.
    """
    lambda_client = boto3.client("lambda")
    load_function_name = (
        "arn:aws:lambda:us-east-1:1236547899871:function:LoadFunctionName"
    )

    # Extracted data from event
    extracted_data = event.get("data", [])

    # Simulate data transformation
    transformed_data = []
    for item in extracted_data:
        transformed_data.append(
            {"id": item["id"], "transformed_value": item["value"] * 2}
        )

    try:
        response = lambda_client.invoke(
            FunctionName=load_function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({"data": transformed_data}),
        )

        response_payload = json.load(response["Payload"])

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Transformed data sent to load function",
                    "response": response_payload,
                }
            ),
        }

    except Exception as e:
        print(f"Error invoking load function: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": "Failed to invoke load function", "error": str(e)}
            ),
        }


import json


def lambda_handler_load(event, context):
    """
    Load the transformed data into the target destination.
    """
    # Transformed data from event
    transformed_data = event.get("data", [])

    try:
        # Simulate loading data into the target destination (e.g., a database)
        for item in transformed_data:
            print(f"Loading item: {item}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Data loaded successfully", "loaded_data": transformed_data}
            ),
        }

    except Exception as e:
        print(f"Error loading data: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to load data", "error": str(e)}),
        }
