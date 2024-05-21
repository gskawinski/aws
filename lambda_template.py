"""
AWS Lambda is a serverless computing service that allows you to run code without provisioning or managing servers. 

AWS Lambda integrates with other AWS services to invoke functions or take other actions. These are some common use cases:

- Invoke a function in response to resource lifecycle events, such as with Amazon S3. 
This is Using AWS Lambda with Amazon S3. For example when an object upload to S3 bucket, this event can be trigger a lambda function.
- Respond to incoming HTTP requests. This is using Lambda with API Gateway. 
Typical REST API use cases can be trigger API gateway and respond back to client.
- Consume events from a queue. This is Using Lambda with Amazon SQS. Lambda poll queue records from Amazon SQS.
- Run a function on a schedule. This is Using AWS Lambda with Amazon EventBridge CloudWatch Events.

When an AWS Lambda function is invoked, two main objects are passed to the function: the event and the context. 

- The event object contains the data that triggers the Lambda function. It includes information about the event source and the actual data payload that needs processing. The structure of the event object varies depending on the source of the event, such as an API Gateway, S3 bucket, DynamoDB stream, etc.
- The context object provides information about the Lambda function's execution environment. This includes metadata about the invocation, function, and execution context.

The event object is highly dynamic and varies based on the triggering source, whereas the context object is more static and provides metadata about the function execution environment. Both are crucial for the effective operation and debugging of AWS Lambda functions.

The Lambda function handler is the method in your function code that processes events.
When your function is invoked, Lambda runs the handler method.
Your function runs until the handler returns a response, exits, or times out.
The event parameter contains information from the invoking service (usually a Python dictionary).
The context parameter provides information about the invocation, function, and runtime environment

After triggering event to lambda function lambda launch the execution environment with different language and runtimes, you can basically develop any language and runtime into lambda code.
After execution, lambda has destinations that can be interaction with your function code it depends your function business logic and business requirements.

Returning a Value:
Optionally, a handler can return a value.
What happens to the returned value depends on the invocation type and the service that invoked the function.

If you use the RequestResponse invocation type (synchronous invocation), AWS Lambda returns the result of the Python function call to the client invoking the Lambda function (serialized into JSON).
If the handler returns objects that can’t be serialized by json.dumps, the runtime returns an error.

- https://aws-lambda-for-python-developers.readthedocs.io/en/latest/02_event_and_context/
- https://docs.aws.amazon.com/lambda/latest/dg/lambda-services.html#eventsources
- https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventsourcemapping.html
- https://docs.powertools.aws.dev/lambda/python/latest/
- https://docs.aws.amazon.com/lambda/latest/dg/lambda-samples.html
- https://hands-on.cloud/aws-lambda-use-cases-examples/

Example of events data
- Amazon S3 Events: Records: A list of S3 event records, each containing information about the event (e.g., bucket name, object key, event type).
- Amazon DynamoDB Streams: Records: A list of DynamoDB stream records, each representing a change to an item in the table.
- Amazon Kinesis Streams: Records: A list of Kinesis stream records, each containing data from a shard.
- Amazon SQS (Simple Queue Service): Records: A list of SQS message records, each containing message attributes and body.
- Amazon EventBridge (formerly CloudWatch Events): 
    source: The source of the event (e.g., “aws.s3”, “aws.dynamodb”, etc.).
    detail-type: The specific type of event (e.g., “ObjectCreated”, “ObjectRemoved”, etc.)
- HTTP Endpoints (via Amazon API Gateway): Request parameters, headers, and body.

AWS Lambda Context:
When Lambda runs your function, it passes a context object to the handler. This context object provides methods and properties that offer information about the invocation, function, and execution environment. Here are some key properties and methods available in the context object:

- function_name: The name of the Lambda function.
- function_version: The version of the function.
- invoked_function_arn: The Amazon Resource Name (ARN) used to invoke the function.
- memory_limit_in_mb: The allocated memory for the function.
- aws_request_id: The identifier of the invocation request.
- log_group_name: The log group for the function.
- log_stream_name: The log stream for the function instance.
- identity (for mobile apps): Information about the Amazon Cognito identity that authorized the request.
- client_context (for mobile apps): Client context provided by the client application.
- custom: A dictionary of custom values set by the mobile client application.
- env: A dictionary of environment information provided by the AWS SDK.


Using Environment Variables in AWS Lambda:
Environment variables allow you to hide sensitive information or/and adjust your function’s behavior without updating your code.

When developing locally, you can use the dotenv library to load environment variables from a .env file. Install the library (pip install python-dotenv) and create a .env file with your key-value pairs. Then, use the following code:
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

If your environment variables are encrypted, you can use the AWS SDK (Boto3) to retrieve them securely:

def get_encrypted_environment_variables(function_name):
    lambda_client = boto3.client('lambda')
    response = lambda_client.get_function_configuration(FunctionName=function_name)
    encrypted_variables = response.get('Environment', {}).get('EncryptedVariables', {})
    return encrypted_variables

# Example usage
my_function_name = 'my-function'
encrypted_env_vars = get_encrypted_environment_variables(my_function_name)
print(f"Encrypted environment variables: {encrypted_env_vars}")

"""

import json
import time

import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

import os

# Using Environment Variables defined in Configuration =>  Environment variables =>  Edit and add your environment variables (key-value pairs).
my_variable = os.environ.get("MY_ENV_VARIABLE")
print(f"Value of MY_ENV_VARIABLE: {my_variable}")

import boto3

REGION = os.environ.get("REGION")
s3 = boto3.resource("s3", region_name=REGION)


# The lambda_handler function is the entry point for the Lambda function
def lambda_handler(event, context):
    """
    AWS Lambda function entry point.

    Args:
        event (dict): Event data passed to the function.
        context (object): AWS Lambda context object.

    Returns:
        dict: Response data.
    """
    try:

        # Print the EVENT key/values in a loop
        for key, value in event.items():
            print(f"Field '{key}' has value: {value}")

        # Print the CONTEXT key/values in a loop
        for key, value in context.items():
            print(f"Field '{key}' has value: {value}")

        # Get data from event
        body_var = event.get("field", "")
        body_str = event.get("body", "{}")
        body_obj = json.loads(body_str)

        # Get the Records from event
        records = event.get("Records", [])
        # Processing Records in a Loop
        for record in records:
            # Your processing logic here
            print("Processing record:", record)

        # Your business logic here

        # Add logs data
        LOGGER.info("Event structure: %s", event)

        response = {"message": "Return from AWS Lambda"}

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/plain"},
            "body": json.dumps(response),
        }

    except Exception as e:
        # Handle exceptions gracefully
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "text/plain"},
            "body": json.dumps({"error": str(e)}),
        }


# Important :
# - Remember to configure your IAM role with necessary permissions. Bu default Lambda has only accees to upload Logs to Cloud Watch
# - Add environment variables, layers, and other settings as needed, to hide sensitive data. Use os module
# - For external python modules create Lambda Deployment Packege, ZIP the folder and upload, by CLI, or to S3.
# - For more details, refer to the official AWS Lambda documentation.
#   DOCS = > https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html


# ====== Write the Function Inside AWS Editor
# This Option can only be used when when your lambda doesn’t need any libraries apart from the default libraries provided by AWS.
# Some important default ones are boto3, requests, json, os, sys, secrets, math, logging.
# LIST : https://insidelambda.com/ or https://gist.github.com/sjehutch/36493ff674b1b9a16fc44a5fc270760c
# In addition to the default Python Standard Library, some third-party packages are embedded in AWS Lambda Python runtimes
# https://www.feitsui.com/en/article/2

# ====== Create Deployment Package in your code editor
# 2. Write the lambda Function in your code editor, zip it and upload the zip to AWS Lambda upload. This is Deployment Package
# This option is the only choice when your code uses external libraries. Example pymongo to query MongoDB database,
# https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

# Create a file called lambda_function.py and write the function which will receive the events/ which will be called, name it as
# def lambda_handler(event, context):
# Note : By default AWS calls the lambda_handler as lambda_function.lambda_handler. This configuration can be overwritten, but requires
# Download the package you want to use with pip, specifying the location ; a new folder which is at the root of lambda_function.py. Name it ‘dependencies’ (not mandatory, can be named as you like).
# zip the dependencies folder and then zip the top most folder, the folder in which both dependencies and lambda_function.py are living. At this stage you have a nested zip folder structure.
# Upload the top-most folder in AWS Lambda.

# 3. Use small library called juniper to automate the packaging of code for AWS lambda functions.
# https://github.com/eabglobal/juniper
# All you need to do to use juniper is create a small manifest.yml file that looks like:

# functions:
#   # Name the zip file you want juni to create
#   router:
#     # Where are your dependencies located?
#     requirements: ./src/requirements.txt.
#     # Your source code.
#     include:
#     - ./src/lambda_function.py
# As long as you have your dependencies in the requirements.txt, juniper will package them for you. There are a few examples in our code base that showcase the features of juniper.


# AWS Lambda Layers allow you to manage your code and dependencies separately from your function code, enabling you to share and reuse them across multiple Lambda functions. Layers can contain libraries, a custom runtime, or other dependencies.

# mkdir -p python/lib/python3.11/site-packages
# pip install -t python/lib/python3.11/site-packages requests

# my-layer/
# ├── python/
# │   ├── lib/
# │   │   └── python3.11/
# │   │       ├── my_dependency/
# │   │       └── ...
# └── layer.zip
