import json
import boto3
from botocore.exceptions import ClientError


# ===================
# basic SNS Lambda
def lambda_handler(event, context):
    """
    AWS Lambda function to process SNS event data.

    Parameters:
    event (dict): Event data that triggered the Lambda function.
    context (LambdaContext): Runtime information about the Lambda function.

    Returns:
    str: The message extracted from the SNS event.
    """

    # Log the received event in a readable JSON format.
    print("Received event: " + json.dumps(event, indent=2))

    # Extract the SNS message from the event data.
    try:
        message = event["Records"][0]["Sns"]["Message"]
        print("From SNS: " + message)
    except KeyError as e:
        print(f"Key error: {e}")
        return f"Error: {e}"

    # Return the SNS message.
    return message


# simulate SNS event, data payload that might trigger the Lambda function
event = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:example-topic",
            "Sns": {
                "Type": "Notification",
                "MessageId": "11112222-3333-4444-5555-666677778888",
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:example-topic",
                "Subject": "example subject",
                "Message": "This is a test message from SNS",
                "Timestamp": "2024-05-21T12:34:56.789Z",
                "SignatureVersion": "1",
                "Signature": "EXAMPLE",
                "SigningCertUrl": "EXAMPLE",
                "UnsubscribeUrl": "EXAMPLE",
                "MessageAttributes": {},
            },
        }
    ]
}

lambda_handler(event, None)
