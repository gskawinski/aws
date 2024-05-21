import json
import boto3
from botocore.exceptions import ClientError


# =========================
# a Lambda function that processes an SNS message and sends a welcome email to the user who just signed up. This example uses the AWS SDK for Python (Boto3) to send an email via Amazon SES (Simple Email Service).

# Initialize the SES client
ses_client = boto3.client("ses", region_name="us-east-1")


def lambda_handler(event, context):
    """
    AWS Lambda function to process SNS messages and send welcome emails via SES.

    Parameters:
    event (dict): Event data that triggered the Lambda function.
    context (LambdaContext): Runtime information about the Lambda function.

    Returns:
    dict: Response indicating success or failure.
    """

    # Log the received event
    print("Received event: " + json.dumps(event, indent=2))

    try:
        # Extract the SNS message
        sns_message = event["Records"][0]["Sns"]["Message"]
        print("From SNS: " + sns_message)

        # Parse the SNS message (assuming it's a JSON string)
        message_data = json.loads(sns_message)
        user_email = message_data["email"]
        user_name = message_data["name"]

        # Send a welcome email
        send_welcome_email(user_email, user_name)

        return {
            "statusCode": 200,
            "body": json.dumps("Welcome email sent successfully!"),
        }
    except KeyError as e:
        print(f"Key error: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps(f"Error processing SNS message: {e}"),
        }
    except ClientError as e:
        print(f"Client error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                f"Error sending email: {e.response['Error']['Message']}"
            ),
        }
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Unexpected error: {e}")}


def send_welcome_email(email, name):
    """
    Send a welcome email to the new user.

    Parameters:
    email (str): The email address of the new user.
    name (str): The name of the new user.

    Returns:
    None
    """

    # Email content
    sender = "no-reply@example.com"
    subject = "Welcome to Our Service!"
    body_text = (
        f"Dear {name},\n\nWelcome to our service! We're excited to have you on board."
    )
    body_html = f"""<html>
    <head></head>
    <body>
      <h1>Welcome to Our Service, {name}!</h1>
      <p>We're excited to have you on board.</p>
    </body>
    </html>"""

    # Email parameters
    charset = "UTF-8"
    response = ses_client.send_email(
        Source=sender,
        Destination={
            "ToAddresses": [email],
        },
        Message={
            "Subject": {"Data": subject, "Charset": charset},
            "Body": {
                "Text": {"Data": body_text, "Charset": charset},
                "Html": {"Data": body_html, "Charset": charset},
            },
        },
    )

    # Log the response
    print(f"Email sent! Message ID: {response['MessageId']}")


# simulate event
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
                "Subject": "User Signup",
                "Message": '{"email": "newuser@example.com", "name": "New User"}',
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
# test lambda with test payload
lambda_handler(event, None)
