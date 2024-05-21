import json
import boto3
from botocore.exceptions import ClientError

# Initialize clients
autoscaling_client = boto3.client("autoscaling", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")


def lambda_handler(event, context):
    """
    AWS Lambda function to respond to CloudWatch alarm notifications.

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
        alarm_name = message_data["AlarmName"]
        new_state = message_data["NewStateValue"]
        reason = message_data["NewStateReason"]

        if new_state == "ALARM":
            # Scale up the Auto Scaling group
            scale_auto_scaling_group("my-auto-scaling-group", 5)
            # Notify administrators
            notify_administrators(alarm_name, reason)

        return {"statusCode": 200, "body": json.dumps("Alarm processed successfully!")}
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
            "body": json.dumps(f"Error: {e.response['Error']['Message']}"),
        }
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Unexpected error: {e}")}


def scale_auto_scaling_group(group_name, desired_capacity):
    """
    Scale the specified Auto Scaling group to the desired capacity.

    Parameters:
    group_name (str): The name of the Auto Scaling group.
    desired_capacity (int): The desired number of instances.

    Returns:
    None
    """
    response = autoscaling_client.set_desired_capacity(
        AutoScalingGroupName=group_name,
        DesiredCapacity=desired_capacity,
        HonorCooldown=True,
    )
    print(f"Auto Scaling group '{group_name}' scaled to {desired_capacity} instances.")


def notify_administrators(alarm_name, reason):
    """
    Send a notification email to administrators.

    Parameters:
    alarm_name (str): The name of the CloudWatch alarm.
    reason (str): The reason for the alarm state change.

    Returns:
    None
    """
    sender = "no-reply@example.com"
    recipient = "admin@example.com"
    subject = f"CloudWatch Alarm: {alarm_name} in ALARM state"
    body_text = f"Alarm {alarm_name} is in ALARM state. Reason: {reason}"
    body_html = f"""<html>
    <head></head>
    <body>
      <h1>CloudWatch Alarm: {alarm_name} in ALARM state</h1>
      <p>Reason: {reason}</p>
    </body>
    </html>"""

    charset = "UTF-8"
    response = ses_client.send_email(
        Source=sender,
        Destination={
            "ToAddresses": [recipient],
        },
        Message={
            "Subject": {"Data": subject, "Charset": charset},
            "Body": {
                "Text": {"Data": body_text, "Charset": charset},
                "Html": {"Data": body_html, "Charset": charset},
            },
        },
    )

    print(f"Notification email sent! Message ID: {response['MessageId']}")


# Simulated SNS Event Data, to represents a CloudWatch alarm notification.

alarm = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:example-topic",
            "Sns": {
                "Type": "Notification",
                "MessageId": "11112222-3333-4444-5555-666677778888",
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:example-topic",
                "Subject": 'ALARM: "ExampleAlarmName" in US East (N. Virginia)',
                "Message": '{"AlarmName":"ExampleAlarmName","NewStateValue":"ALARM","NewStateReason":"Threshold Crossed: 1 datapoint [15.0 (21/05/2024 12:00:00)] was greater than or equal to the threshold (10.0)."}',
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

# test
lambda_handler(event=alarm, context=None)
