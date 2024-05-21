import boto3


def create_sqs_queue(queue_name):
    sqs = boto3.client("sqs")
    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "DelaySeconds": "0",
            "MessageRetentionPeriod": "86400",  # 1 day
            "VisibilityTimeout": "60",  # 1 minute
        },
    )
    print(f"Queue '{queue_name}' created with URL: {response['QueueUrl']}")
    return response["QueueUrl"]


def send_message(queue_url, message_body):
    sqs = boto3.client("sqs")
    response = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
    print(f"Message sent to queue with ID: {response['MessageId']}")


def receive_messages(queue_url, max_number_of_messages=1, wait_time_seconds=0):
    sqs = boto3.client("sqs")
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_number_of_messages,
        WaitTimeSeconds=wait_time_seconds,
    )
    messages = response.get("Messages", [])
    if not messages:
        print("No messages received.")
        return

    for message in messages:
        print(f"Received message: {message['Body']}")
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        print(f"Message deleted from queue.")


if __name__ == "__main__":
    queue_name = "MyTestQueue"
    queue_url = create_sqs_queue(queue_name)
    send_message(queue_url, "Hello, SQS!")
    receive_messages(queue_url)
