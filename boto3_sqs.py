"""
Amazon SQS
Amazon Simple Queue Service (SQS) is a fully managed message queuing service that enables you to decouple and scale microservices, distributed systems, and serverless applications. SQS allows you to send, store, and receive messages between software components at any volume without losing messages or requiring other services to be always available.

Types of Queues in SQS
1. Standard Queues: These offer maximum throughput, best-effort ordering, and at-least-once delivery. Standard queues guarantee that a message is delivered at least once and allow multiple copies of a message to be delivered out of order.

2. FIFO (First-In-First-Out) Queues: These guarantee that messages are processed exactly once, in the exact order that they are sent. FIFO queues are designed to ensure that the order of operations is preserved.

SQS Attributes
When creating or configuring an SQS queue, you can set various attributes to control its behavior. Here are the key attributes for both Standard and FIFO queues:

DelaySeconds: The time (in seconds) that the delivery of all messages in the queue is delayed. Valid values are 0 to 900 (15 minutes).

MaximumMessageSize: The limit of how large a message can be (in bytes). The default is 256 KB, and the maximum is 262144 bytes (256 KB).

MessageRetentionPeriod: The length of time (in seconds) that Amazon SQS retains a message. The default is 4 days (345600 seconds), and the maximum is 14 days (1209600 seconds).

ReceiveMessageWaitTimeSeconds: The amount of time (in seconds) that a ReceiveMessage call will wait for a message to arrive if the queue is empty. The default is 0, and the maximum is 20 seconds.

VisibilityTimeout: The length of time (in seconds) that a message received from a queue will be invisible to other receiving components when they are in the process of being processed. The default is 30 seconds, and the maximum is 12 hours (43200 seconds).

RedrivePolicy: A JSON object that includes the parameters for the dead-letter queue functionality, which includes the deadLetterTargetArn (the ARN of the dead-letter queue) and maxReceiveCount (the number of times a message can be received before it is moved to the dead-letter queue).

QueueArn: The Amazon Resource Name (ARN) for the queue.

ApproximateNumberOfMessages: The approximate number of messages available for retrieval from the queue.

ApproximateNumberOfMessagesNotVisible: The approximate number of messages that are in flight (i.e., messages that have been received from the queue by a consumer but not yet deleted or timed out).

CreatedTimestamp: The time when the queue was created (in Unix epoch time).

LastModifiedTimestamp: The time when the queue was last changed (in Unix epoch time).

Policy: The JSON-formatted string that contains the access policy for the queue.

KmsMasterKeyId: The ID of an AWS-managed customer master key (CMK) for Amazon SQS or a custom CMK.

KmsDataKeyReusePeriodSeconds: The length of time, in seconds, for which Amazon SQS can reuse a data key to encrypt or decrypt messages before calling AWS KMS again. The default is 300 seconds (5 minutes).

SqsManagedSseEnabled: Indicates whether server-side encryption (SSE) is enabled using SQS-managed encryption keys.

ContentBasedDeduplication: (FIFO only) Enables content-based deduplication, ensuring that messages with identical content are treated as duplicates and only one copy is delivered.

FifoQueue: (FIFO only) Indicates whether the queue is FIFO.

DeduplicationScope: (FIFO only) Specifies whether message deduplication occurs at the message group or queue level.

FifoThroughputLimit: (FIFO only) Specifies whether throughput is limited to one message per second per message group or at the queue level.

Dead-Letter Queue (DLQ)
A Dead-Letter Queue is a queue that other (source) queues can target for messages that can't be processed (consumed) successfully. Amazon SQS supports DLQ redrive policies, which specify when messages are sent to the DLQ.

Key Concepts of DLQ
DeadLetterTargetArn: The Amazon Resource Name (ARN) of the DLQ to which Amazon SQS moves messages after they have been received unsuccessfully the number of times specified by maxReceiveCount.

MaxReceiveCount: The maximum number of times a message can be received by consumers before it is moved to the DLQ. For example, if the maxReceiveCount is set to 5, a message will be moved to the DLQ after it has been received 5 times without being deleted.
"""

# SQS Class Manager
import boto3
from botocore.exceptions import ClientError
import json


class SQSManager:
    def __init__(self, region_name="us-east-1"):
        """
        Initialize the SQSManager with the specified AWS region.
        """
        self.sqs = boto3.client("sqs", region_name=region_name)

    def create_queue(self, queue_name, attributes=None):
        """
        Create an SQS queue with the given name and attributes.

        :param queue_name: The name of the queue.
        :param attributes: A dictionary of attributes for the queue.
        :return: The URL of the created queue.
        """
        try:
            response = self.sqs.create_queue(
                QueueName=queue_name, Attributes=attributes or {}
            )
            return response["QueueUrl"]
        except ClientError as e:
            print(f"Error creating queue: {e}")
            return None

    def get_queue_url(self, queue_name):
        """
        Retrieve the URL of an existing SQS queue by name.

        :param queue_name: The name of the queue.
        :return: The URL of the queue.
        """
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]
        except ClientError as e:
            print(f"Error retrieving queue URL: {e}")
            return None

    def send_message(
        self, queue_url, message_body, delay_seconds=0, message_attributes=None
    ):
        """
        Send a message to the specified SQS queue.

        :param queue_url: The URL of the queue.
        :param message_body: The body of the message.
        :param delay_seconds: The delay in seconds for the message.
        :param message_attributes: Additional attributes for the message.
        :return: The response from the send message request.
        """
        try:
            response = self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                DelaySeconds=delay_seconds,
                MessageAttributes=message_attributes or {},
            )
            return response
        except ClientError as e:
            print(f"Error sending message: {e}")
            return None

    def receive_messages(
        self, queue_url, max_number_of_messages=1, wait_time_seconds=0
    ):
        """
        Receive messages from the specified SQS queue.

        :param queue_url: The URL of the queue.
        :param max_number_of_messages: The maximum number of messages to retrieve.
        :param wait_time_seconds: The duration (in seconds) for which the call waits for a message to arrive.
        :return: A list of messages.
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_number_of_messages,
                WaitTimeSeconds=wait_time_seconds,
            )
            return response.get("Messages", [])
        except ClientError as e:
            print(f"Error receiving messages: {e}")
            return []

    def delete_message(self, queue_url, receipt_handle):
        """
        Delete a message from the specified SQS queue.

        :param queue_url: The URL of the queue.
        :param receipt_handle: The receipt handle associated with the message to delete.
        :return: The response from the delete message request.
        """
        try:
            response = self.sqs.delete_message(
                QueueUrl=queue_url, ReceiptHandle=receipt_handle
            )
            return response
        except ClientError as e:
            print(f"Error deleting message: {e}")
            return None

    def set_queue_attributes(self, queue_url, attributes):
        """
        Set attributes for the specified SQS queue.

        :param queue_url: The URL of the queue.
        :param attributes: A dictionary of attributes to set.
        :return: The response from the set attributes request.
        """
        try:
            response = self.sqs.set_queue_attributes(
                QueueUrl=queue_url, Attributes=attributes
            )
            return response
        except ClientError as e:
            print(f"Error setting queue attributes: {e}")
            return None

    def create_dead_letter_queue(self, dlq_name, source_queue_url, max_receive_count):
        """
        Create a dead-letter queue and associate it with a source queue.

        :param dlq_name: The name of the dead-letter queue.
        :param source_queue_url: The URL of the source queue.
        :param max_receive_count: The maximum number of receives before a message is moved to the dead-letter queue.
        :return: The URL of the created dead-letter queue.
        """
        dlq_url = self.create_queue(dlq_name)
        if not dlq_url:
            return None

        dlq_arn = self.get_queue_attributes(dlq_url).get("QueueArn")
        if not dlq_arn:
            return None

        redrive_policy = {
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": str(max_receive_count),
        }
        self.set_queue_attributes(
            source_queue_url, {"RedrivePolicy": json.dumps(redrive_policy)}
        )
        return dlq_url

    def get_queue_attributes(self, queue_url):
        """
        Retrieve attributes of the specified SQS queue.

        :param queue_url: The URL of the queue.
        :return: A dictionary of queue attributes.
        """
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["All"]
            )
            return response["Attributes"]
        except ClientError as e:
            print(f"Error getting queue attributes: {e}")
            return {}

    def purge_queue(self, queue_url):
        """
        Purge all messages in the specified SQS queue.

        :param queue_url: The URL of the queue.
        :return: The response from the purge request.
        """
        try:
            response = self.sqs.purge_queue(QueueUrl=queue_url)
            return response
        except ClientError as e:
            print(f"Error purging queue: {e}")
            return None


# Example of how to use the SQSManager class to design a queue system for tracking votes in a street poll for a presidential election with 5 candidates. We'll create a main queue for receiving votes, a dead-letter queue to handle failed messages, and demonstrate how to send, receive, and process messages.


def main():
    """
    Main function to demonstrate the usage of SQSManager for managing a queue
    for street polls in a presidential election with 5 candidates.
    """
    # Initialize the SQSManager
    sqs_manager = SQSManager(region_name="us-east-1")

    # Define the queue name and attributes
    queue_name = "PresidentialElectionPolls"
    attributes = {
        "DelaySeconds": "0",
        "MaximumMessageSize": "262144",  # 256 KB
        "MessageRetentionPeriod": "345600",  # 4 days
        "ReceiveMessageWaitTimeSeconds": "0",
        "VisibilityTimeout": "30",
    }

    # Create the main queue for votes
    print("Creating the main queue for votes...")
    queue_url = sqs_manager.create_queue(queue_name, attributes)
    print(f"Queue URL: {queue_url}")

    # Create a dead-letter queue for failed messages
    # a dead-letter queue to handle messages that cannot be processed successfully after a defined number of attempts.
    dlq_name = "PresidentialElectionPollsDLQ"
    max_receive_count = 5  # Messages will be moved to DLQ after 5 failed attempts

    print("Creating the dead-letter queue...")
    dlq_url = sqs_manager.create_dead_letter_queue(
        dlq_name, queue_url, max_receive_count
    )
    print(f"Dead-letter Queue URL: {dlq_url}")

    # Define candidates
    candidates = [
        "Candidate_A",
        "Candidate_B",
        "Candidate_C",
        "Candidate_D",
        "Candidate_E",
    ]

    # Simulate sending votes to the queue
    # Sending Votes: A loop is used to simulate sending 100 votes to the queue. Each vote is a JSON object containing a voter ID and their chosen candidate.
    print("Sending votes to the queue...")
    for i in range(100):  # Simulate 100 votes
        vote = {
            "voter_id": f"voter_{i}",
            "vote": candidates[i % 5],  # Round-robin voting for simplicity
        }
        message_body = json.dumps(vote)
        send_response = sqs_manager.send_message(queue_url, message_body)
        print(f"Sent vote: {vote}")

    # Receive and process votes
    print("Receiving and processing votes...")
    while True:
        messages = sqs_manager.receive_messages(
            queue_url, max_number_of_messages=10, wait_time_seconds=10
        )
        if not messages:
            print("No more messages to process.")
            break
        for message in messages:
            try:
                vote = json.loads(message["Body"])
                print(f"Processing vote: {vote}")
                # Here you can add logic to count the votes, update a database, etc.

                # Delete the message after processing
                sqs_manager.delete_message(queue_url, message["ReceiptHandle"])
                print(f"Deleted message with ReceiptHandle: {message['ReceiptHandle']}")
            except Exception as e:
                print(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
