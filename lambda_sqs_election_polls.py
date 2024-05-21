import json


def lambda_handler(event, context):
    """
    Lambda function to process votes from the PresidentialElectionPolls SQS queue.

    :param event: The event data received from the SQS queue.
    :param context: The runtime information of the Lambda function.
    """
    try:
        # Initialize a vote count dictionary
        vote_counts = {
            "Candidate_A": 0,
            "Candidate_B": 0,
            "Candidate_C": 0,
            "Candidate_D": 0,
            "Candidate_E": 0,
        }

        # Process each record (vote) in the event
        for record in event["Records"]:
            # Parse the message body
            message_body = json.loads(record["body"])
            voter_id = message_body["voter_id"]
            vote = message_body["vote"]

            # Increment the vote count for the candidate
            if vote in vote_counts:
                vote_counts[vote] += 1
            else:
                print(f"Invalid vote detected from voter {voter_id}: {vote}")

        # Print the vote counts (or update a database, etc.)
        print(f"Vote counts: {json.dumps(vote_counts)}")

        # Return success response
        return {"statusCode": 200, "body": json.dumps("Votes processed successfully.")}

    except Exception as e:
        print(f"Error processing votes: {e}")
        return {"statusCode": 500, "body": json.dumps("Error processing votes.")}


# Example invocation (for testing purposes)
if __name__ == "__main__":

    # Simulated event for testing
    event = {
        "Records": [
            {
                "messageId": "1",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": '{"voter_id": "voter_1", "vote": "Candidate_A"}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1529104986221",
                    "SenderId": "123456789012",
                    "ApproximateFirstReceiveTimestamp": "1529104986230",
                },
                "messageAttributes": {},
                "md5OfBody": "9bb58f26192e4ba00f01e2e7b136bbd8",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:PresidentialElectionPolls",
                "awsRegion": "us-east-1",
            },
            {
                "messageId": "2",
                "receiptHandle": "AQEBzW5WvV4vG8q3e6VG8gUswJ56Rfxa5f...",
                "body": '{"voter_id": "voter_2", "vote": "Candidate_B"}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1529104986222",
                    "SenderId": "123456789012",
                    "ApproximateFirstReceiveTimestamp": "1529104986231",
                },
                "messageAttributes": {},
                "md5OfBody": "1e4ddab45f3b90adf58daff7d739aa6e",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:PresidentialElectionPolls",
                "awsRegion": "us-east-1",
            },
        ]
    }

    lambda_handler(event, None)

""" 

Explanation
Records: A list of messages received from the SQS queue. Each message represents a vote.
messageId: Unique identifier for the message.
receiptHandle: A unique identifier associated with the act of receiving the message. This is used to delete the message from the queue after it has been processed.
body: The content of the message, which in this case is a JSON string representing a vote.
attributes: Metadata about the message such as the approximate number of times the message has been received, the timestamp when the message was sent, and the sender ID.
messageAttributes: Any additional custom attributes sent with the message. In this example, it's empty.
md5OfBody: An MD5 digest of the message body, used to verify the integrity of the message.
eventSource: The source of the event, which is aws:sqs for SQS messages.
eventSourceARN: The Amazon Resource Name (ARN) of the SQS queue from which the message was received.
awsRegion: The AWS region where the SQS queue is located.

"""
