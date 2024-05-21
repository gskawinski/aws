"""

Amazon Simple Notification Service (SNS) is a fully managed messaging service that enables you to decouple and scale microservices, distributed systems, and serverless applications. SNS can be used to send messages to a variety of endpoints including email, HTTP/HTTPS, AWS Lambda functions, and other AWS services. 

Amazon SNS is designed for high-throughput, push-based, many-to-many messaging. It facilitates the sending of messages from applications to subscribers or other applications. SNS supports multiple messaging protocols, including HTTP/HTTPS, email, SMS, and AWS Lambda.

Key Features of AWS SNS
- Message Delivery: Messages can be delivered to various endpoints.
- Scalability: Automatically scales to handle large volumes of messages.
- Durability: Ensures messages are stored across multiple availability zones.
- Security: Integrates with AWS IAM for access control and supports encryption.

SNS Concepts
Topics
A topic is a logical access point that acts as a communication channel. Topics are used to group multiple recipients (subscribers) so that a message can be delivered to multiple destinations.

Subscriptions
A subscription is the endpoint that receives messages from a topic. Each subscription is associated with a topic and can be an HTTP/HTTPS endpoint, an email address, an SMS number, or an AWS Lambda function.

Messages
Messages are the content that is sent to the subscribers of a topic. Messages can include notifications, alerts, or other types of data.

AWS SNS provides various attributes that can be configured for topics, subscriptions, and messages.

Topic Attributes
- DeliveryPolicy: Defines how Amazon SNS retries the delivery of messages to HTTP/S endpoints.
- DisplayName: A human-readable name used in email notifications.
- Policy: The IAM policy that controls access to the topic.
- KmsMasterKeyId: The ID of an AWS KMS key for encrypting messages.
- FifoTopic: Boolean value to indicate if the topic is FIFO (First-In-First-Out).
- ContentBasedDeduplication: Boolean value to enable content-based deduplication for FIFO topics.

Subscription Attributes
- DeliveryPolicy: Defines how Amazon SNS retries the delivery of messages to the subscription endpoint.
- FilterPolicy: Allows you to specify filtering rules for messages sent to this subscription.
- RawMessageDelivery: Enables raw message delivery to SQS endpoints.
- RedrivePolicy: Specifies the dead-letter queue to use for the subscription.
- SubscriptionRoleArn: The IAM role that allows SNS to access the subscription endpoint.

Message Attributes
- MessageDeduplicationId: A unique identifier for deduplicating messages in FIFO topics.
- MessageGroupId: The tag that specifies that a message belongs to a specific message group in FIFO topics.
- MessageAttributes: Key-value pairs that are used to provide structured metadata about the message.

Common Use Cases
- Alerting Systems: Send notifications about system events or anomalies.
- Mobile Push Notifications: Deliver messages to mobile devices.
- Fanout Patterns: Distribute messages to multiple systems or microservices.
- Workflow Coordination: Orchestrate tasks across different services using messages

DOCS:
https://docs.aws.amazon.com/sns/latest/dg/welcome.html

"""

import boto3
from botocore.exceptions import ClientError


class SNSManager:
    """
    A class to manage AWS SNS topics, subscriptions, and messages.
    """

    def __init__(self, region_name="us-east-1"):
        """
        Initialize the SNS client.

        :param region_name: AWS region name. Default is 'us-east-1'.
        """
        self.sns_client = boto3.client("sns", region_name=region_name)

    def create_topic(self, name, attributes=None):
        """
        Create an SNS topic.

        :param name: Name of the topic.
        :param attributes: Dictionary of optional attributes for the topic.
        :return: ARN of the created topic.
        """
        try:
            response = self.sns_client.create_topic(
                Name=name, Attributes=attributes or {}
            )
            return response["TopicArn"]
        except ClientError as e:
            print(f"Error creating topic: {e}")
            return None

    def subscribe(self, topic_arn, protocol, endpoint, attributes=None):
        """
        Subscribe an endpoint to an SNS topic.

        :param topic_arn: ARN of the topic.
        :param protocol: Protocol to use (e.g., 'email', 'sms', 'http', etc.).
        :param endpoint: Endpoint to receive messages (e.g., email address, phone number, URL).
        :param attributes: Dictionary of optional attributes for the subscription.
        :return: ARN of the created subscription.
        """
        try:
            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol=protocol,
                Endpoint=endpoint,
                Attributes=attributes or {},
            )
            return response["SubscriptionArn"]
        except ClientError as e:
            print(f"Error subscribing to topic: {e}")
            return None

    def publish_message(
        self, topic_arn, message, subject=None, message_attributes=None
    ):
        """
        Publish a message to an SNS topic.

        :param topic_arn: ARN of the topic.
        :param message: Message to publish.
        :param subject: Subject of the message (optional).
        :param message_attributes: Dictionary of message attributes (optional).
        :return: MessageId of the published message.
        """
        try:
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Message=message,
                Subject=subject,
                MessageAttributes=message_attributes or {},
            )
            return response["MessageId"]
        except ClientError as e:
            print(f"Error publishing message: {e}")
            return None

    def set_topic_attributes(self, topic_arn, attribute_name, attribute_value):
        """
        Set attributes for an SNS topic.

        :param topic_arn: ARN of the topic.
        :param attribute_name: Name of the attribute to set.
        :param attribute_value: Value of the attribute.
        :return: None
        """
        try:
            self.sns_client.set_topic_attributes(
                TopicArn=topic_arn,
                AttributeName=attribute_name,
                AttributeValue=attribute_value,
            )
        except ClientError as e:
            print(f"Error setting topic attributes: {e}")

    def get_topic_attributes(self, topic_arn):
        """
        Get attributes of an SNS topic.

        :param topic_arn: ARN of the topic.
        :return: Dictionary of topic attributes.
        """
        try:
            response = self.sns_client.get_topic_attributes(TopicArn=topic_arn)
            return response["Attributes"]
        except ClientError as e:
            print(f"Error getting topic attributes: {e}")
            return None

    def set_subscription_attributes(
        self, subscription_arn, attribute_name, attribute_value
    ):
        """
        Set attributes for an SNS subscription.

        :param subscription_arn: ARN of the subscription.
        :param attribute_name: Name of the attribute to set.
        :param attribute_value: Value of the attribute.
        :return: None
        """
        try:
            self.sns_client.set_subscription_attributes(
                SubscriptionArn=subscription_arn,
                AttributeName=attribute_name,
                AttributeValue=attribute_value,
            )
        except ClientError as e:
            print(f"Error setting subscription attributes: {e}")

    def get_subscription_attributes(self, subscription_arn):
        """
        Get attributes of an SNS subscription.

        :param subscription_arn: ARN of the subscription.
        :return: Dictionary of subscription attributes.
        """
        try:
            response = self.sns_client.get_subscription_attributes(
                SubscriptionArn=subscription_arn
            )
            return response["Attributes"]
        except ClientError as e:
            print(f"Error getting subscription attributes: {e}")
            return None

    def delete_topic(self, topic_arn):
        """
        Delete an SNS topic.

        :param topic_arn: ARN of the topic to delete.
        :return: None
        """
        try:
            self.sns_client.delete_topic(TopicArn=topic_arn)
        except ClientError as e:
            print(f"Error deleting topic: {e}")

    def unsubscribe(self, subscription_arn):
        """
        Unsubscribe from an SNS topic.

        :param subscription_arn: ARN of the subscription to delete.
        :return: None
        """
        try:
            self.sns_client.unsubscribe(SubscriptionArn=subscription_arn)
        except ClientError as e:
            print(f"Error unsubscribing: {e}")


# Example usage
if __name__ == "__main__":
    sns_manager = SNSManager()

    # Create a topic
    topic_arn = sns_manager.create_topic("MyTopic")
    if topic_arn:
        print(f"Created topic ARN: {topic_arn}")

    # Subscribe to the topic
    subscription_arn = sns_manager.subscribe(topic_arn, "email", "example@example.com")
    if subscription_arn:
        print(f"Created subscription ARN: {subscription_arn}")

    # Publish a message to the topic
    message_id = sns_manager.publish_message(
        topic_arn, "Hello, this is a test message!", "Test Subject"
    )
    if message_id:
        print(f"Published message ID: {message_id}")

    # Set a topic attribute
    sns_manager.set_topic_attributes(topic_arn, "DisplayName", "My Display Name")

    # Get topic attributes
    attributes = sns_manager.get_topic_attributes(topic_arn)
    if attributes:
        print(f"Topic attributes: {attributes}")

    # Delete the subscription
    if subscription_arn:
        sns_manager.unsubscribe(subscription_arn)

    # Delete the topic
    sns_manager.delete_topic(topic_arn)

    attr = {
        "Policy": "",  # IAM policy controlling access to the topic
        "DisplayName": "",  # Human-readable name for email notifications
        "DeliveryPolicy": "",  # Delivery retry policy for HTTP/S endpoints
        "KmsMasterKeyId": "",  # ID of AWS KMS key for message encryption
        "FifoTopic": False,  # Indicates if the topic is FIFO (First-In-First-Out)
        "ContentBasedDeduplication": False,  # Enables content-based deduplication for FIFO topics
    }


# Examples
# 1. Alerting Systems: Send notifications about system events or anomalies
# Alerting Systems: Send email notifications to administrators when system events occur.
def alerting_system_example():
    sns_manager = SNSManager()

    # Create a topic for system alerts
    topic_arn = sns_manager.create_topic("SystemAlerts")
    print(f"Created topic ARN: {topic_arn}")

    # Subscribe an email endpoint to the topic
    email = "admin@example.com"
    subscription_arn = sns_manager.subscribe(topic_arn, "email", email)
    print(f"Subscribed {email} to topic ARN: {subscription_arn}")

    # Publish an alert message to the topic
    message = "Alert: CPU usage has exceeded 80% on server XYZ."
    subject = "High CPU Usage Alert"
    message_id = sns_manager.publish_message(topic_arn, message, subject)
    print(f"Published alert message ID: {message_id}")


alerting_system_example()


# 2. Mobile Push Notifications: Deliver messages to mobile devices
# Mobile Push Notifications: Deliver messages to mobile devices via an HTTP endpoint.
def mobile_push_notifications_example():
    sns_manager = SNSManager()

    # Create a topic for mobile push notifications
    topic_arn = sns_manager.create_topic("MobilePushNotifications")
    print(f"Created topic ARN: {topic_arn}")

    # Subscribe an HTTP endpoint to the topic (e.g., an endpoint handling push notifications)
    http_endpoint = "https://example.com/push"
    subscription_arn = sns_manager.subscribe(topic_arn, "http", http_endpoint)
    print(f"Subscribed {http_endpoint} to topic ARN: {subscription_arn}")

    # Publish a push notification message to the topic
    message = "New update available! Check out the latest features in our app."
    subject = "App Update Notification"
    message_id = sns_manager.publish_message(topic_arn, message, subject)
    print(f"Published push notification message ID: {message_id}")


mobile_push_notifications_example()


# 3. Fanout Patterns: Distribute messages to multiple systems or microservices
# Fanout Patterns: Distribute a message to multiple services/microservices by subscribing multiple HTTP endpoints.
def fanout_pattern_example():
    sns_manager = SNSManager()

    # Create a topic for fanout pattern
    topic_arn = sns_manager.create_topic("FanoutTopic")
    print(f"Created topic ARN: {topic_arn}")

    # Subscribe multiple endpoints to the topic
    endpoints = [
        ("http", "https://service1.example.com/notify"),
        ("http", "https://service2.example.com/notify"),
        ("http", "https://service3.example.com/notify"),
    ]

    for protocol, endpoint in endpoints:
        subscription_arn = sns_manager.subscribe(topic_arn, protocol, endpoint)
        print(f"Subscribed {endpoint} to topic ARN: {subscription_arn}")

    # Publish a message to the topic
    message = "New data available for processing."
    subject = "Data Update"
    message_id = sns_manager.publish_message(topic_arn, message, subject)
    print(f"Published fanout message ID: {message_id}")


fanout_pattern_example()


# 4. Workflow Coordination: Orchestrate tasks across different services using messages
# Workflow Coordination: Orchestrate a sequence of tasks by invoking AWS Lambda functions subscribed to a topic.


def workflow_coordination_example():
    sns_manager = SNSManager()

    # Create a topic for workflow coordination
    topic_arn = sns_manager.create_topic("WorkflowCoordination")
    print(f"Created topic ARN: {topic_arn}")

    # Subscribe AWS Lambda functions to the topic
    lambda_endpoints = [
        "arn:aws:lambda:us-east-1:123456789012:function:StartProcessing",
        "arn:aws:lambda:us-east-1:123456789012:function:ValidateData",
        "arn:aws:lambda:us-east-1:123456789012:function:StoreResults",
    ]

    for endpoint in lambda_endpoints:
        subscription_arn = sns_manager.subscribe(topic_arn, "lambda", endpoint)
        print(f"Subscribed Lambda function {endpoint} to topic ARN: {subscription_arn}")

    # Publish a message to the topic to start the workflow
    message = "Initiate data processing workflow."
    subject = "Workflow Initiation"
    message_id = sns_manager.publish_message(topic_arn, message, subject)
    print(f"Published workflow initiation message ID: {message_id}")


workflow_coordination_example()
