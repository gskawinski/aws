"""
AWS Lambda functions example for Real-time Data Processing

Business Logic: Lambda functions can process data streams from various sources social media, or clickstreams in real-time. They can perform actions such as filtering, aggregation, or transformation on incoming data.
"""

# Load and init dependencis
import json
from faker import Faker
import random
import time

# Initialize Faker for generating simulated data
fake = Faker()


def generate_clickstream_event():
    """
    Simulates a single clickstream event.
    Returns:
        dict: A dictionary representing a single clickstream event.
    """

    actions = ["click", "copy", "paste"]
    pages = ["homepage", "contact", "products", "about"]

    event = {
        "user_id": fake.random_int(min=1, max=100),
        "action": random.choice(actions),
        "timestamp": fake.date_time_this_month().isoformat(),
        "page": random.choice(pages),
    }
    return event


def simulate_clickstream_events(num_events):
    """
    Generates simulated clickstream events.
    Args:
        num_events (int): Number of events to generate.
    Yields:
        dict: A dictionary representing a single clickstream event.
    """

    for _ in range(num_events):
        yield generate_clickstream_event()


# Filtering and aggregation are performed on the clickstream events.
# In this example, we're simply counting the number of clicks on the homepage and product pages.
# In a real application, you would likely perform further actions such as storing the aggregated data in a database or triggering additional processes.


# Local (testing) Lambda function definition
def lambda_handler_local(event, context):
    """
    Lambda function entry point.

    Args:
        event (dict): The event data.
        context (object): The Lambda runtime information.

    Returns:
        dict: A dictionary containing the result of clickstream processing.
    """
    try:
        # Generate simulated clickstream events
        clickstream_events = simulate_clickstream_events(num_events=10)

        # Initialize counters for actions and pages
        action_counts = {}
        page_counts = {}

        # Perform filtering and aggregation
        for event in clickstream_events:
            # Count actions
            action = event["action"]
            action_counts[action] = action_counts.get(action, 0) + 1

            # Count pages
            page = event["page"]
            page_counts[page] = page_counts.get(page, 0) + 1

        # Construct result
        result = {"action_counts": action_counts, "page_counts": page_counts}

        # Print result for demonstration
        print("Real-time clickstream processing result:", result)

        # Return result
        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        # Handle any errors and return an error response
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        return {"statusCode": 500, "body": json.dumps({"error": error_message})}
