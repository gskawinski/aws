""" 
The lambda handler processes different types of event data, demonstrating its flexibility and robustness in handling new event types and incomplete events.
"""

import logging

# Set up logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Loading function")

# Define the initial event data
event = {
    "events": [
        {"name": "user:created", "event_id": 1},
        {"name": "user:updated", "event_id": 2},
        {"name": "account:created", "event_id": 3},
        {"name": "campaign:created", "event_id": 4},
        {"name": "video:watched", "event_id": 5},
        {"name": "user:created", "event_id": 6},
        {"name": "request:performed", "event_id": 7},
    ]
}


def add_type(event):
    """
    Adds an 'event_type' key to the event dictionary based on the 'name' field.

    Args:
        event (dict): The event dictionary.

    Returns:
        dict: The updated event dictionary with 'event_type'.
    """
    try:
        event["event_type"] = event["name"].split(":")[0]
        return event
    except KeyError as e:
        logger.error(f"Missing key in event: {e}")
        raise


def group_dict(type_name):
    """
    Creates a dictionary structure for grouping events by type.

    Args:
        type_name (str): The type of events to group.

    Returns:
        dict: The group dictionary with the specified event type.
    """
    return {"event_type": type_name, "events": []}


def add_events_to_group(*, events, event_types):
    """
    Groups events by their type.

    Args:
        events (list): List of event dictionaries.
        event_types (list): List of event types.

    Returns:
        list: List of grouped events dictionaries.
    """
    groups = [group_dict(t) for t in event_types]
    for e in events:
        logger.debug(f"Processing event: {e}")
        for g in groups:
            if e["event_type"] == g["event_type"]:
                g["events"].append(e)
    return groups


def validate_event(event):
    """
    Validates the event structure.

    Args:
        event (dict): The event dictionary to validate.

    Returns:
        bool: True if the event is valid, False otherwise.
    """
    required_keys = {"name", "event_id"}
    return required_keys.issubset(event.keys())


def lambda_handler(event, context):
    """
    AWS Lambda handler function to process events.

    Args:
        event (dict): Input event data.
        context: AWS Lambda context object.

    Returns:
        str: Result message.
    """
    try:
        events = event["events"]
        # Validate events
        events_validate = [e for e in events if validate_event(e)]
        if len(events_validate) != len(events):
            logger.warning("Some events failed validation and were excluded.")
        events_with_type = [add_type(e) for e in events_validate]
        event_types = list(set([e["event_type"] for e in events_with_type]))
        events_groups = add_events_to_group(
            events=events_with_type, event_types=event_types
        )
        for g in events_groups:
            print(g)
        return "events processed"
    except KeyError as e:
        logger.error(f"Missing key in event data: {e}")
        return "Error: Missing key in event data"
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return "Error: An unexpected error occurred"


lambda_handler(event, None)


# New event data with additional types
new_event = {
    "events": [
        {"name": "user:created", "event_id": 1},
        {"name": "order:placed", "event_id": 8},
        {"name": "order:shipped", "event_id": 9},
        {"name": "order:delivered", "event_id": 10},
        {"name": "user:updated", "event_id": 2},
    ]
}

# Invoke lambda_handler with the new event
lambda_handler(new_event, None)

# Event data with some invalid events
invalid_event = {
    "events": [
        {"name": "user:created", "event_id": 1},
        {"name": "order:placed"},  # Missing 'event_id'
        {"name": "order:shipped", "event_id": 9},
        {"event_id": 10},  # Missing 'name'
        {"name": "user:updated", "event_id": 2},
    ]
}

# Invoke lambda_handler with the invalid event
lambda_handler(invalid_event, None)
