# EXAMPLES

# # Automated Start and Stop of EC2 Instances:
# AWS Lambda functions can be scheduled to start and stop EC2 instances at specific times, which is particularly useful for cost-saving. For instance, development and test environments can be powered off outside of business hours to reduce unnecessary costs. This can be achieved using Amazon CloudWatch Events to trigger the Lambda function at the desired times.

# Automated Start and Stop of EC2 Instances
# Lambda Function: Start/Stop EC2 Instances

import boto3
import json

ec2 = boto3.client("ec2")


def lambda_handler(event, context):
    """
    Start or stop EC2 instances based on the schedule.
    Event structure:
    {
        "action": "start" or "stop",
        "instance_ids": ["i-0123456789abcdef0", "i-0123456789abcdef1"]
    }
    """
    action = event.get("action")
    instance_ids = event.get("instance_ids")

    if not instance_ids or action not in ["start", "stop"]:
        return {"status": "error", "message": "Invalid input data"}

    try:
        if action == "start":
            ec2.start_instances(InstanceIds=instance_ids)
            message = f"Successfully started instances: {instance_ids}"
        else:
            ec2.stop_instances(InstanceIds=instance_ids)
            message = f"Successfully stopped instances: {instance_ids}"

        return {"status": "success", "message": message}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example event data for testing
test_event_start = {"action": "start", "instance_ids": ["i-0123456789abcdef0"]}
test_event_stop = {"action": "stop", "instance_ids": ["i-0123456789abcdef0"]}


def test_lambda_handler():
    print(lambda_handler(test_event_start, None))
    print(lambda_handler(test_event_stop, None))


# Run the test function
test_lambda_handler()

# =======================================
# # EC2 Instance Health Monitoring and Auto-Healing:
# AWS Lambda can be used to monitor the health of EC2 instances and perform auto-healing tasks. By integrating with CloudWatch Alarms, Lambda functions can automatically restart or replace instances that are not responding or have failed health checks. This ensures high availability and reliability of applications running on EC2 instances.

# EC2 Instance Health Monitoring and Auto-Healing
# Lambda Function: EC2 Auto-Healing

import boto3
import json

ec2 = boto3.client("ec2")
cloudwatch = boto3.client("cloudwatch")


def lambda_handler(event, context):
    """
    Monitor and auto-heal EC2 instances based on CloudWatch alarms.
    Event structure:
    {
        "instance_id": "i-0123456789abcdef0",
        "state": "ALARM"
    }
    """
    instance_id = event.get("instance_id")
    state = event.get("state")

    if state != "ALARM":
        return {"status": "error", "message": "No action needed, state is not ALARM"}

    try:
        ec2.reboot_instances(InstanceIds=[instance_id])
        return {
            "status": "success",
            "message": f"Successfully rebooted instance: {instance_id}",
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example event data for testing
test_event_health = {"instance_id": "i-0123456789abcdef0", "state": "ALARM"}


def test_lambda_handler():
    print(lambda_handler(test_event_health, None))


# Run the test function
test_lambda_handler()

# ================================
# # Automated Scaling Based on Custom Metrics:
# While AWS provides Auto Scaling, there are scenarios where custom scaling logic is required. AWS Lambda can be used to implement custom scaling policies based on specific metrics or business logic. For example, if a particular application metric exceeds a threshold, a Lambda function can be triggered to provision additional EC2 instances or scale down when the load decreases.

# Automated Scaling Based on Custom Metrics
# Lambda Function: Custom Scaling

import boto3
import json

ec2 = boto3.client("ec2")


def lambda_handler(event, context):
    """
    Scale EC2 instances based on custom metrics.
    Event structure:
    {
        "action": "scale_out" or "scale_in",
        "instance_id": "i-0123456789abcdef0"
    }
    """
    action = event.get("action")
    instance_id = event.get("instance_id")

    if action not in ["scale_out", "scale_in"] or not instance_id:
        return {"status": "error", "message": "Invalid input data"}

    try:
        if action == "scale_out":
            # Example: Starting an additional instance (simplified logic)
            ec2.start_instances(InstanceIds=[instance_id])
            message = f"Successfully scaled out: started instance {instance_id}"
        else:
            # Example: Stopping an instance (simplified logic)
            ec2.stop_instances(InstanceIds=[instance_id])
            message = f"Successfully scaled in: stopped instance {instance_id}"

        return {"status": "success", "message": message}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example event data for testing
test_event_scale_out = {"action": "scale_out", "instance_id": "i-0123456789abcdef0"}
test_event_scale_in = {"action": "scale_in", "instance_id": "i-0123456789abcdef0"}


def test_lambda_handler():
    print(lambda_handler(test_event_scale_out, None))
    print(lambda_handler(test_event_scale_in, None))


# Run the test function
test_lambda_handler()


# # Snapshot and Backup Management:
# AWS Lambda can automate the creation and deletion of EC2 snapshots, ensuring that backups are regularly taken and old snapshots are deleted to save on storage costs. Lambda functions can be scheduled using CloudWatch Events to periodically create snapshots of EBS volumes attached to EC2 instances and clean up outdated backups according to a retention policy.

# Snapshot and Backup Management
# Lambda Function: Snapshot Management

import boto3
import json
from datetime import datetime, timedelta

ec2 = boto3.client("ec2")


def lambda_handler(event, context):
    """
    Create and delete EBS snapshots for backup management.
    Event structure:
    {
        "action": "create" or "delete",
        "volume_id": "vol-0123456789abcdef0"
    }
    """
    action = event.get("action")
    volume_id = event.get("volume_id")

    if action not in ["create", "delete"] or not volume_id:
        return {"status": "error", "message": "Invalid input data"}

    try:
        if action == "create":
            snapshot = ec2.create_snapshot(
                VolumeId=volume_id, Description="Automated backup"
            )
            message = f'Successfully created snapshot: {snapshot["SnapshotId"]}'
        else:
            # Example: Deleting snapshots older than 7 days (simplified logic)
            cutoff = datetime.utcnow() - timedelta(days=7)
            snapshots = ec2.describe_snapshots(
                Filters=[{"Name": "volume-id", "Values": [volume_id]}]
            )
            for snapshot in snapshots["Snapshots"]:
                if snapshot["StartTime"].replace(tzinfo=None) < cutoff:
                    ec2.delete_snapshot(SnapshotId=snapshot["SnapshotId"])
            message = "Successfully deleted old snapshots"

        return {"status": "success", "message": message}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Example event data for testing
test_event_create_snapshot = {"action": "create", "volume_id": "vol-0123456789abcdef0"}
test_event_delete_snapshot = {"action": "delete", "volume_id": "vol-0123456789abcdef0"}


def test_lambda_handler():
    print(lambda_handler(test_event_create_snapshot, None))
    print(lambda_handler(test_event_delete_snapshot, None))


# Run the test function
test_lambda_handler()


""" 

Use Amazon EventBridge (formerly known as Amazon CloudWatch Events) to create cron jobs.

EventBridge is a serverless event bus service that allows you to build event-driven architectures.
It acts as a central hub for events, enabling communication between different services and components within your AWS environment.
EventBridge supports custom events, partner events, and built-in events from various AWS services.
You can create rules to route events to targets (e.g., Lambda functions, SNS topics, Step Functions) based on event patterns or cron expressions.

Provides more flexible event patterns using JSONPath expressions.
Allows complex matching based on event content.
Supports multiple targets per rule.
Can fan out events to multiple Lambda functions, SNS topics, etc.

---------- 
CloudWatch Events is a service that monitors events from AWS services and custom applications.
It focuses primarily on events generated by AWS services (e.g., EC2 instance state changes, S3 object creation, etc.).
Monitors specific events within those services.
Typically routes events to a single target.
-----------


 Example EventBridge event rule in JSON format that can trigger your Lambda function at a specific interval (e.g., daily, weekly, etc.).

 {
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "resources": ["arn:aws:events:REGION:ACCOUNT-ID:rule/RULE-NAME"],
  "detail": {
    "eventName": "AWS CloudWatch Events scheduled event",
    "requestParameters": {
      "source": ["aws.events"],
      "resources": ["arn:aws:events:REGION:ACCOUNT-ID:rule/RULE-NAME"],
      "time": ["YYYY-MM-DDTHH:MM:SSZ"],
      "region": ["REGION"]
    }
  }
}

CRON scheduler

A cron job is a scheduled task that runs automatically at specified intervals on Unix-like operating systems.

Cron Expressions
A cron expression consists of five fields, representing minute, hour, day of the month, month, and day of the week. Each field can contain specific values or wildcards:

Minute (0-59): The minute when the task should run.
Hour (0-23): The hour when the task should run.
Day of the Month (1-31): The day of the month when the task should run.
Month (1-12): The month when the task should run.
Day of the Week (0-6): The day of the week (Sunday to Saturday) when the task should run (both 0 and 7 represent Sunday).
Examples of Cron Expressions
Here are some examples of cron expressions:

( 0 10 * * ? ): Runs every day at 10:00 AM UTC.
( 0 0 1 * ? ): Runs on the first day of every month at midnight UTC.
( 0 20 ? * MON-FRI) : Runs every weekday (Monday to Friday) at 8:00 PM UTC.






"""
