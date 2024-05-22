""" 
Amazon EC2:
Amazon Elastic Compute Cloud (EC2) is a web service that provides resizable compute capacity in the cloud. It allows you to quickly scale compute resources up or down based on your application needs. EC2 provides a wide selection of instance types optimized for different use cases, such as compute, memory, storage, and GPU-intensive workloads.

Attributes of Amazon EC2:

1. Instance Types: Amazon EC2 offers a variety of instance types, each optimized for different workloads. These include:
- General Purpose: Balanced CPU, memory, and network resources.
- Compute Optimized: High-performance processors, ideal for compute-bound applications.
- Memory Optimized: Instances with high memory-to-CPU ratios, suitable for memory-intensive workloads.
- Storage Optimized: Instances optimized for high disk throughput or I/O performance.
- GPU Instances: Instances equipped with graphics processing units for parallel processing tasks like machine learning, rendering, and scientific simulations.

2. AMI (Amazon Machine Image): An AMI is a template that contains the software configuration (operating system, application server, and applications) required to launch an EC2 instance. You can choose from a wide range of pre-built AMIs provided by AWS or create custom AMIs tailored to your specific needs.

3. Elastic IP Addresses: Elastic IP addresses are static IPv4 addresses designed for dynamic cloud computing. They can be associated with and disassociated from instances, providing a persistent IP address for your instances even if they are stopped and restarted.

4. Security Groups: Security groups act as virtual firewalls for your EC2 instances, controlling inbound and outbound traffic. You can define rules that allow specific types of traffic to reach your instances based on protocols, ports, and IP ranges.

5. Key Pairs: When you launch an EC2 instance, you can specify a key pair, consisting of a public key and a private key. The private key is used to securely connect to the instance via SSH (for Linux instances) or RDP (for Windows instances).

6. Placement Groups: Placement groups are logical groupings of instances within a single Availability Zone. They enable applications to have more control over the placement of instances to meet specific requirements, such as low-latency networking or high-throughput.

7. EBS (Elastic Block Store): Amazon EBS provides persistent block-level storage volumes for use with EC2 instances. EBS volumes can be attached to EC2 instances as primary storage or additional data volumes, offering durability, scalability, and performance.

8. Auto Scaling: Auto Scaling allows you to automatically adjust the number of EC2 instances in response to changing demand. You can define scaling policies based on metrics such as CPU utilization, network traffic, or custom CloudWatch metrics.

9. Load Balancing: Elastic Load Balancing distributes incoming application traffic across multiple EC2 instances to ensure high availability and fault tolerance. It automatically scales to handle varying levels of traffic and performs health checks on instances to route traffic only to healthy instances.

10. Tags: Tags are key-value pairs that you can assign to EC2 instances to organize and manage your resources. They can be used for cost allocation, resource grouping, and automation purposes.

Manage EC2 Instances:

- Start, stop, terminate, or reboot instances as needed from the EC2 dashboard.
- Monitor instance performance and status using CloudWatch metrics and logs.
- Resize instances by changing the instance type or modifying the attached EBS volumes.
- Create snapshots of EBS volumes for backup and disaster recovery purposes.
- Use Auto Scaling to automatically adjust the number of instances based on demand.


"""

import boto3

# Python module that provides functions to interact with Amazon EC2, including creating, starting, stopping, terminating, suspending, describing, and listing EC2 instances for a given AWS account.

# =========================
# set of functions to manage EC2 services

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


# Initialize a session using Amazon EC2
def init_ec2_client(region_name="us-east-1"):
    return boto3.client("ec2", region_name=region_name)


# Get the account ID if not provided
def get_account_id():
    try:
        sts_client = boto3.client("sts")
        identity = sts_client.get_caller_identity()
        return identity["Account"]
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available or incomplete: ", e)
        return None


# List all EC2 instances for a given account
def list_ec2_instances(account_id=None, region_name="us-east-1"):
    if account_id is None:
        account_id = get_account_id()
    if account_id is None:
        return "Unable to determine account ID."

    ec2_client = init_ec2_client(region_name)
    instances = []
    try:
        response = ec2_client.describe_instances()
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                instances.append(instance)
        return instances
    except Exception as e:
        print(f"Error listing EC2 instances: {e}")
        return None


# Start an EC2 instance
def start_ec2_instance(instance_id, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.start_instances(InstanceIds=[instance_id])
        return response
    except Exception as e:
        print(f"Error starting EC2 instance {instance_id}: {e}")
        return None


# Stop an EC2 instance
def stop_ec2_instance(instance_id, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.stop_instances(InstanceIds=[instance_id])
        return response
    except Exception as e:
        print(f"Error stopping EC2 instance {instance_id}: {e}")
        return None


# Terminate an EC2 instance
def terminate_ec2_instance(instance_id, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.terminate_instances(InstanceIds=[instance_id])
        return response
    except Exception as e:
        print(f"Error terminating EC2 instance {instance_id}: {e}")
        return None


# Describe EC2 instance status
def describe_instance_status(instance_id, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
        return response
    except Exception as e:
        print(f"Error describing status of EC2 instance {instance_id}: {e}")
        return None


# Create a new EC2 instance
def create_ec2_instance(
    image_id, instance_type, key_name, security_group_ids, region_name="us-east-1"
):
    ec2_client = init_ec2_client(region_name)
    try:
        instances = ec2_client.run_instances(
            ImageId=image_id,
            InstanceType=instance_type,
            KeyName=key_name,
            SecurityGroupIds=security_group_ids,
            MinCount=1,
            MaxCount=1,
        )
        return instances
    except Exception as e:
        print(f"Error creating EC2 instance: {e}")
        return None


# Reboot an EC2 instance
def reboot_ec2_instance(instance_id, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.reboot_instances(InstanceIds=[instance_id])
        return response
    except Exception as e:
        print(f"Error rebooting EC2 instance {instance_id}: {e}")
        return None


# Create a snapshot of an EBS volume
def create_snapshot(volume_id, description=None, region_name="us-east-1"):
    ec2_client = init_ec2_client(region_name)
    try:
        response = ec2_client.create_snapshot(
            VolumeId=volume_id, Description=description
        )
        return response
    except Exception as e:
        print(f"Error creating snapshot for volume {volume_id}: {e}")
        return None


if __name__ == "__main__":
    # List EC2 instances
    print("Listing EC2 instances:")
    instances = list_ec2_instances()
    if instances is not None:
        for instance in instances:
            print(instance)
    else:
        print("Failed to retrieve instances.")

    # Start an instance
    instance_id = "i-0123456789abcdef0"  # Replace with your actual instance ID
    print(f"\nStarting instance {instance_id}:")
    start_response = start_ec2_instance(instance_id)
    print(start_response)

    # Stop an instance
    print(f"\nStopping instance {instance_id}:")
    stop_response = stop_ec2_instance(instance_id)
    print(stop_response)

    # Terminate an instance
    print(f"\nTerminating instance {instance_id}:")
    terminate_response = terminate_ec2_instance(instance_id)
    print(terminate_response)

    # Describe instance status
    print(f"\nDescribing status of instance {instance_id}:")
    status_response = describe_instance_status(instance_id)
    print(status_response)

    # Create a new instance
    image_id = "ami-0abcdef1234567890"  # Replace with your actual image ID
    instance_type = "t2.micro"
    key_name = "my-key-pair"  # Replace with actual key pair name
    security_group_ids = [
        "sg-0123456789abcdef0"
    ]  # Replace with your actual security group ID(s)

    print("\nCreating a new instance:")
    new_instance_response = create_ec2_instance(
        image_id=image_id,
        instance_type=instance_type,
        key_name=key_name,
        security_group_ids=security_group_ids,
    )
    print(new_instance_response)

    # Reboot an instance
    print(f"\nRebooting instance {instance_id}:")
    reboot_response = reboot_ec2_instance(instance_id)
    print(reboot_response)

    # Create a snapshot of a volume
    volume_id = "vol-0123456789abcdef0"  # Replace with your actual volume ID
    description = "My snapshot description"

    print(f"\nCreating snapshot for volume {volume_id}:")
    snapshot_response = create_snapshot(volume_id, description)
    print(snapshot_response)

# ==============
# EC2 manager class

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class EC2Manager:
    def __init__(self, region_name="us-east-1"):
        """
        Initialize the EC2Manager with the specified region.

        :param region_name: AWS region name, default is 'us-east-1'
        """
        self.region_name = region_name
        self.ec2_client = self.init_ec2_client()
        self.iam_client = boto3.client("iam")
        self.sts_client = boto3.client("sts")

    def init_ec2_client(self):
        """
        Initialize the EC2 client.

        :return: EC2 client object
        """
        return boto3.client("ec2", region_name=self.region_name)

    def get_account_id(self):
        """
        Get the AWS account ID.

        :return: AWS account ID or None if unable to determine
        """
        try:
            identity = self.sts_client.get_caller_identity()
            return identity["Account"]
        except (NoCredentialsError, PartialCredentialsError) as e:
            print("Credentials not available or incomplete: ", e)
            return None

    def list_ec2_instances(self):
        """
        List all EC2 instances in the account.

        :return: List of EC2 instances or None if an error occurs
        """
        try:
            response = self.ec2_client.describe_instances()
            instances = [
                instance
                for reservation in response["Reservations"]
                for instance in reservation["Instances"]
            ]
            return instances
        except Exception as e:
            print(f"Error listing EC2 instances: {e}")
            return None

    def start_ec2_instance(self, instance_id):
        """
        Start an EC2 instance.

        :param instance_id: ID of the instance to start
        :return: Response from the start_instances API call or None if an error occurs
        """
        try:
            response = self.ec2_client.start_instances(InstanceIds=[instance_id])
            return response
        except Exception as e:
            print(f"Error starting EC2 instance {instance_id}: {e}")
            return None

    def stop_ec2_instance(self, instance_id):
        """
        Stop an EC2 instance.

        :param instance_id: ID of the instance to stop
        :return: Response from the stop_instances API call or None if an error occurs
        """
        try:
            response = self.ec2_client.stop_instances(InstanceIds=[instance_id])
            return response
        except Exception as e:
            print(f"Error stopping EC2 instance {instance_id}: {e}")
            return None

    def terminate_ec2_instance(self, instance_id):
        """
        Terminate an EC2 instance.

        :param instance_id: ID of the instance to terminate
        :return: Response from the terminate_instances API call or None if an error occurs
        """
        try:
            response = self.ec2_client.terminate_instances(InstanceIds=[instance_id])
            return response
        except Exception as e:
            print(f"Error terminating EC2 instance {instance_id}: {e}")
            return None

    def describe_instance_status(self, instance_id):
        """
        Describe the status of an EC2 instance.

        :param instance_id: ID of the instance to describe
        :return: Response from the describe_instance_status API call or None if an error occurs
        """
        try:
            response = self.ec2_client.describe_instance_status(
                InstanceIds=[instance_id]
            )
            return response
        except Exception as e:
            print(f"Error describing status of EC2 instance {instance_id}: {e}")
            return None

    def create_ec2_instance(
        self, image_id, instance_type, key_name, security_group_ids
    ):
        """
        Create a new EC2 instance.

        :param image_id: ID of the AMI to use
        :param instance_type: Instance type (e.g., 't2.micro')
        :param key_name: Name of the key pair
        :param security_group_ids: List of security group IDs
        :return: Response from the run_instances API call or None if an error occurs
        """
        try:
            instances = self.ec2_client.run_instances(
                ImageId=image_id,
                InstanceType=instance_type,
                KeyName=key_name,
                SecurityGroupIds=security_group_ids,
                MinCount=1,
                MaxCount=1,
            )
            return instances
        except Exception as e:
            print(f"Error creating EC2 instance: {e}")
            return None

    def reboot_ec2_instance(self, instance_id):
        """
        Reboot an EC2 instance.

        :param instance_id: ID of the instance to reboot
        :return: Response from the reboot_instances API call or None if an error occurs
        """
        try:
            response = self.ec2_client.reboot_instances(InstanceIds=[instance_id])
            return response
        except Exception as e:
            print(f"Error rebooting EC2 instance {instance_id}: {e}")
            return None

    def create_snapshot(self, volume_id, description=None):
        """
        Create a snapshot of an EBS volume.

        :param volume_id: ID of the volume to snapshot
        :param description: Description for the snapshot
        :return: Response from the create_snapshot API call or None if an error occurs
        """
        try:
            response = self.ec2_client.create_snapshot(
                VolumeId=volume_id, Description=description
            )
            return response
        except Exception as e:
            print(f"Error creating snapshot for volume {volume_id}: {e}")
            return None

    def list_attached_roles(self):
        """
        List IAM roles attached to the current user.

        :return: List of attached IAM roles or None if an error occurs
        """
        try:
            user_info = self.iam_client.get_user()
            user_name = user_info["User"]["UserName"]
            response = self.iam_client.list_attached_user_policies(UserName=user_name)
            roles = response.get("AttachedPolicies", [])
            return roles
        except Exception as e:
            print(f"Error listing attached IAM roles: {e}")
            return None


# Comprehensive example of using the EC2Manager class
if __name__ == "__main__":
    ec2_manager = EC2Manager(region_name="us-east-1")

    # Get account ID
    account_id = ec2_manager.get_account_id()
    print(f"AWS Account ID: {account_id}")

    # List attached IAM roles
    print("\nListing attached IAM roles:")
    roles = ec2_manager.list_attached_roles()
    if roles is not None:
        for role in roles:
            print(role)
    else:
        print("Failed to retrieve attached IAM roles.")

    # List EC2 instances
    print("\nListing EC2 instances:")
    instances = ec2_manager.list_ec2_instances()
    if instances is not None:
        for instance in instances:
            print(instance)
    else:
        print("Failed to retrieve instances.")

    # Example instance ID for operations
    instance_id = "i-0123456789abcdef0"  # Replace with your actual instance ID

    # Start an instance
    print(f"\nStarting instance {instance_id}:")
    start_response = ec2_manager.start_ec2_instance(instance_id)
    print(start_response)

    # Stop an instance
    print(f"\nStopping instance {instance_id}:")
    stop_response = ec2_manager.stop_ec2_instance(instance_id)
    print(stop_response)

    # Terminate an instance
    print(f"\nTerminating instance {instance_id}:")
    terminate_response = ec2_manager.terminate_ec2_instance(instance_id)
    print(terminate_response)

    # Describe instance status
    print(f"\nDescribing status of instance {instance_id}:")
    status_response = ec2_manager.describe_instance_status(instance_id)
    print(status_response)

    # Create a new instance
    image_id = "ami-0abcdef1234567890"  # Replace with your actual image ID
    instance_type = "t2.micro"
    key_name = "my-key-pair"  # Replace with your actual key pair name
    security_group_ids = [
        "sg-0123456789abcdef0"
    ]  # Replace with your actual security group ID(s)

    print("\nCreating a new instance:")
    new_instance_response = ec2_manager.create_ec2_instance(
        image_id=image_id,
        instance_type=instance_type,
        key_name=key_name,
        security_group_ids=security_group_ids,
    )
    print(new_instance_response)

    # Reboot an instance
    print(f"\nRebooting instance {instance_id}:")
    reboot_response = ec2_manager.reboot_ec2_instance(instance_id)
    print(reboot_response)

    # Create a snapshot of a volume
    volume_id = "vol-0123456789abcdef0"  # Replace with your actual volume ID
    description = "My snapshot description"

    print(f"\nCreating snapshot for volume {volume_id}:")
    snapshot_response = ec2_manager.create_snapshot(volume_id, description)
    print(snapshot_response)
