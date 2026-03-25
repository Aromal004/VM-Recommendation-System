import boto3

ec2 = boto3.client("ec2")

def lambda_handler(event, context):

    instance_id = event.get("instance_id")

    if not instance_id:
        return {"error": "instance_id not provided"}

    ec2.terminate_instances(
        InstanceIds=[instance_id]
    )

    return {
        "message": "Instance terminated successfully",
        "instance_id": instance_id
    }
