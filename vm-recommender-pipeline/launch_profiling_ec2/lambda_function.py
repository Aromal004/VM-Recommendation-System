import boto3

ec2 = boto3.client("ec2")

def lambda_handler(event, context):

    container_image = event["container_image"]
    port            = event["port"]
    endpoints       = event["endpoints"]   # list of {name, path}
    total_requests  = event["total_requests"]
    concurrency     = event["concurrency"]

    # Build one ab command + S3 upload per endpoint
    ab_commands = ""
    s3_uploads  = ""
    for ep in endpoints:
        name = ep["name"]
        path = ep["path"]
        ab_commands += f"""
echo "=== AB test: {name} ({path}) ==="
ab -n {total_requests} -c {concurrency} \\
   -e /tmp/ab_{name}.csv \\
   -g /tmp/ab_{name}.tsv \\
   http://localhost:{port}{path}
"""
        s3_uploads += f"""
aws s3 cp /tmp/ab_{name}.csv s3://vm-recommendation-data/profiling/$INSTANCE_ID/ab_{name}.csv
aws s3 cp /tmp/ab_{name}.tsv s3://vm-recommendation-data/profiling/$INSTANCE_ID/ab_{name}.tsv
"""

    user_data_script = f"""#!/bin/bash
set -e

docker run -d -p {port}:{port} {container_image}
sleep 30

TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \\
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s \\
  -H "X-aws-ec2-metadata-token: $TOKEN" \\
  http://169.254.169.254/latest/meta-data/instance-id)

{ab_commands}
{s3_uploads}

aws s3 cp /dev/stdin \\
  s3://vm-recommendation-data/profiling/$INSTANCE_ID/done.txt <<< "done"
"""

    response = ec2.run_instances(
        ImageId="ami-016d9022e2ce362d1",
        InstanceType="t3.large",
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={"Name": "ProfilingEC2Role2"},
        UserData=user_data_script
    )

    instance_id = response["Instances"][0]["InstanceId"]

    return {
        "instance_id":     instance_id,
        "container_image": container_image,
        "port":            port,
        "endpoints":       endpoints,
        "total_requests":  total_requests,
        "concurrency":     concurrency
    }
