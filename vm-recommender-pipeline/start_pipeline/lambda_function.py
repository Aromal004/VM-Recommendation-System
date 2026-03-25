import json
import boto3

sf = boto3.client("stepfunctions")

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:634914382615:stateMachine:Recommendation-Pipeline"

HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):

    # Handle CORS preflight
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": HEADERS, "body": ""}

    try:
        body = event.get("body")
        if body:
            body = json.loads(body)
        else:
            body = event

        required_fields = [
            "container_image",
            "port",
            "endpoints",       # list of {name, path}
            "total_requests",
            "concurrency"
        ]

        for field in required_fields:
            if field not in body:
                return {
                    "statusCode": 400,
                    "headers": HEADERS,
                    "body": json.dumps({"error": f"{field} missing"})
                }

        # Validate endpoints is a non-empty list
        endpoints = body["endpoints"]
        if not isinstance(endpoints, list) or len(endpoints) == 0:
            return {
                "statusCode": 400,
                "headers": HEADERS,
                "body": json.dumps({"error": "endpoints must be a non-empty list of {name, path}"})
            }

        for ep in endpoints:
            if "name" not in ep or "path" not in ep:
                return {
                    "statusCode": 400,
                    "headers": HEADERS,
                    "body": json.dumps({"error": "each endpoint must have 'name' and 'path' fields"})
                }

        response = sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(body)
        )

        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "message": "Pipeline started",
                "executionArn": response["executionArn"]
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)})
        }
