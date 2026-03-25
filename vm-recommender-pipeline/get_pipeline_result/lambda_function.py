import json
import boto3

sf = boto3.client("stepfunctions")

HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):

    # Handle CORS preflight
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": HEADERS, "body": ""}

    try:
        params        = event.get("queryStringParameters") or {}
        execution_arn = params.get("executionArn")

        if not execution_arn:
            return {
                "statusCode": 400,
                "headers": HEADERS,
                "body": json.dumps({"error": "executionArn query param required"})
            }

        response = sf.describe_execution(executionArn=execution_arn)
        status   = response["status"]

        if status == "SUCCEEDED":
            output = json.loads(response["output"])

            recommendations = output.get("recommendation_result", {}).get(
                "recommended_instances", []
            )
            metrics   = output.get("metrics_result", {})
            inference = output.get("inference_result", {})

            # Per-endpoint A/B breakdown lives inside metrics_result
            ab_results = metrics.get("ab_results", {})

            return {
                "statusCode": 200,
                "headers": HEADERS,
                "body": json.dumps({
                    "status":          "SUCCEEDED",
                    "recommendations": recommendations,
                    "metrics":         metrics,
                    "inference":       inference,
                    "ab_results":      ab_results,
                })
            }

        if status == "FAILED":
            return {
                "statusCode": 200,
                "headers": HEADERS,
                "body": json.dumps({
                    "status": "FAILED",
                    "error":  response.get("cause", "Unknown error")
                })
            }

        # RUNNING / WAITING
        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "status":  status,
                "message": "Pipeline still running. Poll again in 15 seconds."
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)})
        }
