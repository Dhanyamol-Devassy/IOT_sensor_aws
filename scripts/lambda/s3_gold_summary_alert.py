import os, json, boto3

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

s3 = boto3.client("s3")
sns = boto3.client("sns")

def lambda_handler(event, context):
    # Triggered by S3 Put on gold_summary/...
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    # Read JSON summary
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8").strip()
    try:
        data = json.loads(body) if body else []
    except json.JSONDecodeError:
        print(f"Invalid JSON in {bucket}/{key}")
        data = []

    total = sum(int(item.get("anomaly_count", 0)) for item in data)

    if total > 0:
        msg = f"🚨 [IoT Anomaly] {total} anomalies detected in {bucket}/{key}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="IoT Anomaly Alert",
            Message=msg
        )
        status = "ALERT"
    else:
        print(f"No anomalies detected in {bucket}/{key}")
        status = "OK"

    return {"status": status, "anomalies": total}
