import os
import sys
import time
import boto3
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
# --------------------------
# Config
# --------------------------
AWS_REGION      = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET       = os.getenv("S3_BUCKET")
BRONZE_PREFIX   = os.getenv("BRONZE_PREFIX", "bronze")
GLUE_SILVER_JOB = "iot-silver-job"
GLUE_GOLD_JOB   = "iot-gold-job"
ATHENA_DB       = "iot_anomaly"
ATHENA_OUTPUT   = f"s3://{S3_BUCKET}/athena-results/"
LAMBDA_FUNC     = "IoTGoldAlert"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------
# Step 1: Generate CSV locally
# ----------------------------
def generate_csv(**ctx):
    # Run generator script
    subprocess.check_call([
        sys.executable,
        "/opt/airflow/scripts/generator/generate_iot_csv.py"
    ])

    # Build expected file name (matches script logic)
    ts = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    ts_str = ts.strftime("%Y%m%dT%H%M%SZ")
    file_name = f"iot_batch_{ts_str}.csv"
    local_path = f"/opt/airflow/tmp/{file_name}"

    proc_date = ts.strftime("%Y-%m-%d")
    proc_hour = ts.strftime("%H")

    print(f"[generate_csv] Created {local_path}, date={proc_date}, hour={proc_hour}")
    ctx['ti'].xcom_push(key="local_csv_path", value=local_path)
    ctx['ti'].xcom_push(key="proc_date", value=proc_date)
    ctx['ti'].xcom_push(key="proc_hour", value=proc_hour)

# ----------------------------
# Step 2: Upload to S3 (bronze)
# ----------------------------
def upload_to_s3(**ctx):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    local_path = ctx['ti'].xcom_pull(key="local_csv_path", task_ids="generate_csv")
    proc_date  = ctx['ti'].xcom_pull(key="proc_date", task_ids="generate_csv")
    proc_hour  = ctx['ti'].xcom_pull(key="proc_hour", task_ids="generate_csv")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"{local_path} not found")

    file_name = os.path.basename(local_path)
    key = f"{BRONZE_PREFIX}/dt={proc_date}/hour={proc_hour}/{file_name}"

    s3.upload_file(local_path, S3_BUCKET, key)
    print(f"[upload_to_s3] Uploaded {local_path} → s3://{S3_BUCKET}/{key}")
    ctx['ti'].xcom_push(key="bronze_key", value=key)

# --------------------------
# Step 3 & 4: Run Glue Jobs with Polling
# --------------------------
def run_glue_job(job_name, **ctx):
    glue = boto3.client("glue", region_name=AWS_REGION)

    proc_date = ctx['ti'].xcom_pull(key="proc_date", task_ids="generate_csv")
    proc_hour = ctx['ti'].xcom_pull(key="proc_hour", task_ids="generate_csv")

    args = {
        "--s3_bucket": S3_BUCKET,
        "--proc_date": proc_date,
        "--proc_hour": proc_hour
    }

    response = glue.start_job_run(JobName=job_name, Arguments=args)
    run_id = response["JobRunId"]
    print(f"Started Glue job {job_name} with RunId: {run_id}, date={proc_date}, hour={proc_hour}")

    # Poll until Glue finishes
    while True:
        job = glue.get_job_run(JobName=job_name, RunId=run_id)
        run_state = job["JobRun"]["JobRunState"]
        print(f"Job {job_name} status: {run_state}")
        if run_state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            break
        time.sleep(30)

    if run_state != "SUCCEEDED":
        raise Exception(f"Glue job {job_name} failed with status: {run_state}")

    print(f"Glue job {job_name} finished successfully")
    return run_id

# --------------------------
# Step 5: Fetch summary JSON
# --------------------------
def fetch_summary(**ctx):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    proc_date = ctx['ti'].xcom_pull(key="proc_date", task_ids="generate_csv")
    proc_hour = ctx['ti'].xcom_pull(key="proc_hour", task_ids="generate_csv")

    summary_key = f"gold_summary/dt={proc_date}/hour={proc_hour}/summary.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=summary_key)
        content = obj["Body"].read().decode("utf-8")
        print("Summary JSON:\n", content)
    except Exception as e:
        print(f"Could not fetch summary.json: {e}")

# --------------------------
# Step 6: Validate In Athena
# --------------------------
def athena_validate(**ctx):
    athena = boto3.client("athena", region_name=AWS_REGION)

    query = """
        SELECT proc_date, proc_hour, anomaly_count
        FROM gold_summary
        ORDER BY proc_date DESC, proc_hour DESC
        LIMIT 5
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    execution_id = response["QueryExecutionId"]
    print(f"Athena query started: {execution_id}")

    # Poll until query finishes
    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(5)

    if state != "SUCCEEDED":
        raise Exception(f"Athena query failed with state: {state}")

    # Fetch results
    results = athena.get_query_results(QueryExecutionId=execution_id)
    rows = results["ResultSet"]["Rows"]

    print("Athena Validation Results:")
    for row in rows:
        print([col.get("VarCharValue", "") for col in row["Data"]])
        
    # --------------------------
# Step 7: Invoke Lambda after Gold
# --------------------------
def invoke_lambda(**ctx):
    client = boto3.client("lambda", region_name=AWS_REGION)

    proc_date = ctx['ti'].xcom_pull(key="proc_date", task_ids="generate_csv")
    proc_hour = ctx['ti'].xcom_pull(key="proc_hour", task_ids="generate_csv")

    payload = {
        "Records": [{
            "s3": {
                "bucket": {"name": S3_BUCKET},
                "object": {"key": f"gold_summary/dt={proc_date}/hour={proc_hour}/summary.json"}
            }
        }]
    }

    response = client.invoke(
    FunctionName=LAMBDA_FUNC,
    InvocationType="RequestResponse",  # sync call
    Payload=json.dumps(payload),
    )

    result = json.loads(response["Payload"].read())
    print("Lambda result:", result)

# --------------------------
# DAG Definition
# --------------------------
with DAG(
    dag_id="iot_ingest_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule="0 * * * *",   # hourly
    catchup=False,
    default_args=default_args,
    tags=["iot","anomaly"],
) as dag:

    t1 = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_csv
    )

    t2 = PythonOperator(
        task_id="upload_bronze",
        python_callable=upload_to_s3
    )

    t3 = PythonOperator(
        task_id="run_silver",
        python_callable=lambda **ctx: run_glue_job(GLUE_SILVER_JOB, **ctx)
    )

    t4 = PythonOperator(
        task_id="run_gold",
        python_callable=lambda **ctx: run_glue_job(GLUE_GOLD_JOB, **ctx)
    )

    t5 = PythonOperator(
        task_id="fetch_summary",
        python_callable=fetch_summary
    )

    t6 = PythonOperator(
        task_id="athena_validate",
        python_callable=athena_validate
    )
    
    t7 = PythonOperator(
        task_id="invoke_lambda", 
        python_callable=invoke_lambda
    ) 

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
