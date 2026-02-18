IoT Anomaly Detection on AWS - Detailed Project Report

1. Introduction
This report presents the complete design, implementation, and deployment of an IoT anomaly detection pipeline on AWS. The project demonstrates how raw IoT device data can be ingested, processed, cleaned, analyzed for anomalies, and visualized using AWS services like S3, Glue, Athena, Lambda, and QuickSight. The pipeline follows a multi-layered architecture (Bronze, Silver, Gold), enabling structured data processing and anomaly detection at scale.
The purpose of this project is to showcase a real-world enterprise-grade data engineering pipeline. It includes step-by-step setup instructions, automation with Airflow, serverless anomaly detection with AWS Glue, alerts using Lambda + SNS, and visualization dashboards in QuickSight. Screenshots of the setup, Athena queries, Airflow DAG execution, and dashboards are included.

2. Architecture Overview
The architecture follows a medallion (Bronze → Silver → Gold) approach. Raw IoT sensor data is generated and stored in S3 (Bronze). AWS Glue jobs process the raw data into structured Silver datasets. Anomaly detection logic runs in the Gold job, identifying outliers using z-score thresholds. Results and summaries are stored in S3, queried with Athena, and visualized in QuickSight. Alerts are triggered via Lambda + SNS.

3. Setup and Configuration
This section provides detailed setup steps for each AWS component.

3.1 Prerequisites
- AWS account with admin permissions
- IAM user with programmatic access
- AWS CLI installed and configured
- Docker + Airflow for orchestration
- Python 3.9 with Boto3 and dependencies

3.2 S3 Bucket Setup
1. Create an S3 bucket for storing IoT data (bronze, silver, gold layers).
2. Setup folder prefixes:
   - bronze/
   - silver/
   - gold/
   - gold_summary/
3. Upload initial datasets or use generator scripts to simulate IoT data.

3.3 IAM Policies
IAM roles and policies were created to allow Glue, Lambda, and Athena access to S3. Policies are stored in the infra/iam-policies/ folder as JSON files.

3.4 Airflow DAG Setup
An Airflow DAG orchestrates the pipeline steps: CSV generation, S3 upload, Glue jobs, Lambda alerts, Athena validation, and QuickSight refresh. The DAG file (iot_ingest_dag.py) is in the dags/ folder.

3.5 AWS Glue Jobs
Two Glue jobs were created:
- silver_job.py: Cleans raw IoT data, adds partitions (year, month, day, hour).
- gold_job_zscore.py: Performs z-score based anomaly detection, writes anomalies to gold/ and summary to gold_summary/.

4. Troubleshooting
Common issues encountered and their fixes:
- InvalidAccessKeyId when uploading to S3 → Ensure AWS CLI is configured with correct IAM keys.
- No rows in Athena query → Run MSCK REPAIR TABLE after new partitions are written.
- Silver row count = 0 → Verify input CSV and filtering logic in Glue job.
- Lambda errors with json not defined → Add import json at the top of the function.

5. Cost Optimization
- Use S3 lifecycle policies to move older data to Glacier.
- Use Glue job bookmarks to avoid reprocessing.
- Use Athena partition pruning to minimize data scanned.
- Delete unused Lambda layers and test logs.
- Run Airflow locally to avoid Managed Workflows cost.

6. Visualizations
QuickSight dashboards were created with the following visuals:
1. Anomalies over time (Line chart)
2. Anomalies by device (Bar chart)
3. Average sensor values by location (Heatmap)
4. Summary statistics (Table)

7. Conclusion
This project demonstrates an end-to-end IoT anomaly detection pipeline using AWS cloud-native services. It highlights data engineering best practices including layered architecture, serverless anomaly detection, real-time alerting, and cost-efficient visualization. The architecture is extensible to support larger datasets, more complex ML models, and integration with real-time streaming data sources.
