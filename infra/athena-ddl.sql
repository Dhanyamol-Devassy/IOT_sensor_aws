-- Create Athena database if not exists
CREATE DATABASE IF NOT EXISTS iot_anomaly;
USE iot_anomaly;

-- ----------------------------
-- Silver table (clean IoT data)
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS silver_events (
    event_time   timestamp,
    temp_c       double,
    humidity_pc  double,
    vibration_g  double,
    location     string
)
PARTITIONED BY (
    year  int,
    month int,
    day   int,
    hour  int,
    device_id string
)
STORED AS PARQUET
LOCATION 's3://iot-anomaly-dhanya/silver/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- ----------------------------
-- Gold anomalies table
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold_anomalies (
    event_time   timestamp,
    temp_c       double,
    humidity_pc  double,
    vibration_g  double,
    location     string
)
PARTITIONED BY (
    year  int,
    month int,
    day   int,
    hour  int,
    device_id string
)
STORED AS PARQUET
LOCATION 's3://iot-anomaly-dhanya/gold/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- ----------------------------
-- Gold summary table
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold_summary (
    anomaly_count int,
    proc_date     string,
    proc_hour     string
)
PARTITIONED BY (
    dt   string,
    hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://iot-anomaly-dhanya/gold_summary/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- After creating tables, refresh partitions
MSCK REPAIR TABLE silver_events;
MSCK REPAIR TABLE gold_anomalies;
MSCK REPAIR TABLE gold_summary;
