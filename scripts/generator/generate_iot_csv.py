import os, csv, random, time
from datetime import datetime, timedelta
from faker import Faker

# Env
bucket   = os.getenv("S3_BUCKET")
device_count = int(os.getenv("DEVICE_COUNT", "50"))
recs_per_device = int(os.getenv("RECS_PER_DEVICE", "40"))
anomaly_rate = float(os.getenv("ANOMALY_RATE", "0.03"))

# Output path (local temp; Airflow task uploads to S3)
out_dir = "/opt/airflow/tmp"
os.makedirs(out_dir, exist_ok=True)

fake = Faker()
now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
start = now - timedelta(minutes=recs_per_device)  # one reading per minute

def maybe_anomalize(value):
    # inject occasional spikes/drops
    if random.random() < anomaly_rate:
        mult = random.choice([0.2, 0.3, 3.0, 3.5])  # extreme low or high
        return round(value * mult, 2), True
    return value, False

def baseline_temp(): return round(random.uniform(20.0, 28.0), 2)
def baseline_hum():  return round(random.uniform(35.0, 65.0), 2)
def baseline_vib():  return round(random.uniform(0.1, 3.0), 3)

def generate_file():
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")
    file_name = f"iot_batch_{ts_str}.csv"
    file_path = os.path.join(out_dir, file_name)

    with open(file_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_time","device_id","temp_c","humidity_pc","vibration_g","location"])
        for d in range(device_count):
            device_id = f"dev-{d:03d}"
            # ensure at least a few rows per device
            cur = start
            for _ in range(recs_per_device):
                base_t, base_h, base_v = baseline_temp(), baseline_hum(), baseline_vib()
                t, a1 = maybe_anomalize(base_t)
                h, a2 = maybe_anomalize(base_h)
                v, a3 = maybe_anomalize(base_v)
                w.writerow([
                    cur.isoformat(),
                    device_id,
                    t if random.random() > 0.02 else "",       # occasional missing
                    h if random.random() > 0.02 else "",
                    v if random.random() > 0.02 else "",
                    fake.city()
                ])
                cur += timedelta(minutes=1)
    return file_path, file_name

if __name__ == "__main__":
    p, n = generate_file()
    print(p)
