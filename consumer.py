import io
import sys
from confluent_kafka import Consumer
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pymongo
import requests

# ----------------------
# Wiring
# ----------------------
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka1.dlandau.nl:19092',
    'group.id': '14452',   # use a NEW id when replaying
    'auto.offset.reset': 'latest',    # use 'earliest' to get ExperimentConfig
    'security.protocol': 'SSL',
    'ssl.ca.location': '/app/auth/ca.crt',
    'ssl.keystore.location': '/app/auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'enable.auto.commit': 'true',
    'ssl.endpoint.identification.algorithm': 'none',
}

TOPIC = "group3"

MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "test"

NOTIFY_URL = (
    "https://notifications-service-cec.ad.dlandau.nl/api/notify"
    "?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3NjY0MDg0MzAsInN1YiI6Imdyb3VwMyJ9.UqC2ZtMDVJhgil2LxCFTVYlO_yCvWx1vR-A3OAznAVxzY_6FTHm-e24qAvBWOPb_P2ZMtBmcsbRQym2K_TZbrtEhW1LBkQokCEYIa15rjeuR9q90x0rkbf4VHipK59dc45Z5VgWV07AImHAapb-peiWUhcES951J98XJQHgI-1JRbw-ThcUd3sKQEX-_CsYCy_JWWV5Q3Mn2RzrGcAgxQeBfhgri9cq9uQ8zrJBjaecGXE6nu8A7tWmn6WXoyBpCBpHDRgoQ4KvcLnePbpEehxgmRhLppHSNBvAL4K_xd4X_OFlr1fDKk5gJF_00dLgRKXhwUmLREzsu5J9I2dXI9g")
NOTIFY_HEADERS = {"accept": "/", "Content-Type": "application/json; charset=utf-8"}

# ----------------------
# State (old-style)
# ----------------------
experiment_data = {}  # exp_id -> dict

# ----------------------
# Helpers
# ----------------------
def in_range(v, lo, hi):
    # inclusive bounds to avoid edge misses
    return lo <= v <= hi

def get_record_name(msg):
    hdrs = dict(msg.headers() or [])
    rn = hdrs.get("record_name")
    if isinstance(rn, bytes):
        rn = rn.decode("utf-8")
    return rn

def iter_avro_records(msg_value):
    with DataFileReader(io.BytesIO(msg_value), DatumReader()) as rdr:
        for rec in rdr:
            yield rec

def notify(payload):
    try:
        resp = requests.post(NOTIFY_URL, headers=NOTIFY_HEADERS, json=payload, timeout=5)
        print(f"[NOTIFY {payload['notification_type']}] {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"[NOTIFY ERROR] {e}", file=sys.stderr)

# ----------------------
# Decode / process
# ----------------------
def decode(msg_value, record_name, experiments_col, records_col):
    print("---STARTING---")
    for msg in iter_avro_records(msg_value):
        exp_id = msg.get("experiment")
        if not exp_id:
            continue

        if record_name == "ExperimentConfig":
            experiment_data[exp_id] = {
                "exp_id": exp_id,
                "temp_min_threshold": float(msg["temperature_range"]["lower_threshold"]),
                "temp_max_threshold": float(msg["temperature_range"]["upper_threshold"]),
                "nr_sensors": int(len(msg["sensors"])),
                "researcher": msg["researcher"],

                # phase flags (old-style)
                "stabilizing": False,        # True after stabilization_started
                "stabilized": False,         # send stabilized once
                "experiment_started": False, # True after experiment_started
                "previously_oor": False,

                # aggregation
                "average_temp": 0.0,
                "sensors_counted": 0,
            }
            # (optional) persist summary/config if you want
            experiments_col.update_one(
                {"exp_id": exp_id},
                {"$set": {
                    "temp_min_threshold": experiment_data[exp_id]["temp_min_threshold"],
                    "temp_max_threshold": experiment_data[exp_id]["temp_max_threshold"],
                    "nr_sensors": experiment_data[exp_id]["nr_sensors"],
                    "researcher": experiment_data[exp_id]["researcher"],
                }},
                upsert=True
            )
            print(f"[{exp_id}] ExperimentConfig loaded")

        elif record_name == "stabilization_started":
            if exp_id in experiment_data:
                experiment_data[exp_id]["stabilizing"] = True
                print(f"[{exp_id}] stabilization_started at {msg.get('timestamp')}")

        elif record_name == "experiment_started":
            if exp_id in experiment_data:
                experiment_data[exp_id]["experiment_started"] = True
                print(f"[{exp_id}] experiment_started at {msg.get('timestamp')}")

        elif record_name == "sensor_temperature_measured":
            # guard: skip if we never saw config for this exp_id
            if exp_id not in experiment_data:
                print(f"[{exp_id}] measurement but no config (start consumer earlier); skipping")
                continue

            st = experiment_data[exp_id]
            # aggregate simple average over sensors per tick
            st["average_temp"] += float(msg["temperature"]) / st["nr_sensors"]
            st["sensors_counted"] += 1

            if st["sensors_counted"] == st["nr_sensors"]:
                avg = st["average_temp"]
                ts = msg["timestamp"]
                mid = msg.get("measurement_id") or msg.get("measurement-id")
                mhash = msg["measurement_hash"]

                # Stabilization: send Stabilized once when entering range
                if st["stabilizing"] and not st["stabilized"] and in_range(avg, st["temp_min_threshold"], st["temp_max_threshold"]):
                    notify({
                        "notification_type": "Stabilized",
                        "researcher": st["researcher"],
                        "measurement_id": mid,
                        "experiment_id": exp_id,
                        "cipher_data": mhash,
                    })
                    st["stabilized"] = True
                    print(f"[{exp_id}] STABILIZED avg={avg} at ts={ts}")

                # Experiment: store + OutOfRange edge notify
                if st["experiment_started"]:
                    records_col.insert_one({"exp_id": exp_id, "timestamp": ts, "avg_temperature": avg})

                    if not in_range(avg, st["temp_min_threshold"], st["temp_max_threshold"]):
                        if not st["previously_oor"]:
                            notify({
                                "notification_type": "OutOfRange",
                                "researcher": st["researcher"],
                                "measurement_id": mid,
                                "experiment_id": exp_id,
                                "cipher_data": mhash,
                            })
                            print(f"[{exp_id}] OUT_OF_RANGE avg={avg} at ts={ts}")
                        st["previously_oor"] = True
                    else:
                        st["previously_oor"] = False

                # reset for next tick
                st["average_temp"] = 0.0
                st["sensors_counted"] = 0

        elif record_name == "experiment_terminated":
            if exp_id in experiment_data:
                experiments_col.update_one(
                    {"exp_id": exp_id},
                    {"$set": {"terminated_at": msg.get("timestamp")}},
                    upsert=True
                )
                print(f"[{exp_id}] experiment_terminated; summary stored")
                # optional: free memory
                # del experiment_data[exp_id]


def main():
    # Mongo
    conn = pymongo.MongoClient(MONGO_URI)
    print("ESTABLISHED CONNECTION TO DATABASE")
    db = conn[DB_NAME]
    experiments_col = db["experiments"]
    records_col = db["records"]

    # Kafka
    c = Consumer(KAFKA_CONFIG)

   
    from confluent_kafka import OFFSET_BEGINNING
    def rewind_on_assign(consumer, partitions):
        print(f"Partitions assigned: {partitions} — rewinding to beginning")
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    c.subscribe([TOPIC], on_assign=rewind_on_assign)
    print(f"Subscribed to topic: {TOPIC}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            record_name = get_record_name(msg)
            if not record_name:
                print("[WARN] Missing 'record_name' header; skipping")
                continue

            try:
                decode(msg.value(), record_name, experiments_col, records_col)
            except Exception as e:
                print(f"[PROCESSING ERROR] {e}", file=sys.stderr)

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        c.close()

if __name__ == "__main__":
    main()