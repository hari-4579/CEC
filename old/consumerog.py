#!/usr/bin/env python3
import io
import os
import sys
import json
import time
import signal
import threading
from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Consumer
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import pymongo
import requests


# CONFIG 

KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka1.dlandau.nl:19092,kafka2.dlandau.nl:29092,kafka3.dlandau.nl:39092',
    'group.id': 'group3_123456',
    'auto.offset.reset': 'latest',
    'security.protocol': 'SSL',
    'ssl.ca.location': '/app/auth/ca.crt',
    'ssl.keystore.location': '/app/auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'enable.auto.commit': 'true',
    'ssl.endpoint.identification.algorithm': 'none',
}
TEAM_TOPIC = os.getenv("TEAM_TOPIC", "group3")
NOTIFY_URL = "https://notifications-service-cec.ad.dlandau.nl/api/notify?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3NjY0MDg0MzAsInN1YiI6Imdyb3VwMyJ9.UqC2ZtMDVJhgil2LxCFTVYlO_yCvWx1vR-A3OAznAVxzY_6FTHm-e24qAvBWOPb_P2ZMtBmcsbRQym2K_TZbrtEhW1LBkQokCEYIa15rjeuR9q90x0rkbf4VHipK59dc45Z5VgWV07AImHAapb-peiWUhcES951J98XJQHgI-1JRbw-ThcUd3sKQEX-_CsYCy_JWWV5Q3Mn2RzrGcAgxQeBfhgri9cq9uQ8zrJBjaecGXE6nu8A7tWmn6WXoyBpCBpHDRgoQ4KvcLnePbpEehxgmRhLppHSNBvAL4K_xd4X_OFlr1fDKk5gJF_00dLgRKXhwUmLREzsu5J9I2dXI9g"
NOTIFY_HEADERS = {"accept": "/", "Content-Type": "application/json; charset=utf-8"}

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb-0.mongodb:27017/test?gssapiServiceName=mongodb")
DB_NAME = os.getenv("DB_NAME", "test")


# Avro Schemas (writer)

SCHEMAS_RAW = {
    "experiment_configured": """
    {"type":"record","name":"ExperimentConfig",
     "fields":[
       {"type":"string","name":"experiment"},
       {"type":"string","name":"researcher"},
       {"name":"sensors","type":{"type":"array","items":"string"}},
       {"name":"temperature_range","type":{
          "type":"record","name":"temperature_range",
          "fields":[
            {"name":"upper_threshold","type":"float"},
            {"name":"lower_threshold","type":"float"}
          ]}}
     ]}
    """,
    "stabilization_started": """
    {"type":"record","name":"stabilization_started",
     "fields":[
       {"type":"string","name":"experiment"},
       {"name":"timestamp","type":"double"}
     ]}
    """,
    "experiment_started": """
    {"type":"record","name":"experiment_started",
     "fields":[
       {"type":"string","name":"experiment"},
       {"name":"timestamp","type":"double"}
     ]}
    """,
    "sensor_temperature_measured": """
    {"type":"record","name":"sensor_temperature_measured",
     "fields":[
       {"name":"experiment","type":"string"},
       {"name":"sensor","type":"string"},
       {"name":"measurement_id","type":"string"},
       {"name":"timestamp","type":"double"},
       {"name":"temperature","type":"float"},
       {"name":"measurement_hash","type":"string"}
     ]}
    """,
    "experiment_terminated": """
    {"type":"record","name":"experiment_terminated",
     "fields":[
       {"type":"string","name":"experiment"},
       {"name":"timestamp","type":"double"}
     ]}
    """,
}
SCHEMAS = {k: avro.schema.parse(v) for k, v in SCHEMAS_RAW.items()}

def decode_avro(msg_bytes: bytes, record_name: str) -> dict:
    schema = SCHEMAS.get(record_name)
    if not schema:
        raise ValueError(f"Unknown record_name in header: {record_name}")
    b = io.BytesIO(msg_bytes)
    dec = BinaryDecoder(b)
    reader = DatumReader(schema)
    return reader.read(dec)


# Mongo setup

mongo = pymongo.MongoClient(MONGO_URI)
db = mongo[DB_NAME]

# Create time-series collection if not present
try:
    db.create_collection(
        "records",
        timeseries={"timeField": "timestamp", "metaField": "experiment_id", "granularity": "seconds"},
    )
except Exception:
    pass  # likely already exists

experiments_coll = db["experiments"]
records_coll = db["records"]
excursions_coll = db["excursions"]


# In-memory state

class ExpState:
    _slots_ = ("exp_id", "email", "lower", "upper", "nr_sensors",
                 "monitoring", "started", "stabilized_sent", "previously_oor",
                 "buffer_by_mid")

    def _init_(self, exp_id, email, lower, upper, nr_sensors):
        self.exp_id = exp_id
        self.email = email
        self.lower = lower
        self.upper = upper
        self.nr_sensors = nr_sensors
        self.monitoring = False     # after stabilization_started
        self.started = False        # after experiment_started
        self.stabilized_sent = False
        self.previously_oor = False
        # measurement_id -> {"temps":[...], "hash": str, "ts": float}
        self.buffer_by_mid = defaultdict(lambda: {"temps": [], "hash": None, "ts": None})

state: dict[str, ExpState] = {}


def send_notification(payload: dict):
    try:
        r = requests.post(NOTIFY_URL, headers=NOTIFY_HEADERS, json=payload, timeout=3)
        print(f"[notify {payload['notification_type']}] {r.status_code} {r.text}")
    except Exception as e:
        print(f"[notify error] {e}", file=sys.stderr)

def notify_stabilized(exp: ExpState, measurement_id: str, cipher: str):
    payload = {
        "notification_type": "Stabilized",
        "researcher": exp.email,
        "experiment_id": exp.exp_id,
        "measurement_id": measurement_id,
        "cipher_data": cipher,
    }
    threading.Thread(target=send_notification, args=(payload,), daemon=True).start()

def notify_out_of_range(exp: ExpState, measurement_id: str, cipher: str):
    payload = {
        "notification_type": "OutOfRange",
        "researcher": exp.email,
        "experiment_id": exp.exp_id,
        "measurement_id": measurement_id,
        "cipher_data": cipher,
    }
    threading.Thread(target=send_notification, args=(payload,), daemon=True).start()

def epoch_to_dt(ts: float):
    return datetime.fromtimestamp(ts, tz=timezone.utc)

def handle_average_sample(exp: ExpState, avg: float, ts: float, measurement_id: str, mhash: str):
    in_range = exp.lower <= avg <= exp.upper

    # During stabilization window (monitoring==True, started==False):
    if exp.monitoring and not exp.started and not exp.stabilized_sent:
        if in_range:
            notify_stabilized(exp, measurement_id, mhash)
            exp.stabilized_sent = True
        return

    # During running experiment:
    if exp.started:
        # store point (RUNNING phase only)
        records_coll.insert_one({
            "experiment_id": exp.exp_id,
            "timestamp": epoch_to_dt(ts),
            "temperature": float(avg),
            "measurement_id": measurement_id,
            "measurement_hash": mhash,
        })

        # Transition detection for out-of-range
        if not in_range and not exp.previously_oor:
            # store first point of excursion & notify
            excursions_coll.insert_one({
                "experiment_id": exp.exp_id,
                "timestamp": epoch_to_dt(ts),
                "temperature": float(avg),
                "measurement_id": measurement_id,
                "measurement_hash": mhash,
            })
            notify_out_of_range(exp, measurement_id, mhash)
            exp.previously_oor = True
        elif in_range:
            exp.previously_oor = False

def process_measurement(msg: dict):
    exp_id = msg["experiment"]
    if exp_id not in state:
        # Unknown experiment; ignore safely
        return
    exp = state[exp_id]

    mid = msg["measurement_id"]
    buf = exp.buffer_by_mid[mid]
    buf["temps"].append(float(msg["temperature"]))
    buf["hash"] = msg["measurement_hash"]
    buf["ts"] = float(msg["timestamp"])

    if len(buf["temps"]) >= exp.nr_sensors:
        avg = sum(buf["temps"]) / exp.nr_sensors
        handle_average_sample(exp, avg, buf["ts"], mid, buf["hash"])
        # clear buffer for this measurement_id
        del exp.buffer_by_mid[mid]

def handle_event(record_name: str, msg: dict):
    exp_id = msg["experiment"]

    if record_name == "experiment_configured":
        lower = float(msg["temperature_range"]["lower_threshold"])
        upper = float(msg["temperature_range"]["upper_threshold"])
        email = msg["researcher"]
        nr = int(len(msg["sensors"]))
        state[exp_id] = ExpState(exp_id, email, lower, upper, nr)
        print(f"[config] {exp_id} sensors={nr} range=({lower},{upper}) -> {email}")

    elif record_name == "stabilization_started":
        if exp_id in state:
            state[exp_id].monitoring = True
            print(f"[stabilization_started] {exp_id}")

    elif record_name == "experiment_started":
        if exp_id in state:
            state[exp_id].started = True
            print(f"[experiment_started] {exp_id}")

    elif record_name == "sensor_temperature_measured":
        process_measurement(msg)

    elif record_name == "experiment_terminated":
        # persist final experiment metadata and clean up memory
        if exp_id in state:
            exp = state[exp_id]
            experiments_coll.insert_one({
                "experiment_id": exp.exp_id,
                "researcher": exp.email,
                "lower": exp.lower,
                "upper": exp.upper,
                "nr_sensors": exp.nr_sensors,
                "terminated_at": epoch_to_dt(msg["timestamp"]),
            })
            del state[exp_id]
            print(f"[terminated] {exp_id} (state cleaned)")


running = True
def shutdown(*_):
    global running
    running = False
signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def main():
    print("Connecting to Mongo…")
    mongo.admin.command("ping")
    print("Mongo OK.")

    print("Starting Kafka consumer…")
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TEAM_TOPIC], on_assign=lambda _, p: print(f"Assigned: {p}"))

    while running:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[kafka error] {msg.error()}", file=sys.stderr)
                continue

            headers = dict(msg.headers() or [])
            # record_name is sent in header as bytes
            record_name = headers.get('record_name') or headers.get('record-name') or headers.get('recordName')
            if isinstance(record_name, bytes):
                record_name = record_name.decode('utf-8')

            if not record_name:
                print("[warn] missing record_name header; skipping")
                continue

            try:
                payload = decode_avro(msg.value(), record_name)
            except Exception as e:
                print(f"[avro decode error] {e}", file=sys.stderr)
                continue

            handle_event(record_name, payload)

        except Exception as e:
            print(f"[loop error] {e}", file=sys.stderr)

    print("Shutting down…")
    consumer.close()

if _name_ == "_main_":
    main()