import io
import sys
import time
import logging
from confluent_kafka import Consumer, OFFSET_BEGINNING
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pymongo
from pymongo.errors import DuplicateKeyError, PyMongoError
import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")


# Wiring / config

KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka1.dlandau.nl:19092',
    'group.id': '14452',   # change for fresh replay
    'auto.offset.reset': 'latest',    # 'earliest' if want replay
    'security.protocol': 'SSL',
    'ssl.ca.location': '/app/auth/ca.crt',
    'ssl.keystore.location': '/app/auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'ssl.endpoint.identification.algorithm': 'none',
   
    'enable.auto.commit': False,
}

TOPIC = "experiment"

MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "test"

# Notification endpoint -
NOTIFY_URL = (
    "https://notifications-service-cec.ad.dlandau.nl/api/notify"
    "?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJleHAiOjE3NjY0MDg0MzAsInN1YiI6Imdyb3VwMyJ9.UqC2ZtMDVJhgil2LxCFTVYlO_yCvWx1vR-A3OAznAVxzY_6FTHm-e24qAvBWOPb_P2ZMtBmcsbRQym2K_TZbrtEhW1LBkQokCEYIa15rjeuR9q90x0rkbf4VHipK59dc45Z5VgWV07AImHAapb-peiWUhcES951J98XJQHgI-1JRbw-ThcUd3sKQEX-_CsYCy_JWWV5Q3Mn2RzrGcAgxQeBfhgri9cq9uQ8zrJBjaecGXE6nu8A7tWmn6WXoyBpCBpHDRgoQ4KvcLnePbpEehxgmRhLppHSNBvAL4K_xd4X_OFlr1fDKk5gJF_00dLgRKXhwUmLREzsu5J9I2dXI9g"
)
NOTIFY_HEADERS = {"Accept": "/", "Content-Type": "application/json; charset=utf-8"}

experiment_data = {}


# Helpers

def in_range(v, lo, hi):
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

def notify(payload, retries=3, backoff=0.5):

    # Send notification with simple retry/backoff. Returns True if a 2xx response was received.
    
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(NOTIFY_URL, headers=NOTIFY_HEADERS, json=payload, timeout=5)
            log.info("[NOTIFY %s] attempt=%d status=%s text=%s", payload.get("notification_type"), attempt, resp.status_code, resp.text)
            if 200 <= resp.status_code < 300:
                return True
            # treat other status codes as retryable a few times (up to you)
        except Exception as e:
            log.warning("Notify attempt %d failed: %s", attempt, e)
        if attempt < retries:
            time.sleep(backoff * attempt)
    return False

# DB utils

def ensure_indexes(db):
    try:
        records_col = db["records"]
       
        records_col.create_index(
            [("exp_id", 1), ("measurement_id", 1)],
            unique=True,
            partialFilterExpression={"measurement_id": {"$exists": True, "$ne": None}}
        )
        db["experiments"].create_index([("exp_id", 1)], unique=True)
        log.info("Ensured partial unique index on records and unique index on experiments")
    except Exception as e:
        log.exception("Error creating indexes: %s", e)
        
def load_experiments_from_db(experiments_col):
    
    #Load persisted experiment metadata and state into in-memory experiment_data
    
    try:
        for exp in experiments_col.find({}):
            exp_id = exp.get("exp_id")
            if not exp_id:
                continue
            experiment_data[exp_id] = {
                "exp_id": exp_id,
                "temp_min_threshold": float(exp.get("temp_min_threshold")) if exp.get("temp_min_threshold") is not None else None,
                "temp_max_threshold": float(exp.get("temp_max_threshold")) if exp.get("temp_max_threshold") is not None else None,
                "nr_sensors": int(exp.get("nr_sensors")) if exp.get("nr_sensors") is not None else None,
                "researcher": exp.get("researcher"),
                "stabilizing": bool(exp.get("stabilizing", False)),
                "stabilized": bool(exp.get("stabilized", False)),
                "experiment_started": bool(exp.get("experiment_started", False)),
                "previously_oor": bool(exp.get("previously_oor", False)),
                "current_ticks": {},
            }
        log.info("Loaded %d experiments from DB into memory", len(experiment_data))
    except Exception:
        log.exception("Failed to load experiments from DB")


# Decode / process

def decode(msg_value, record_name, experiments_col, records_col, kafka_msg, consumer):
    
    for msg in iter_avro_records(msg_value):
        exp_id = msg.get("experiment")
        if not exp_id:
            log.warning("Record without experiment id; skipping")
            continue

        # ensure in-memory entry exists
        if exp_id not in experiment_data:
            experiment_data[exp_id] = {
                "exp_id": exp_id,
                "temp_min_threshold": None,
                "temp_max_threshold": None,
                "nr_sensors": None,
                "researcher": None,
                "stabilizing": False,
                "stabilized": False,
                "experiment_started": False,
                "previously_oor": False,
                "current_ticks": {},
            }
        st = experiment_data[exp_id]

        if record_name == "ExperimentConfig":
            # load config and persist it
            st["temp_min_threshold"] = float(msg["temperature_range"]["lower_threshold"])
            st["temp_max_threshold"] = float(msg["temperature_range"]["upper_threshold"])
            st["nr_sensors"] = int(len(msg["sensors"]))
            st["researcher"] = msg["researcher"]
            # persist summary/config and flags to recover after restart
            experiments_col.update_one(
                {"exp_id": exp_id},
                {"$set": {
                    "temp_min_threshold": st["temp_min_threshold"],
                    "temp_max_threshold": st["temp_max_threshold"],
                    "nr_sensors": st["nr_sensors"],
                    "researcher": st["researcher"],
                    "stabilizing": st["stabilizing"],
                    "stabilized": st["stabilized"],
                    "experiment_started": st["experiment_started"],
                    "previously_oor": st["previously_oor"],
                }},
                upsert=True
            )
            log.info("[%s] ExperimentConfig loaded; sensors=%s researcher=%s", exp_id, st["nr_sensors"], st["researcher"])

        elif record_name == "stabilization_started":
            st["stabilizing"] = True
            experiments_col.update_one({"exp_id": exp_id}, {"$set": {"stabilizing": True}}, upsert=True)
            log.info("[%s] stabilization_started", exp_id)

        elif record_name == "experiment_started":
            st["experiment_started"] = True
            experiments_col.update_one({"exp_id": exp_id}, {"$set": {"experiment_started": True}}, upsert=True)
            log.info("[%s] experiment_started", exp_id)

        elif record_name == "sensor_temperature_measured":
            # guard for skip if no config known yet
            if st["nr_sensors"] in (None, 0):
                log.warning("[%s] measurement but no config; skipping", exp_id)
                continue

            mid = msg.get("measurement_id") or msg.get("measurement-id")
            if mid is None:
                log.warning("[%s] measurement missing measurement_id; skipping", exp_id)
                continue

            # aggregate per id
            ticks = st.setdefault("current_ticks", {})
            tick = ticks.setdefault(mid, {"sum": 0.0, "count": 0, "timestamp": msg.get("timestamp")})
            # one reading per sensor per measurement (sum)
            tick["sum"] += float(msg["temperature"])
            tick["count"] += 1

            # compute average and persist
            if tick["count"] >= st["nr_sensors"]:
                avg = tick["sum"] / st["nr_sensors"]
                ts = tick["timestamp"]
                mhash = msg.get("measurement_hash")

                if st["stabilizing"] and not st["stabilized"] and in_range(avg, st["temp_min_threshold"], st["temp_max_threshold"]):
                    payload = {
                        "notification_type": "Stabilized",
                        "researcher": st["researcher"],
                        "measurement_id": mid,
                        "experiment_id": exp_id,
                        "cipher_data": mhash,
                    }
                    if notify(payload):
                        st["stabilized"] = True
                        experiments_col.update_one({"exp_id": exp_id}, {"$set": {"stabilized": True}}, upsert=True)
                        log.info("[%s] STABILIZED mid=%s avg=%.3f", exp_id, mid, avg)
                    else:
                        # notify failed; raise to avoid committing offset so we retry
                        raise RuntimeError("Failed to send Stabilized notification")

                # Store measurement record, using unix indexes here, due to problems with indexing
                doc = {
                    "exp_id": exp_id,
                    "measurement_id": mid,
                    "timestamp": ts,
                    "avg_temperature": avg,
                }
                try:
                    records_col.insert_one(doc)
                    log.info("[%s] inserted record mid=%s avg=%.3f", exp_id, mid, avg)
                except DuplicateKeyError:
                    
                    log.info("[%s] record mid=%s already exists (duplicate insert ignored)", exp_id, mid)

                # OutOfRange - detect edges and notify on first exceedance
                if st["experiment_started"]:
                    if not in_range(avg, st["temp_min_threshold"], st["temp_max_threshold"]):
                        if not st["previously_oor"]:
                            payload = {
                                "notification_type": "OutOfRange",
                                "researcher": st["researcher"],
                                "measurement_id": mid,
                                "experiment_id": exp_id,
                                "cipher_data": mhash,
                            }
                            if notify(payload):
                                log.info("[%s] OUT_OF_RANGE mid=%s avg=%.3f", exp_id, mid, avg)
                            else:
                                # notification failed; raise to avoid committing offset
                                raise RuntimeError("Failed to send OutOfRange notification")
                        st["previously_oor"] = True
                        experiments_col.update_one({"exp_id": exp_id}, {"$set": {"previously_oor": True}}, upsert=True)
                    else:
                        if st["previously_oor"]:
                            # Clear persisted flag if returning in-range
                            experiments_col.update_one({"exp_id": exp_id}, {"$set": {"previously_oor": False}}, upsert=True)
                        st["previously_oor"] = False

                # if finished processing this tick, just remove aggregation state
                try:
                    del ticks[mid]
                except KeyError:
                    pass

        elif record_name == "experiment_terminated":
            experiments_col.update_one(
                {"exp_id": exp_id},
                {"$set": {"terminated_at": msg.get("timestamp")}},
                upsert=True
            )
            log.info("[%s] experiment_terminated; summary stored", exp_id)

    # If processing reached here without exceptions, commit Kafka offset for this message
    try:
        consumer.commit(message=kafka_msg, asynchronous=False)
        log.debug("Committed offsets for message")
    except Exception:
        log.exception("Failed to commit offsets")



def main():
    # Mongo
    conn = pymongo.MongoClient(MONGO_URI)
    log.info("ESTABLISHED CONNECTION TO DATABASE")
    db = conn[DB_NAME]
    experiments_col = db["experiments"]
    records_col = db["records"]

    ensure_indexes(db)
    load_experiments_from_db(experiments_col)

    # Kafka consumer
    c = Consumer(KAFKA_CONFIG)

    def rewind_on_assign(consumer, partitions):
        log.info("Partitions assigned: %s — rewinding to beginning", partitions)
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    c.subscribe([TOPIC], on_assign=rewind_on_assign)
    log.info("Subscribed to topic: %s", TOPIC)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            record_name = get_record_name(msg)
            if not record_name:
                log.warning("Missing 'record_name' header; committing and skipping message")
                try:
                    c.commit(message=msg, asynchronous=False)
                except Exception:
                    log.exception("Failed to commit missing-header message")
                continue

            try:
                decode(msg.value(), record_name, experiments_col, records_col, msg, c)
            except Exception as e:
                
                log.exception("Processing error: %s -- message will be retried", e)
                
    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        c.close()

if __name__ == "_main_":
    main()