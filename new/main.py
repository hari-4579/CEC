import os
import logging
from typing import Optional, List, Set
from datetime import datetime

import httpx
from fastapi import FastAPI, Query, HTTPException, Depends
from dateutil import parser as date_parser
from sqlalchemy.orm import Session

from models import TemperatureReading, OutOfRangeSummary
from db import init_db, get_db, query_readings
from kafka_consumer import KafkaConsumerService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cec-temp-service")

app = FastAPI(title="CEC Temperature Service with Kafka & SQLite")

# Config
TOKEN_FILE = os.environ.get("TOKEN_FILE", "./credentials/token")
NOTIFICATIONS_HOST = os.environ.get("NOTIFICATIONS_HOST", "https://notifications-service-cec.ad.dlandau.nl")
NOTIFICATIONS_PATH = "/api/notify"
TEMP_MIN = float(os.environ.get("TEMP_MIN", "-10"))
TEMP_MAX = float(os.environ.get("TEMP_MAX", "60"))

kafka_service = KafkaConsumerService()

def load_token() -> Optional[str]:
    try:
        with open(TOKEN_FILE, "r") as fh:
            token = fh.read().strip()
            if not token:
                logger.warning("Token file found but empty: %s", TOKEN_FILE)
                return None
            return token
    except FileNotFoundError:
        logger.warning("Token file not found at %s", TOKEN_FILE)
        return None
    except Exception as e:
        logger.error("Error reading token file: %s", e)
        return None

async def notify_out_of_range(summary: OutOfRangeSummary, token: str) -> dict:
    url = f"{NOTIFICATIONS_HOST}{NOTIFICATIONS_PATH}"
    params = {"token": token}
    payload = {
        "type": "out_of_range",
        "experiment_id": summary.experiment_id,
        "out_of_range_count": summary.out_of_range_count,
        "sensors": summary.sensors,
        "min_allowed": summary.min_allowed,
        "max_allowed": summary.max_allowed,
        "total_readings": summary.total_readings,
    }
    logger.info("Sending notification to %s with token=%s", url, "****" if token else None)
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(url, params=params, json=payload)
            r.raise_for_status()
            logger.info("Notification sent successfully, status=%s", r.status_code)
            return {"status_code": r.status_code, "body": r.text}
        except httpx.HTTPError as exc:
            logger.error("Failed to send notification: %s", exc)
            return {"error": str(exc)}

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing DB")
    init_db()
    # Start Kafka consumer (it will try to connect to Kafka bootstrap servers)
    try:
        await kafka_service.start()
    except Exception:
        logger.exception("Kafka consumer failed to start (this is a template; you can run without Kafka).")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        await kafka_service.stop()
    except Exception:
        logger.exception("Error stopping kafka consumer")

@app.get("/temperature", response_model=List[TemperatureReading])
def get_temperature(
    experiment_id: str = Query(..., alias="experiment-id"),
    start_time: Optional[str] = Query(None, alias="start-time"),
    end_time: Optional[str] = Query(None, alias="end-time"),
    db: Session = Depends(get_db),
):
    """
    Get temperature readings for an experiment between start-time and end-time.
    - start-time and end-time should be ISO8601 strings (e.g. 2025-10-28T12:00:00Z)
    """
    try:
        start_dt = date_parser.isoparse(start_time) if start_time else None
        end_dt = date_parser.isoparse(end_time) if end_time else None
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    rows = query_readings(db, experiment_id, start_dt, end_dt)
    return [TemperatureReading.from_orm(r) for r in rows]

@app.get("/temperature/out-of-range", response_model=OutOfRangeSummary)
async def get_out_of_range(experiment_id: str = Query(..., alias="experiment-id"), db: Session = Depends(get_db)):
    """
    Check for out-of-range temperatures for an experiment.
    If out-of-range readings are found, attempt to notify the notifications-service.
    """
    rows = query_readings(db, experiment_id, None, None)
    if not rows:
        raise HTTPException(status_code=404, detail="No readings found for this experiment")

    out_of_range = [r for r in rows if (r.temperature < TEMP_MIN or r.temperature > TEMP_MAX)]
    sensors: Set[str] = set(r.sensor_id for r in out_of_range)

    summary = OutOfRangeSummary(
        experiment_id=experiment_id,
        total_readings=len(rows),
        out_of_range_count=len(out_of_range),
        sensors=sorted(list(sensors)),
        min_allowed=TEMP_MIN,
        max_allowed=TEMP_MAX,
        notified=False,
        notification_response=None,
    )

    if summary.out_of_range_count > 0:
        token = load_token()
        if not token:
            logger.warning("Cannot notify: token not available (TOKEN_FILE=%s)", TOKEN_FILE)
        else:
            resp = await notify_out_of_range(summary, token)
            summary.notified = True if "status_code" in resp and resp.get("status_code", 0) < 400 else False
            summary.notification_response = resp

    return summary