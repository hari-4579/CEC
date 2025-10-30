import os
import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer
from dateutil import parser as date_parser

from models import TemperatureReading
from db import SessionLocal, insert_reading

logger = logging.getLogger("kafka_consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "cec-consumer-group")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "experiment")

class KafkaConsumerService:
    def __init__(self):
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    async def start(self):
        logger.info("Starting Kafka consumer for topic %s (bootstrap=%s)", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
        self._consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self._stopping.clear()
        self._task = asyncio.create_task(self._consume_loop())

    async def stop(self):
        logger.info("Stopping Kafka consumer")
        self._stopping.set()
        if self._task:
            await self._task
        if self._consumer:
            await self._consumer.stop()
        logger.info("Kafka consumer stopped")

    async def _consume_loop(self):
        try:
            while not self._stopping.is_set():
                try:
                    msg = await self._consumer.getone(timeout_ms=1000)
                except Exception as e:
                    # getone raises if timeout - continue; specific exceptions depend on aiokafka version
                    await asyncio.sleep(0.1)
                    continue

                try:
                    payload = msg.value.decode("utf-8")
                    data = json.loads(payload)
                    # Expecting data with experiment_id, sensor_id, timestamp, temperature
                    reading = TemperatureReading(
                        experiment_id=data["experiment_id"],
                        sensor_id=data["sensor_id"],
                        timestamp=date_parser.isoparse(data["timestamp"]),
                        temperature=float(data["temperature"]),
                    )
                    # Insert into DB (synchronously via SessionLocal)
                    db = SessionLocal()
                    try:
                        insert_reading(db, reading)
                    finally:
                        db.close()
                    logger.debug("Inserted reading from kafka: %s", reading)
                except Exception as ex:
                    logger.exception("Failed to process message: %s", ex)
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception:
            logger.exception("Unexpected error in consumer loop")
        finally:
            logger.info("Exiting consumer loop")