import os
import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict
from io import BytesIO

from confluent_kafka import Consumer, KafkaError
import fastavro
import httpx

from db import SessionLocal, insert_reading
from models import TemperatureReading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1.dlandau.nl:19092,kafka2.dlandau.nl:29092,kafka3.dlandau.nl:39092"
)
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "group3_123456")
KAFKA_TOPIC = os.getenv("TEAM_TOPIC", "experiment")

# SSL Configuration
SSL_CA_LOCATION = os.getenv("SSL_CA_LOCATION", "auth/ca.crt")
SSL_KEYSTORE_LOCATION = os.getenv("SSL_KEYSTORE_LOCATION", "auth/kafka.keystore.pkcs12")
SSL_KEYSTORE_PASSWORD = os.getenv("SSL_KEYSTORE_PASSWORD", "cc2023")

# Notifications Service
NOTIFICATIONS_HOST = os.getenv("NOTIFICATIONS_HOST", "https://notifications-service-cec.ad.dlandau.nl")
TOKEN_FILE = os.getenv("TOKEN_FILE", "./credentials/token")


class ExperimentState:
    """Tracks the state of an experiment"""
    def __init__(self, experiment_id: str, researcher: str, sensors: List[str], 
                 min_temp: float, max_temp: float):
        self.experiment_id = experiment_id
        self.researcher = researcher
        self.sensors = set(sensors)
        self.min_temp = min_temp
        self.max_temp = max_temp
        self.phase = "configured"  # configured, stabilizing, running, terminated
        self.stabilized = False
        self.last_notification_time = {}  # measurement_id -> timestamp for deduplication
        self.sensor_readings = defaultdict(dict)  # timestamp -> {sensor_id -> temp}


class KafkaConsumerService:
    def __init__(self):
        self.consumer = None
        self.running = False
        self.experiments: Dict[str, ExperimentState] = {}
        self.consumer_task = None
        
    async def start(self):
        """Initialize Kafka consumer and start consuming messages"""
        try:
            logger.info("Initializing Kafka Consumer Service")
            
            # Kafka Consumer Configuration
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': KAFKA_GROUP_ID,
                'auto.offset.reset': 'latest',
                'security.protocol': 'SSL',
                'ssl.ca.location': SSL_CA_LOCATION,
                'ssl.keystore.location': SSL_KEYSTORE_LOCATION,
                'ssl.keystore.password': SSL_KEYSTORE_PASSWORD,
                'enable.auto.commit': True,
                'ssl.endpoint.identification.algorithm': 'none',
                'session.timeout.ms': 30000,
            }
            
            self.consumer = Consumer(conf)
            self.consumer.subscribe([KAFKA_TOPIC])
            
            self.running = True
            logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC}")
            
            # Start consumer loop in background
            self.consumer_task = asyncio.create_task(self._consume_messages())
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            raise
    
    async def stop(self):
        """Gracefully stop the Kafka consumer"""
        try:
            logger.info("Stopping Kafka Consumer Service")
            self.running = False
            
            if self.consumer_task:
                await asyncio.wait_for(self.consumer_task, timeout=5.0)
            
            if self.consumer:
                self.consumer.close()
            
            logger.info("Kafka Consumer Service stopped")
        except asyncio.TimeoutError:
            logger.warning("Kafka consumer task did not stop gracefully")
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {e}")
    
    async def _consume_messages(self):
        """Main message consumption loop"""
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # Deserialize message
                try:
                    # Get record_name from headers (list of tuples)
                    record_name = None
                    if msg.headers():
                        for header_name, header_value in msg.headers():
                            if header_name == "record_name":
                                record_name = header_value.decode() if isinstance(header_value, bytes) else header_value
                                break
                    
                    # Deserialize plain Avro message (not Confluent format)
                    record_data = fastavro.reader(BytesIO(msg.value())).__next__()
                    
                    logger.debug(f"Received event: {record_name}, data: {record_data}")
                    
                    # Route to appropriate handler based on record type
                    # Handle BOTH possible naming conventions
                    if record_name in ["ExperimentConfig", "experiment_configured"]:
                        await self._handle_experiment_config(record_data)
                    elif record_name == "stabilization_started":
                        await self._handle_stabilization_started(record_data)
                    elif record_name == "experiment_started":
                        await self._handle_experiment_started(record_data)
                    elif record_name == "sensor_temperature_measured":
                        await self._handle_sensor_temperature_measured(record_data)
                    elif record_name == "experiment_terminated":
                        await self._handle_experiment_terminated(record_data)
                    else:
                        logger.warning(f"Unknown record type: {record_name}")
                
                except Exception as e:
                    logger.error(f"Failed to deserialize message: {e}")
            
            except Exception as e:
                logger.error(f"Unexpected error in consume loop: {e}")
                await asyncio.sleep(1)
    
    async def _handle_experiment_config(self, data: dict):
        """Handle ExperimentConfig event"""
        experiment_id = data.get("experiment")
        researcher = data.get("researcher")
        sensors = data.get("sensors", [])
        temp_range = data.get("temperature_range", {})
        min_temp = temp_range.get("lower_threshold")
        max_temp = temp_range.get("upper_threshold")
        
        if min_temp is None or max_temp is None:
            logger.error(f"Missing temperature range for experiment {experiment_id}")
            return
        
        logger.info(f"✅ Experiment config received: {experiment_id} (range: {min_temp}°C-{max_temp}°C, {len(sensors)} sensors)")
        
        self.experiments[experiment_id] = ExperimentState(
            experiment_id=experiment_id,
            researcher=researcher,
            sensors=sensors,
            min_temp=min_temp,
            max_temp=max_temp
        )
    
    async def _handle_stabilization_started(self, data: dict):
        """Handle stabilization_started event"""
        experiment_id = data.get("experiment")
        
        logger.info(f"🌡️  Stabilization started for experiment: {experiment_id}")
        
        if experiment_id in self.experiments:
            self.experiments[experiment_id].phase = "stabilizing"
        else:
            logger.warning(f"Stabilization started but experiment {experiment_id} not configured yet")
    
    async def _handle_experiment_started(self, data: dict):
        """Handle experiment_started event"""
        experiment_id = data.get("experiment")
        
        logger.info(f"▶️  Experiment started: {experiment_id}")
        
        if experiment_id in self.experiments:
            self.experiments[experiment_id].phase = "running"
        else:
            logger.warning(f"Experiment started but experiment {experiment_id} not configured yet")
    
    async def _handle_sensor_temperature_measured(self, data: dict):
        """Handle sensor_temperature_measured event"""
        experiment_id = data.get("experiment")
        sensor_id = data.get("sensor")
        measurement_id = data.get("measurement_id")
        timestamp = data.get("timestamp")
        temperature = data.get("temperature")
        measurement_hash = data.get("measurement_hash")
        
        if experiment_id not in self.experiments:
            logger.warning(f"Received measurement for unknown experiment: {experiment_id}")
            return
        
        exp_state = self.experiments[experiment_id]
        
        # Store sensor reading for this timestamp
        if timestamp not in exp_state.sensor_readings:
            exp_state.sensor_readings[timestamp] = {}
        exp_state.sensor_readings[timestamp][sensor_id] = temperature
        
        # Check if we have readings from ALL sensors for this timestamp
        if len(exp_state.sensor_readings[timestamp]) == len(exp_state.sensors):
            # Calculate average temperature across all sensors
            avg_temp = sum(exp_state.sensor_readings[timestamp].values()) / len(exp_state.sensors)
            
            logger.debug(f"📊 Avg temp for {experiment_id}: {avg_temp:.2f}°C (range: {exp_state.min_temp}-{exp_state.max_temp})")
            
            # Handle based on current phase
            if exp_state.phase == "stabilizing":
                await self._handle_stabilization_phase(
                    exp_state, avg_temp, measurement_id, measurement_hash, timestamp
                )
            elif exp_state.phase == "running":
                await self._handle_running_phase(
                    exp_state, avg_temp, measurement_id, measurement_hash, timestamp
                )
            
            # Clean up old readings
            del exp_state.sensor_readings[timestamp]
    
    async def _handle_stabilization_phase(self, exp_state: ExperimentState, avg_temp: float,
                                         measurement_id: str, measurement_hash: str, timestamp: float):
        """Handle temperature reading during stabilization phase"""
        # Check if temperature has stabilized (entered acceptable range)
        if not exp_state.stabilized and exp_state.min_temp <= avg_temp <= exp_state.max_temp:
            logger.info(f"✨ Temperature STABILIZED for experiment: {exp_state.experiment_id} at {avg_temp:.2f}°C")
            exp_state.stabilized = True
            
            # Notify about stabilization
            await self._notify_stabilization(
                exp_state, measurement_id, measurement_hash
            )
    
    async def _handle_running_phase(self, exp_state: ExperimentState, avg_temp: float,
                                   measurement_id: str, measurement_hash: str, timestamp: float):
        """Handle temperature reading during running phase"""
        # Insert reading into database
        reading = TemperatureReading(
            experiment_id=exp_state.experiment_id,
            sensor_id="aggregated",
            timestamp=datetime.utcfromtimestamp(timestamp),
            temperature=avg_temp
        )
        
        try:
            db = SessionLocal()
            insert_reading(db, reading)
            logger.debug(f"💾 Inserted reading: {avg_temp:.2f}°C")
        except Exception as e:
            logger.error(f"Failed to insert reading: {e}")
        finally:
            db.close()
        
        # Check if out of range
        if avg_temp < exp_state.min_temp or avg_temp > exp_state.max_temp:
            # Only notify once per unique measurement
            if measurement_id not in exp_state.last_notification_time:
                logger.warning(f"🔴 OUT OF RANGE: {avg_temp:.2f}°C (allowed: {exp_state.min_temp}-{exp_state.max_temp})")
                exp_state.last_notification_time[measurement_id] = timestamp
                
                await self._notify_out_of_range(
                    exp_state, measurement_id, measurement_hash
                )
    
    async def _handle_experiment_terminated(self, data: dict):
        """Handle experiment_terminated event"""
        experiment_id = data.get("experiment")
        
        logger.info(f"⏹️  Experiment terminated: {experiment_id}")
        
        if experiment_id in self.experiments:
            self.experiments[experiment_id].phase = "terminated"
    
    async def _notify_stabilization(self, exp_state: ExperimentState, 
                                   measurement_id: str, measurement_hash: str):
        """Send notification for stabilization event"""
        token = self._load_token()
        if not token:
            logger.warning("Cannot notify: token not available")
            return
        
        try:
            url = f"{NOTIFICATIONS_HOST}/api/notify"
            params = {"token": token}
            payload = {
                "notification_type": "Stabilized",
                "researcher": exp_state.researcher,
                "experiment_id": exp_state.experiment_id,
                "measurement_id": measurement_id,
                "cipher_data": measurement_hash
            }

            # Validate payload before sending
            required_fields = ["notification_type", "researcher", "measurement_id", "experiment_id", "cipher_data"]
            if not all(payload.get(f) for f in required_fields):
                logger.warning("Skipping invalid notification payload: %s", payload)
                return  # should just send a 200 and skip if anything is missing
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, params=params, json=payload)
                response.raise_for_status()
                logger.info(f"✅ Stabilization notification sent: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Failed to send stabilization notification: {e}")
    
    async def _notify_out_of_range(self, exp_state: ExperimentState,
                                  measurement_id: str, measurement_hash: str):
        """Send notification for out-of-range event"""
        token = self._load_token()
        if not token:
            logger.warning("Cannot notify: token not available")
            return
        
        try:
            url = f"{NOTIFICATIONS_HOST}/api/notify"
            params = {"token": token}
            payload = {
                "notification_type": "OutOfRange",
                "researcher": exp_state.researcher,
                "experiment_id": exp_state.experiment_id,
                "measurement_id": measurement_id,
                "cipher_data": measurement_hash
            }

            # Validate payload before sending
            required_fields = ["notification_type", "researcher", "measurement_id", "experiment_id", "cipher_data"]
            if not all(payload.get(f) for f in required_fields):
                logger.warning("Skipping invalid notification payload: %s", payload)
                return  # should just send a 200 and skip if anything is missing

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, params=params, json=payload)
                response.raise_for_status()
                logger.info(f"✅ Out-of-range notification sent: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Failed to send out-of-range notification: {e}")
    
    def _load_token(self) -> Optional[str]:
        """Load JWT token from file"""
        try:
            with open(TOKEN_FILE, "r") as fh:
                token = fh.read().strip()
                return token if token else None
        except FileNotFoundError:
            logger.warning(f"Token file not found: {TOKEN_FILE}")
            return None
        except Exception as e:
            logger.error(f"Error reading token file: {e}")
            return None
