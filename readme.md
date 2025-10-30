# FastAPI Temperature Service (CEC Assignment)

This FastAPI app implements the REST API required by the DEMO-INSTRUCTIONS for the EC-labs CEC assignment, extended with:

- A Kafka consumer template that reads from the `experiment` topic and stores readings in a local SQLite database.
- Endpoints that query the SQLite database.
- Notification support to the notifications service using the token file.

Features
- Exposes REST endpoints on port 3003:
  - `GET /temperature?experiment-id=<id>&start-time=<iso>&end-time=<iso>`
  - `GET /temperature/out-of-range?experiment-id=<id>`
- A Kafka consumer that listens to the `experiment` topic and inserts messages into SQLite.
  - The consumer runs in the FastAPI process as a background task.
  - Messages are expected as JSON with fields: `experiment_id`, `sensor_id`, `timestamp` (ISO8601), `temperature`.
- Detects out-of-range temperature readings and notifies the notifications service using:
  `https://notifications-service-cec.ad.dlandau.nl/api/notify?token=<your-token>`
  - The token is loaded from a token file. See configuration below.

Quickstart (development)
1. Install Python 3.10+ and create a virtualenv
```markdown
   source .venv/bin/activate
```
2. Install dependencies
   pip install -r requirements.txt
3. Create a token file:
   - By default the app will look for TOKEN_FILE environment variable. If not set it will attempt `./credentials/token`.
   - Create the directory and a file with your token:
     mkdir -p credentials
     echo "your-demo-token" > credentials/token
4. (Optional) If you want to test the Kafka consumer locally, run a Kafka broker and point the KAFKA_BOOTSTRAP_SERVERS env var to it. The consumer will subscribe to the `experiment` topic.
5. Run:
   uvicorn main:app --host 0.0.0.0 --port 3003

Environment variables
- TOKEN_FILE: path to the token file (default: ./credentials/token)
- NOTIFICATIONS_HOST: default `https://notifications-service-cec.ad.dlandau.nl`
- TEMP_MIN / TEMP_MAX: bounds for out-of-range detection (defaults: -10, 60)
- DATABASE_URL: sqlite URL (default: sqlite:///./data/readings.db)
- KAFKA_BOOTSTRAP_SERVERS: comma-separated list (default: localhost:9092)
- KAFKA_GROUP_ID: consumer group id (default: cec-consumer-group)
- KAFKA_TOPIC: default `experiment`

Notes
- The notifications endpoint is contacted as:
  POST https://notifications-service-cec.ad.dlandau.nl/api/notify?token=<token>
  with a JSON body describing the event.
- The consumer template includes reconnection/backoff basics — adapt it for production.
- SQLite file will be created at ./data/readings.db by default.

What I included
- main.py: API + startup/shutdown wiring + notification logic
- db.py: SQLAlchemy engine, session, and CRUD helpers
- models.py: SQLAlchemy model + Pydantic schemas
- kafka_consumer.py: aiokafka-based consumer running as background task
- requirements.txt and a simple Dockerfile (optional)

Next steps you might want:
- Wire up authentication or TLS for Kafka.
- Add validation and deduplication policies for incoming messages.
- Replace SQLite with a cloud DB if required for scale.











- docker file:
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3003

ENV UVICORN_CMD="uvicorn main:app --host 0.0.0.0 --port 3003"

CMD [ "sh", "-c", "exec $UVICORN_CMD" ]



- VERSION of Read me

# FastAPI Temperature Service (CEC Assignment)

This FastAPI app implements the REST API required by the DEMO-INSTRUCTIONS for the EC-labs CEC assignment, extended with:

- A Kafka consumer template that reads from the `experiment` topic and stores readings in a local SQLite database.
- Endpoints that query the SQLite database.
- Notification support to the notifications service using the token file.

Features
- Exposes REST endpoints on port 3003:
  - `GET /temperature?experiment-id=<id>&start-time=<iso>&end-time=<iso>`
  - `GET /temperature/out-of-range?experiment-id=<id>`
- A Kafka consumer that listens to the `experiment` topic and inserts messages into SQLite.
  - The consumer runs in the FastAPI process as a background task.
  - Messages are expected as JSON with fields: `experiment_id`, `sensor_id`, `timestamp` (ISO8601), `temperature`.
- Detects out-of-range temperature readings and notifies the notifications service using:
  `https://notifications-service-cec.ad.dlandau.nl/api/notify?token=<your-token>`
  - The token is loaded from a token file. See configuration below.

Quickstart (development)
1. Install Python 3.10+ and create a virtualenv
```markdown
   source .venv/bin/activate
```
2. Install dependencies
   pip install -r requirements.txt
3. Create a token file:
   - By default the app will look for TOKEN_FILE environment variable. If not set it will attempt `./credentials/token`.
   - Create the directory and a file with your token:
     mkdir -p credentials
     echo "your-demo-token" > credentials/token
4. (Optional) If you want to test the Kafka consumer locally, run a Kafka broker and point the KAFKA_BOOTSTRAP_SERVERS env var to it. The consumer will subscribe to the `experiment` topic.
5. Run:
   uvicorn main:app --host 0.0.0.0 --port 3003

Environment variables
- TOKEN_FILE: path to the token file (default: ./credentials/token)
- NOTIFICATIONS_HOST: default `https://notifications-service-cec.ad.dlandau.nl`
- TEMP_MIN / TEMP_MAX: bounds for out-of-range detection (defaults: -10, 60)
- DATABASE_URL: sqlite URL (default: sqlite:///./data/readings.db)
- KAFKA_BOOTSTRAP_SERVERS: comma-separated list (default: localhost:9092)
- KAFKA_GROUP_ID: consumer group id (default: cec-consumer-group)
- KAFKA_TOPIC: default `experiment`

Notes
- The notifications endpoint is contacted as:
  POST https://notifications-service-cec.ad.dlandau.nl/api/notify?token=<token>
  with a JSON body describing the event.
- The consumer template includes reconnection/backoff basics — adapt it for production.
- SQLite file will be created at ./data/readings.db by default.

What I included
- main.py: API + startup/shutdown wiring + notification logic
- db.py: SQLAlchemy engine, session, and CRUD helpers
- models.py: SQLAlchemy model + Pydantic schemas
- kafka_consumer.py: aiokafka-based consumer running as background task
- requirements.txt and a simple Dockerfile (optional)

Next steps you might want:
- Wire up authentication or TLS for Kafka.
- Add validation and deduplication policies for incoming messages.
- Replace SQLite with a cloud DB if required for scale.