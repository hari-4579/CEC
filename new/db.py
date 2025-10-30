import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional
from datetime import datetime

from models import Base, TemperatureReadingDB, TemperatureReading

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./data/readings.db")

# For SQLite we set check_same_thread=False to allow usage from multiple threads
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    # Create directories if needed (for sqlite file)
    if DATABASE_URL.startswith("sqlite:///"):
        path = DATABASE_URL.replace("sqlite:///", "")
        dirpath = os.path.dirname(path)
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
    Base.metadata.create_all(bind=engine)

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def insert_reading(db: Session, reading: TemperatureReading) -> Optional[TemperatureReadingDB]:
    """Insert a reading into the DB. This does not insert duplicates if exact experiment/sensor/timestamp exists."""
    try:
        # Check duplicate
        existing = db.query(TemperatureReadingDB).filter(
            TemperatureReadingDB.experiment_id == reading.experiment_id,
            TemperatureReadingDB.sensor_id == reading.sensor_id,
            TemperatureReadingDB.timestamp == reading.timestamp
        ).first()
        if existing:
            return existing

        db_obj = TemperatureReadingDB(
            experiment_id=reading.experiment_id,
            sensor_id=reading.sensor_id,
            timestamp=reading.timestamp,
            temperature=reading.temperature
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj
    except SQLAlchemyError:
        db.rollback()
        raise

def query_readings(db: Session, experiment_id: str, start_time: Optional[datetime], end_time: Optional[datetime]) -> List[TemperatureReadingDB]:
    q = db.query(TemperatureReadingDB).filter(TemperatureReadingDB.experiment_id == experiment_id)
    if start_time:
        q = q.filter(TemperatureReadingDB.timestamp >= start_time)
    if end_time:
        q = q.filter(TemperatureReadingDB.timestamp <= end_time)
    return q.order_by(TemperatureReadingDB.timestamp.asc()).all()