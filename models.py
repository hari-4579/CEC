from sqlalchemy import Column, Integer, String, Float, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

Base = declarative_base()

class TemperatureReadingDB(Base):
    __tablename__ = "temperature_readings"
    id = Column(Integer, primary_key=True, index=True)
    experiment_id = Column(String, index=True, nullable=False)
    sensor_id = Column(String, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    temperature = Column(Float, nullable=False)

    # Optional composite index to help de-duplication / lookups
    __table_args__ = (
        Index("ix_experiment_sensor_timestamp", "experiment_id", "sensor_id", "timestamp"),
    )

class TemperatureReading(BaseModel):
    experiment_id: str
    sensor_id: str
    timestamp: datetime
    temperature: float

    class Config:
        orm_mode = True

class OutOfRangeSummary(BaseModel):
    experiment_id: str
    total_readings: int
    out_of_range_count: int
    sensors: list[str]
    min_allowed: float
    max_allowed: float
    notified: Optional[bool] = False
    notification_response: Optional[dict] = None