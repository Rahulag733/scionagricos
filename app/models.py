"""
ScionAgricos - SQLAlchemy Models
Tables stored in AWS RDS PostgreSQL:
  - transit_time  : import/export route transit days
  - seasonality   : product × origin × month availability
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime, CheckConstraint
from app.db import Base


class TransitTime(Base):
    __tablename__ = "transit_time"

    id = Column(Integer, primary_key=True, autoincrement=True)
    origin = Column(String(100), nullable=False)
    destination = Column(String(100), nullable=False)
    transit_type = Column(String(20), nullable=False)   # 'import' or 'export'
    transit_days_min = Column(Integer, nullable=True)
    transit_days_avg = Column(Integer, nullable=False)
    transit_days_max = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        CheckConstraint("transit_type IN ('import', 'export')", name="chk_transit_type"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "origin": self.origin,
            "destination": self.destination,
            "transit_type": self.transit_type,
            "transit_days_min": self.transit_days_min,
            "transit_days_avg": self.transit_days_avg,
            "transit_days_max": self.transit_days_max,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Seasonality(Base):
    __tablename__ = "seasonality"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product = Column(String(100), nullable=False)
    origin = Column(String(100), nullable=False)
    month = Column(Integer, nullable=False)             # 1–12
    availability = Column(String(20), nullable=True)    # 'high', 'medium', 'low'
    is_peak_season = Column(Boolean, default=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        CheckConstraint("month BETWEEN 1 AND 12", name="chk_month_range"),
        CheckConstraint(
            "availability IN ('high', 'medium', 'low') OR availability IS NULL",
            name="chk_availability"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "product": self.product,
            "origin": self.origin,
            "month": self.month,
            "availability": self.availability,
            "is_peak_season": self.is_peak_season,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
