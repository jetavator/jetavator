from .Base import Base

from datetime import datetime

from sqlalchemy import Column, Table
from sqlalchemy.types import *


class PerformanceLog(Base):

    __tablename__ = "jetavator_performance_log"

    type: str = Column("type", VARCHAR(124), nullable=True)
    name: str = Column("name", VARCHAR(124), nullable=True)
    stage: str = Column("stage", VARCHAR(124), nullable=True)
    start_timestamp: datetime = Column("start_timestamp", TIMESTAMP, nullable=True)
    end_timestamp: datetime = Column("end_timestamp", TIMESTAMP, nullable=True)
    rows: int = Column("rows", BIGINT, nullable=True)

    __table__ = Table(
        __tablename__,
        Base.metadata,
        type,
        name,
        stage,
        start_timestamp,
        end_timestamp,
        rows,
        extend_existing=True
    )

    __mapper_args__ = {
        "primary_key": [
            __table__.c.type,
            __table__.c.name,
            __table__.c.stage,
            __table__.c.start_timestamp
        ]
    }
