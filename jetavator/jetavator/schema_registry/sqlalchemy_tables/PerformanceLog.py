from .Base import Base

from sqlalchemy import Column, Table
from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT


class PerformanceLog(Base):

    __tablename__ = "jetavator_performance_log"

    type = Column("type", VARCHAR(124), nullable=True)
    name = Column("name", VARCHAR(124), nullable=True)
    stage = Column("stage", VARCHAR(124), nullable=True)
    start_timestamp = Column("start_timestamp", TIMESTAMP, nullable=True)
    end_timestamp = Column("end_timestamp", TIMESTAMP, nullable=True)
    rows = Column("rows", BIGINT, nullable=True)

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
