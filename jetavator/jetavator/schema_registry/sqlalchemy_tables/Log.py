from .Base import Base

from sqlalchemy import Column, Table
from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT


class Log(Base):

    __tablename__ = "jetavator_log"

    schema = Column("schema", VARCHAR(124), nullable=True)
    procedure = Column("procedure", VARCHAR(124), nullable=True)
    message = Column("message", VARCHAR(8000), nullable=True)
    log_time = Column("log_time", TIMESTAMP, nullable=True)

    __table__ = Table(
        __tablename__,
        Base.metadata,
        schema,
        procedure,
        message,
        log_time,
        extend_existing=True
    )

    __mapper_args__ = {
        "primary_key": [
            __table__.c.schema,
            __table__.c.procedure,
            __table__.c.log_time
        ]
    }
