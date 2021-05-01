from .Base import Base

from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.types import *


class ObjectLoad(Base):
    __tablename__ = "jetavator_object_loads"
    type: str = Column(VARCHAR(124), primary_key=True)
    name: str = Column(VARCHAR(124), primary_key=True)
    load_dt: datetime = Column(
        TIMESTAMP, default="1900-01-01 00:00:00.000", primary_key=True)
