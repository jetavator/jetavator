from .Base import Base

from sqlalchemy import Column
from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT


class ObjectLoad(Base):
    __tablename__ = "jetavator_object_loads"
    type = Column(VARCHAR(124), primary_key=True)
    name = Column(VARCHAR(124), primary_key=True)
    load_dt = Column(
        TIMESTAMP, default="1900-01-01 00:00:00.000", primary_key=True)
