from .Base import Base

from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT


class Deployment(Base):

    __tablename__ = "jetavator_deployments"

    name = Column(VARCHAR(124))
    version = Column(VARCHAR(124), primary_key=True)
    jetavator_version = Column(VARCHAR(124))
    checksum = Column(CHAR(40))
    deploy_dt = Column(TIMESTAMP, default="1900-01-01 00:00:00.000")

    object_definitions = relationship(
        "ObjectDefinition", back_populates="deployment")

    def __repr__(self):
        return (
            "<Deployment("
            f"name='{self.name}', "
            f"version='{self.version}', "
            f"jetavator_version='{self.jetavator_version}', "
            f"checksum='{self.checksum}', "
            f"deploy_dt='{self.deploy_dt}'"
            ")>"
        )

    @property
    def is_latest(self):
        cls = type(self)
        latest_deployment = object_session(self).query(cls).order_by(
            cls.deploy_dt.desc()).first()
        return (self is latest_deployment)
