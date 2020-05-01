from .Base import Base

from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.orm import relationship
# noinspection PyProtectedMember
from sqlalchemy.orm.session import object_session
from sqlalchemy.types import *


class Deployment(Base):

    __tablename__ = "jetavator_deployments"

    name: str = Column(VARCHAR(124))
    version: str = Column(VARCHAR(124), primary_key=True)
    jetavator_version: str = Column(VARCHAR(124))
    checksum: str = Column(CHAR(40))
    deploy_dt: datetime = Column(TIMESTAMP, default="1900-01-01 00:00:00.000")

    object_definitions: relationship = relationship(
        "ObjectDefinition", back_populates="deployment")

    def __repr__(self) -> str:
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
    def is_latest(self) -> bool:
        cls = type(self)
        latest_deployment = object_session(self).query(cls).order_by(
            cls.deploy_dt.desc()).first()
        return self is latest_deployment

