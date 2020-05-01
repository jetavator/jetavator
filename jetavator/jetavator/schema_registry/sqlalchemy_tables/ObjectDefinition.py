from __future__ import annotations

from typing import Dict, Any

from .Base import Base
from .Deployment import Deployment

from jetavator.utils import dict_checksum

from datetime import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import *

from ast import literal_eval


class ObjectDefinition(Base):

    @classmethod
    def from_dict(
            cls,
            deployment: Deployment,
            definition_dict: Dict[str, Any]
    ) -> ObjectDefinition:

        if not isinstance(definition_dict, dict):
            raise TypeError("definition_dict must be of type dict")

        for key in ["name", "type"]:
            if key not in definition_dict:
                raise ValueError(
                    f"Key '{key}' must be specified in definition.")

        return cls(
            deployment,
            definition_dict,
            _type=definition_dict["type"],
            _name=definition_dict["name"],
            _version=deployment.version,
            _definition=str(definition_dict),
            _checksum=dict_checksum(definition_dict)
        )

    def __init__(
            self,
            deployment: Deployment,
            definition_dict: Dict[str, Any],
            *args: Any,
            **kwargs: Any
    ) -> None:

        self._deployment = deployment
        self._definition_dict = definition_dict

        super().__init__(*args, **kwargs)

        self._verify_checksum()

    def _verify_checksum(self):
        generated_checksum = dict_checksum(self.definition)
        if self._checksum != generated_checksum:
            raise RuntimeError(
                f"""
                Checksum error on object load.
                Stored checksum: {self._checksum}
                Generated checksum: {generated_checksum}
                Definition:
                {self.definition}
                """
            )

    __tablename__ = "jetavator_object_definitions"

    _definition_dict = None

    _type: str = Column("type", VARCHAR(124), primary_key=True)
    _name: str = Column("name", VARCHAR(124), primary_key=True)
    _version: str = Column(
        "version",
        VARCHAR(124),
        ForeignKey('jetavator_deployments.version'),
        primary_key=True)

    _definition: str = Column("definition", VARCHAR(50))
    _checksum: str = Column("checksum", CHAR(40))

    deploy_dt: datetime = Column(TIMESTAMP)
    deleted_ind: str = Column(VARCHAR(1), default=0)

    @property
    def type(self) -> str:
        return self._type

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def definition(self) -> Dict[str, Any]:
        if not self._definition_dict:
            self._definition_dict = literal_eval(self._definition)
            if not isinstance(self._definition_dict, dict):
                raise TypeError(
                    "definition does not evaluate to a dict: "
                    f"{self._definition}"
                )
        return self._definition_dict

    @property
    def checksum(self) -> str:
        return self._checksum

    def __repr__(self) -> str:
        return (
            "<ObjectDefinition("
            f"type='{self._type}', "
            f"name='{self._name}', "
            f"version='{self._version}', "
            f"definition='{self._definition}', "
            f"checksum='{self._checksum}', "
            f"deploy_dt='{self.deploy_dt}'"
            f"deleted_ind='{self.deleted_ind}'"
            ")>"
        )

    deployment: relationship = relationship(
        "Deployment", back_populates="object_definitions")
