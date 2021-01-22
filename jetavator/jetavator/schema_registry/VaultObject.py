from __future__ import annotations

from typing import Any, Dict, List
from abc import ABC, abstractmethod

from datetime import datetime
from collections import namedtuple

from lazy_property import LazyProperty

from .sqlalchemy_tables import ObjectDefinition

import wysdom

from jetavator.services import ComputeServiceABC

from .ProjectABC import ProjectABC

VaultObjectKey = namedtuple('VaultObjectKey', ['type', 'name'])
HubKeyColumn = namedtuple('HubKeyColumn', ['name', 'source'])


class VaultObject(wysdom.UserObject, wysdom.RegistersSubclasses, ABC):

    name: str = wysdom.UserProperty(str)
    type: str = wysdom.UserProperty(str)

    optional_yaml_properties = []

    def __init__(
        self,
        project: ProjectABC,
        sqlalchemy_object: ObjectDefinition
    ) -> None:
        self.project = project
        self._sqlalchemy_object = sqlalchemy_object
        super().__init__(self.definition)

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}({self.name})'

    @classmethod
    def subclass_instance(
        cls,
        project: ProjectABC,
        definition: ObjectDefinition
    ) -> VaultObject:
        return cls.registered_subclass_instance(
            definition.type,
            project,
            definition
        )

    @LazyProperty
    def key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def definition(self) -> Dict[str, Any]:
        return self._sqlalchemy_object.definition

    def export_sqlalchemy_object(self) -> ObjectDefinition:
        if self._sqlalchemy_object.version != str(self.project.version):
            raise ValueError(
                "ObjectDefinition version must match project version "
                "and cannot be updated."
            )
        self._sqlalchemy_object.deploy_dt = str(datetime.now())
        return self._sqlalchemy_object

    @abstractmethod
    def validate(self) -> None:
        pass

    @property
    def compute_service(self) -> ComputeServiceABC:
        return self.project.compute_service

    @property
    def full_name(self) -> str:
        return f'{self.type}_{self.name}'

    @property
    def checksum(self) -> str:
        return str(self._sqlalchemy_object.checksum)

    @property
    def dependent_satellites(self) -> List[VaultObject]:
        return [
            satellite
            for satellite in self.project.satellites.values()
            if any(
                dependency.type == self.type
                and dependency.name == self.name
                for dependency in satellite.pipeline.dependencies
            )
        ]


