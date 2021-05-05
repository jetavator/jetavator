from __future__ import annotations

from typing import Any, Dict, List, TypeVar, Generic, Iterator, Tuple
from collections.abc import Mapping
from abc import ABC, abstractmethod

import semver
from datetime import datetime
from collections import namedtuple

from lazy_property import LazyProperty

from .sqlalchemy_tables import ObjectDefinition

import wysdom

VaultObjectKey = namedtuple('VaultObjectKey', ['type', 'name'])
HubKeyColumn = namedtuple('HubKeyColumn', ['name', 'source'])


class VaultObject(wysdom.UserObject, wysdom.RegistersSubclasses, ABC):

    name: str = wysdom.UserProperty(str)
    type: str = wysdom.UserProperty(str)

    optional_yaml_properties = []

    def __init__(
        self,
        owner: VaultObjectOwner,
        sqlalchemy_object: ObjectDefinition
    ) -> None:
        self._owner = owner
        self._sqlalchemy_object = sqlalchemy_object
        super().__init__(self.definition)

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}({self.name})'

    @classmethod
    def subclass_instance(
        cls,
        owner: VaultObjectOwner,
        definition: ObjectDefinition
    ) -> VaultObject:
        return cls.registered_subclass_instance(
            definition.type,
            owner,
            definition
        )

    @property
    def owner(self) -> VaultObjectOwner:
        return self._owner

    @LazyProperty
    def key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def definition(self) -> Dict[str, Any]:
        return self._sqlalchemy_object.definition

    def export_sqlalchemy_object(self) -> ObjectDefinition:
        if self._sqlalchemy_object.version != str(self.owner.version):
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
    def full_name(self) -> str:
        return f'{self.type}_{self.name}'

    @property
    def checksum(self) -> str:
        return str(self._sqlalchemy_object.checksum)

    def depends_on(self, other_object: VaultObject) -> bool:
        return False

    @property
    def dependent_satellites(self) -> List[VaultObject]:
        return [
            satellite
            for satellite in self.owner.satellites.values()
            if satellite.depends_on(self)
        ]


VaultObjectType = TypeVar('VaultObjectType', covariant=True, bound=VaultObject)


class VaultObjectOwner(Mapping, Generic[VaultObjectType], ABC):

    @abstractmethod
    def __getitem__(
            self,
            key: Tuple[str, str]
    ) -> VaultObject:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[VaultObjectType]:
        pass

    @property
    @abstractmethod
    def version(self) -> semver.VersionInfo:
        pass

    @property
    @abstractmethod
    def hubs(self) -> Dict[str, VaultObject]:
        pass

    @property
    @abstractmethod
    def links(self) -> Dict[str, VaultObject]:
        pass

    @property
    @abstractmethod
    def satellite_owners(self) -> Dict[str, VaultObject]:
        pass

    @property
    @abstractmethod
    def satellites(self) -> Dict[str, VaultObject]:
        pass

    @property
    @abstractmethod
    def sources(self) -> Dict[str, VaultObject]:
        pass


