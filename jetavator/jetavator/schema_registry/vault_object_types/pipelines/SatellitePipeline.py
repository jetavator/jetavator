from typing import Dict, List

from abc import ABC, abstractmethod

import wysdom

from wysdom.mixins import RegistersSubclasses

from .PerformanceHints import PerformanceHints
from .SatellitePipelineDependency import SatellitePipelineDependency

from ... import VaultObject, Project


class SatellitePipeline(wysdom.UserObject, RegistersSubclasses, ABC):

    type: str = wysdom.UserProperty(str)
    performance_hints: PerformanceHints = wysdom.UserProperty(
        PerformanceHints, default={})
    _key_columns: Dict[str, str] = wysdom.UserProperty(
        wysdom.SchemaDict(str), name="key_columns", default={})

    @property
    def satellite(self) -> VaultObject:
        parent = wysdom.parent(self)
        if isinstance(parent, VaultObject):
            return parent
        else:
            raise TypeError('Parent is not a subclass of VaultObject')

    @property
    def project(self) -> Project:
        return self.satellite.project

    @property
    @abstractmethod
    def dependencies(self) -> List[SatellitePipelineDependency]:
        raise NotImplementedError

    def validate(self) -> None:
        for dep in self.dependencies:
            dep.validate()
