from typing import Dict, List

from abc import ABC, abstractmethod

import jsdom

from jsdom.mixins import RegistersSubclasses

from .PerformanceHints import PerformanceHints
from .SatellitePipelineDependency import SatellitePipelineDependency

from ... import VaultObject, Project


class SatellitePipeline(jsdom.Object, RegistersSubclasses, ABC):

    type: str = jsdom.Property(str)
    performance_hints: PerformanceHints = jsdom.Property(
        PerformanceHints, default={})
    _key_columns: Dict[str, str] = jsdom.Property(
        jsdom.Dict(str), name="key_columns", default={})

    @property
    def satellite(self) -> VaultObject:
        parent = jsdom.parent(self)
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
