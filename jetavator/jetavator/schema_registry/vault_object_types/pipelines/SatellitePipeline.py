from typing import Dict, List

from abc import ABC, abstractmethod

from jetavator import json_schema_objects as jso

from jetavator.mixins import RegistersSubclasses

from .PerformanceHints import PerformanceHints
from .SatellitePipelineDependency import SatellitePipelineDependency

from ... import VaultObject, Project


class SatellitePipeline(jso.Object, RegistersSubclasses, ABC):

    type: str = jso.Property(jso.String)
    performance_hints: PerformanceHints = jso.Property(
        PerformanceHints, default=PerformanceHints({}))
    _key_columns: Dict[str, str] = jso.Property(
        jso.Dict[jso.String], name="key_columns", default={})

    @property
    def satellite(self) -> VaultObject:
        # TODO: Add self._parent to jso as well as self._document
        if isinstance(self._document, VaultObject):
            return self._document
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
