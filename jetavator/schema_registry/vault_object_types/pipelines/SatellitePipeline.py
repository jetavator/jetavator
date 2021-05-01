from typing import Dict, List

from abc import ABC, abstractmethod

import wysdom

from wysdom.mixins import RegistersSubclasses

from .PerformanceHints import PerformanceHints
from .SatellitePipelineDependency import SatellitePipelineDependency

from ... import Project
from ..SatelliteABC import SatelliteABC


class SatellitePipeline(wysdom.UserObject, RegistersSubclasses, ABC):

    type: str = wysdom.UserProperty(str)
    performance_hints: PerformanceHints = wysdom.UserProperty(
        PerformanceHints, persist_defaults=True, default={})
    _key_columns: Dict[str, str] = wysdom.UserProperty(
        wysdom.SchemaDict(str), name="key_columns", persist_defaults=True, default={})

    @property
    def satellite(self) -> SatelliteABC:
        # TODO: Improve the type checking here?
        parent = wysdom.parent(self)
        if isinstance(parent, SatelliteABC):
            return parent
        else:
            raise TypeError('Parent is not a subclass of SatelliteABC')

    @property
    def project(self) -> Project:
        return self.satellite.project

    @property
    @abstractmethod
    def dependencies(self) -> List[SatellitePipelineDependency]:
        raise NotImplementedError

    @property
    @abstractmethod
    def key_columns(self) -> Dict[str, str]:
        pass

    def validate(self) -> None:
        for dep in self.dependencies:
            dep.validate()
