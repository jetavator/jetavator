from typing import Dict, List, Iterable

from abc import ABC, abstractmethod

import wysdom

from wysdom.mixins import RegistersSubclasses

from .PerformanceHints import PerformanceHints
from .PipelineDependency import PipelineDependency

from ... import VaultObjectOwner


class PipelineOwner(ABC):

    @property
    @abstractmethod
    def owner(self) -> VaultObjectOwner:
        pass

    @property
    @abstractmethod
    def default_pipeline_keys(self) -> Iterable[str]:
        pass


class Pipeline(wysdom.UserObject, RegistersSubclasses, ABC):

    type: str = wysdom.UserProperty(str)
    performance_hints: PerformanceHints = wysdom.UserProperty(
        PerformanceHints, persist_defaults=True, default={})
    _key_columns: Dict[str, str] = wysdom.UserProperty(
        wysdom.SchemaDict(str), name="key_columns", persist_defaults=True, default={})

    @property
    def owner(self) -> PipelineOwner:
        parent = wysdom.parent(self)
        assert isinstance(parent, PipelineOwner)
        return parent

    @property
    def _vault_objects(self) -> VaultObjectOwner:
        return self.owner.owner

    @property
    @abstractmethod
    def dependencies(self) -> List[PipelineDependency]:
        pass

    @property
    @abstractmethod
    def key_columns(self) -> Dict[str, str]:
        pass

    def validate(self) -> None:
        for dep in self.dependencies:
            dep.validate()
