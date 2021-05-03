from __future__ import annotations

from typing import TypeVar, Generic
from abc import ABC, abstractmethod

import logging

from wysdom.mixins import RegistersSubclasses

from jetavator import ServiceOwner
from jetavator.config import ServiceConfig
from jetavator.EngineABC import EngineABC

ConfigType = TypeVar('ConfigType', bound=ServiceConfig, covariant=True)
ServiceOwnerType = TypeVar('ServiceOwnerType', bound=ServiceOwner, covariant=True)


class Service(RegistersSubclasses, Generic[ConfigType, ServiceOwnerType], ABC):

    def __init__(self, owner: ServiceOwnerType, config: ConfigType):
        super().__init__()
        self._owner = owner
        self._config = config

    @property
    def owner(self) -> ServiceOwnerType:
        return self._owner

    @property
    def config(self) -> ConfigType:
        return self._config

    @property
    def logger(self) -> logging.Logger:
        return self.owner.logger

    @classmethod
    def from_config(cls, owner: ServiceOwnerType, config: ConfigType) -> Service:
        return cls.registered_subclass_instance(config.type, owner, config)

    @property
    @abstractmethod
    def engine(self) -> EngineABC:
        pass


