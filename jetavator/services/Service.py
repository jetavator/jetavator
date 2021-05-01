from __future__ import annotations

from typing import TypeVar, Generic
from abc import ABC, abstractmethod

import logging

from wysdom.mixins import RegistersSubclasses

from jetavator import ServiceOwner
from jetavator.config import ServiceConfig
from jetavator.EngineABC import EngineABC

ConfigType = TypeVar('ConfigType', bound=ServiceConfig)


class Service(RegistersSubclasses, Generic[ConfigType], ABC):

    def __init__(self, owner: ServiceOwner, config: ConfigType):
        super().__init__()
        self._owner = owner
        self._config = config

    @property
    def owner(self) -> ServiceOwner:
        return self._owner

    @property
    def config(self) -> ConfigType:
        return self._config

    @property
    def logger(self) -> logging.Logger:
        return self.owner.logger

    @classmethod
    def from_config(cls, owner: ServiceOwner, config: ConfigType) -> Service:
        return cls.registered_subclass_instance(config.type, owner, config)

    @property
    @abstractmethod
    def engine(self) -> EngineABC:
        pass


