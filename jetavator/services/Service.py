from __future__ import annotations

from typing import TypeVar, Generic, Any
from abc import ABC

import logging

from wysdom.mixins import RegistersSubclasses

from jetavator import ServiceOwner
from jetavator.config import ServiceConfig

ConfigType = TypeVar('ConfigType', bound=ServiceConfig, covariant=True)
ServiceOwnerType = TypeVar('ServiceOwnerType', bound=ServiceOwner, covariant=True)


class Service(RegistersSubclasses, Generic[ConfigType, ServiceOwnerType], ABC):

    def __init__(self, config: ConfigType, owner: ServiceOwnerType):
        super().__init__()
        self._config = config
        self._owner = owner

    @classmethod
    def from_config(cls, config: ConfigType, owner: ServiceOwnerType, *args: Any, **kwargs: Any) -> Service:
        return cls.registered_subclass_instance(config.type, config, owner, *args, **kwargs)

    @property
    def owner(self) -> ServiceOwnerType:
        return self._owner

    @property
    def config(self) -> ConfigType:
        return self._config

    @property
    def logger(self) -> logging.Logger:
        return self.owner.logger


