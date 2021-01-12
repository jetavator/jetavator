from __future__ import annotations

from typing import Dict, Any

import logging
import logging.config

from wysdom.mixins import RegistersSubclasses

from lazy_property import LazyProperty

from jetavator import DEFAULT_LOGGER_CONFIG, Engine
from jetavator.config import ServiceConfig


class Service(RegistersSubclasses):

    def __init__(self, engine: Engine, config: ServiceConfig):
        super().__init__()
        self.engine = engine
        self.config = config

    @property
    def logger_config(self) -> Dict[str, Any]:
        return DEFAULT_LOGGER_CONFIG

    @LazyProperty
    def logger(self) -> logging.Logger:
        logging.config.dictConfig(self.logger_config)
        return logging.getLogger('jetavator')

    @classmethod
    def from_config(cls, engine: Engine, config: ServiceConfig) -> Service:
        return cls.registered_subclass_instance(config.type, engine, config)
