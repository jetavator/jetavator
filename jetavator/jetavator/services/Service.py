import logging
import logging.config

from wysdom.mixins import RegistersSubclasses

from lazy_property import LazyProperty

from jetavator import DEFAULT_LOGGER_CONFIG


class Service(RegistersSubclasses):

    def __init__(self, engine, config):
        super().__init__()
        self.engine = engine
        self.config = config

    @property
    def logger_config(self):
        return DEFAULT_LOGGER_CONFIG

    @LazyProperty
    def logger(self):
        logging.config.dictConfig(self.logger_config)
        return logging.getLogger('jetavator')

    @classmethod
    def from_config(cls, engine, config):
        return cls.registered_subclass_instance(config.type, engine, config)
