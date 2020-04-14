from ..mixins import RegistersSubclasses

class Service(RegistersSubclasses):

    def __init__(self, engine, config):
        self.engine = engine
        self.config = config

    @classmethod
    def from_config(cls, engine, config):
        return cls.registered_subclass_instance(config.type, engine, config)