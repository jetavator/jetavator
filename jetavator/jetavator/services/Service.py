from jsdom.mixins import RegistersSubclasses


class Service(RegistersSubclasses):

    def __init__(self, engine, config):
        super().__init__()
        self.engine = engine
        self.config = config

    @classmethod
    def from_config(cls, engine, config):
        return cls.registered_subclass_instance(config.type, engine, config)
