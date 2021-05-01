from jetavator.EngineABC import EngineABC
from jetavator.services import Service
from jetavator.services.Service import ConfigType


class EngineOwnedService(Service):

    def __init__(self, owner: EngineABC, config: ConfigType):
        super().__init__(owner, config)

    @property
    def owner(self) -> EngineABC:
        assert isinstance(self._owner, EngineABC)
        return self._owner

    @property
    def engine(self) -> EngineABC:
        return self.owner
