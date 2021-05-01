from jetavator.EngineABC import EngineABC
from jetavator.services import Service
from jetavator.services.Service import ConfigType

from .ComputeServiceABC import ComputeServiceABC


class ComputeOwnedService(Service):

    def __init__(self, owner: EngineABC, config: ConfigType):
        super().__init__(owner, config)

    @property
    def owner(self) -> ComputeServiceABC:
        assert isinstance(self._owner, ComputeServiceABC)
        return self._owner

    @property
    def engine(self) -> EngineABC:
        return self.owner.engine
