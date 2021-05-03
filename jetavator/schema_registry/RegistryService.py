from jetavator.EngineABC import EngineABC
from jetavator.config import RegistryServiceConfig
from jetavator.services import Service


class RegistryService(Service[RegistryServiceConfig, EngineABC]):

    @property
    def engine(self) -> EngineABC:
        return self.owner
