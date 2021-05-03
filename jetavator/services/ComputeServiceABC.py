from abc import ABC, abstractmethod

from jetavator import ServiceOwner
from jetavator.EngineABC import EngineABC
from jetavator.config import ComputeServiceConfig
from .Service import Service


class ComputeServiceABC(Service[ComputeServiceConfig, EngineABC], ServiceOwner, ABC):

    @property
    @abstractmethod
    def engine(self) -> EngineABC:
        pass
