from typing import Any
from abc import ABC, abstractmethod

from jetavator.ServiceOwner import ServiceOwner
from jetavator.config import EngineConfig
from .HasConfig import HasConfig
from .HasLogger import HasLogger


class EngineABC(ServiceOwner, HasConfig[EngineConfig], HasLogger, ABC):

    @property
    @abstractmethod
    def compute_service(self) -> Any:
        pass
