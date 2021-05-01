from abc import ABC

from jetavator.ServiceOwner import ServiceOwner
from .HasConfig import HasConfig
from .HasLogger import HasLogger


class EngineABC(ServiceOwner, HasConfig, HasLogger, ABC):
    pass
