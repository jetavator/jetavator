from abc import ABC, abstractmethod

from jetavator.config import Config


# TODO: Pull up generic config-owning functionality from Service

class HasConfig(ABC):

    @property
    @abstractmethod
    def config(self) -> Config:
        pass

