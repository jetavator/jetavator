from abc import ABC, abstractmethod

from sqlalchemy import MetaData

from jetavator.config import Config


class ProjectModelABC(ABC):

    @property
    @abstractmethod
    def config(self) -> Config:
        pass

    @property
    @abstractmethod
    def metadata(self) -> MetaData:
        pass
