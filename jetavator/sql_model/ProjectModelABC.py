from typing import Any
from abc import ABC, abstractmethod

from sqlalchemy import MetaData

from jetavator.config import AppConfig


class ProjectModelABC(ABC):

    @property
    @abstractmethod
    def config(self) -> AppConfig:
        pass

    @property
    @abstractmethod
    def metadata(self) -> MetaData:
        pass
