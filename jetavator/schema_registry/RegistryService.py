from typing import Iterator
from abc import ABC, abstractmethod
from collections.abc import Mapping

from jetavator.ServiceOwner import ServiceOwner
from jetavator.config import RegistryServiceConfig
from jetavator.services import Service
from .Project import Project


class RegistryService(Service[RegistryServiceConfig, ServiceOwner], Mapping, ABC):

    @abstractmethod
    def __getitem__(self, k: str) -> Project:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[Project]:
        pass

    @property
    @abstractmethod
    def loaded(self) -> Project:
        pass

    @property
    @abstractmethod
    def deployed(self) -> Project:
        pass
