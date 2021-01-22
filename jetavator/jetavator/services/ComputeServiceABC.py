from typing import Any, Iterable
from abc import ABC, abstractmethod

import pandas

from jetavator import ServiceOwner
from jetavator.EngineABC import EngineABC
from jetavator.config import ComputeServiceConfig
from .EngineOwnedService import EngineOwnedService
from .Service import Service


class ComputeServiceABC(EngineOwnedService, Service[ComputeServiceConfig], ServiceOwner, ABC):

    @property
    @abstractmethod
    def engine(self) -> EngineABC:
        pass

    @abstractmethod
    def load_dataframe(
            self,
            dataframe: pandas.DataFrame,
            source_name: str,
            source_column_names: Iterable[str]
    ) -> None:
        pass
