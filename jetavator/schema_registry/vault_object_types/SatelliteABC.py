from typing import Dict
from abc import ABC, abstractmethod

from ..VaultObject import VaultObject
from .SatelliteColumn import SatelliteColumn


class SatelliteABC(VaultObject, ABC):

    @property
    @abstractmethod
    def columns(self) -> Dict[str, SatelliteColumn]:
        pass

    @property
    @abstractmethod
    def exclude_from_star_schema(self) -> bool:
        pass
