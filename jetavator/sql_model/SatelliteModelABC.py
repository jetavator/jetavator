from abc import ABC, abstractmethod
from typing import List

from sqlalchemy import Column, Index

from jetavator.schema_registry import Satellite

from .BaseModel import BaseModel


class SatelliteModelABC(BaseModel[Satellite], ABC):

    @property
    @abstractmethod
    def satellite_columns(self) -> List[Column]:
        pass

    @abstractmethod
    def custom_indexes(self, table_name: str) -> List[Index]:
        pass
