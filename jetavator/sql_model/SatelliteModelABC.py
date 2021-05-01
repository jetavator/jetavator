from abc import ABC

from jetavator.schema_registry import Satellite

from .BaseModel import BaseModel


class SatelliteModelABC(BaseModel[Satellite], ABC):
    pass
