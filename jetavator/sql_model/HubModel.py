from typing import List

from sqlalchemy import Column, Index

from jetavator.services import StorageService
from jetavator.schema_registry import Hub

from .SatelliteOwnerModel import SatelliteOwnerModel
from .BaseModel import BaseModel


class HubModel(SatelliteOwnerModel, BaseModel[Hub], register_as="hub"):

    @property
    def static_columns(self) -> List[Column]:
        return [
            Column(
                column_name,
                column.type.sqlalchemy_type,
                nullable=True
            )
            for column_name, column in self.definition.static_columns.items()
        ]

    @property
    def role_specific_columns(self) -> List[Column]:
        return self.static_columns

    def satellite_owner_indexes(
            self,
            table_name: str
    ) -> List[Index]:
        return []
