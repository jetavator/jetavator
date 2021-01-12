from typing import List

from sqlalchemy import Column, Index

from .SatelliteOwnerModel import SatelliteOwnerModel


class HubModel(SatelliteOwnerModel, register_as="hub"):

    @property
    def static_columns(self) -> List[Column]:
        return [
            Column(
                column_name,
                eval(column.type),
                nullable=True
            )
            for column_name, column in self.definition.static_columns.items()
        ]

    @property
    def role_specific_columns(self) -> List[Column]:
        return self.static_columns

    def satellite_owner_indexes(self, table_name) -> List[Index]:
        return []
