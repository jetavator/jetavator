from typing import Dict, List

from sqlalchemy import Column, Index

from jetavator.schema_registry import Link

from .SatelliteModel import SatelliteOwnerModel
from .SQLModel import SQLModel


class LinkModel(SatelliteOwnerModel, SQLModel[Link], register_as="link"):

    @property
    def hub_key_columns(self) -> List[Column]:
        return [
            key_column
            for hub_alias, hub_model in self.hub_models.items()
            for key_column in hub_model.definition.alias_key_columns(hub_alias)
        ]

    @property
    def role_specific_columns(self) -> List[Column]:
        return self.hub_key_columns

    def satellite_owner_indexes(
            self,
            table_name: str
    ) -> List[Index]:
        return [
            hub_model.index(
                f"{table_name}_hx_{hub_alias}",
                hub_alias
            )
            for hub_alias, hub_model in self.hub_models.items()
        ]

    @property
    def hub_models(self) -> Dict[str, SatelliteOwnerModel]:
        return {
            k: self.owner[v.key]
            for k, v in self.definition.hubs.items()
        }
