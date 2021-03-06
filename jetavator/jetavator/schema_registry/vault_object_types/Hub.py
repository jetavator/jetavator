from typing import Optional, Dict, List

from sqlalchemy import func

import wysdom

from .SatelliteOwner import SatelliteOwner
from .Satellite import Satellite
from .SatelliteColumn import SatelliteColumn
from ..VaultObject import VaultObject, HubKeyColumn


class Hub(SatelliteOwner, register_as="hub"):
    star_prefix = "dim"

    key_length: int = wysdom.UserProperty(int)
    key_type: Optional[str] = wysdom.UserProperty(str, optional=True)
    static_columns: Dict[str, SatelliteColumn] = wysdom.UserProperty(
        wysdom.SchemaDict(SatelliteColumn), persist_defaults=True, default={})

    @property
    def hubs(self) -> Dict[str, VaultObject]:
        return {
            self.name: self
        }

    @property
    def satellites_containing_keys(self) -> Dict[str, VaultObject]:
        return {
            key: sat
            for key, sat in self.project.satellites.items()
            if sat.parent.key == self.key
            or sat.parent.key in [link.key for link in self.links.values()]
            or self.name in sat.referenced_hubs.keys()
        }

    @property
    def links(self) -> Dict[str, VaultObject]:
        return {
            key: link
            for key, link in self.project.links.items()
            if self.name in link.unique_hubs.keys()
        }

    def hub_key_columns(self, satellite: Satellite) -> Dict[str, List[HubKeyColumn]]:
        return {
            self.name: [HubKeyColumn(
                self.key_column_name, f'sat_{satellite.name}'
            )]
        }

    def generate_key(self, from_table):
        return from_table.c[self.key_name]

    def prepare_key_for_link(self, alias, from_table):
        key_column = from_table.c[self.alias_key_name(alias)]
        if self.key_type == "DATETIME":
            return func.convert(VARCHAR, key_column, 126)
        elif self.key_type == "DATE":
            return func.convert(VARCHAR, key_column, 23)
        else:
            return func.upper(func.ltrim(func.rtrim(key_column)))

    @property
    def link_key_columns(self):
        return []

    def validate(self) -> None:
        pass
