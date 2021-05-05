from __future__ import annotations

from typing import Dict, List

import wysdom

from .Satellite import Satellite, SatelliteOwner
from .SatelliteColumn import SatelliteColumn
from .ColumnType import ColumnType
from ..VaultObject import VaultObject, HubKeyColumn


class Hub(SatelliteOwner, register_as="hub"):
    star_prefix = "dim"

    _key_type: str = wysdom.UserProperty(str, name="key_type")

    static_columns: Dict[str, SatelliteColumn] = wysdom.UserProperty(
        wysdom.SchemaDict(SatelliteColumn), persist_defaults=True, default={})

    @property
    def key_type(self) -> ColumnType:
        return ColumnType(self._key_type)

    @property
    def key_length(self) -> int:
        return self.key_type.serialized_length

    @property
    def hubs(self) -> Dict[str, VaultObject]:
        return {
            self.name: self
        }

    @property
    def satellites_containing_keys(self) -> Dict[str, VaultObject]:
        return {
            key: sat
            for key, sat in self.owner.satellites.items()
            if isinstance(sat, Satellite)
            if sat.parent.key == self.key
            or sat.parent.key in [link.key for link in self.links.values()]
            or self.name in sat.referenced_hubs.keys()
        }

    @property
    def links(self) -> Dict[str, VaultObject]:
        return {
            key: link
            for key, link in self.owner.links.items()
            if isinstance(link, SatelliteOwner)
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

    # TODO: Should this be in HubModel?
    def prepare_key_for_link(self, alias, from_table):
        key_column = from_table.c[self.alias_key_name(alias)]
        return self.key_type.serialize_column_expression(key_column)

    @property
    def link_key_columns(self):
        return []

    def validate(self) -> None:
        pass

    @property
    def unique_hubs(self) -> Dict[str, Hub]:
        return {
            self.name: self
        }
