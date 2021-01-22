from typing import Dict

from sqlalchemy import literal_column

import wysdom

from .SatelliteOwner import SatelliteOwner
from ..VaultObject import HubKeyColumn
from .Hub import Hub
from .Satellite import Satellite


class Link(SatelliteOwner, register_as="link"):

    star_prefix = "fact"

    # TODO: Rename link_hubs to hubs
    _link_hubs: Dict[str, str] = wysdom.UserProperty(
        wysdom.SchemaDict(str), name='link_hubs')

    @property
    def hubs(self) -> Dict[str, Hub]:
        return {
            k: self.project['hub', v]
            for k, v in self._link_hubs.items()
        }

    @property
    def satellites_containing_keys(self) -> Dict[str, Satellite]:
        return self.star_satellites

    @property
    def key_length(self) -> int:
        return sum([
            hub.key_length + 1
            for hub in self.hubs.values()
        ]) - 1

    @property
    def unique_hubs(self) -> Dict[str, Hub]:
        return {
            hub_name: self.project["hub", hub_name]
            for hub_name in set(x.name for x in self.hubs.values())
        }

    def hub_key_columns(self, satellite) -> Dict[str, HubKeyColumn]:
        columns = {}
        for alias, hub in self.hubs.items():
            columns.setdefault(hub.name, []).append(
                HubKeyColumn(f'hub_{alias}_key', f'hub_{hub.name}'))
        return columns

    def generate_key(self, from_table):
        key_components = iter([
            hub.prepare_key_for_link(hub_alias, from_table)
            for hub_alias, hub in self.hubs.items()
        ])
        composite_key = next(key_components)
        for column in key_components:
            composite_key = composite_key.concat(
                literal_column("'/'")
            ).concat(column)
        return composite_key

    @property
    def link_key_columns(self):
        return [
            hub.alias_key_column(hub_alias)
            for hub_alias, hub in self.hubs.items()
        ]

    def validate(self) -> None:
        for k, v in self._link_hubs.items():
            if ('hub', v) not in self.project:
                raise KeyError(
                    f"Cannot find referenced hub {v} in object {self.key}"
                )
