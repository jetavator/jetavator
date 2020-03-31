from .SatelliteOwner import SatelliteOwner
from ..VaultObject import HubKeyColumn


class Link(SatelliteOwner, register_as="link"):

    required_yaml_properties = ["link_hubs"]

    optional_yaml_properties = []

    @property
    def satellites_containing_keys(self):
        return self.star_satellites

    @property
    def link_hubs(self):
        return self.hubs

    @property
    def key_length(self):
        return sum([
            hub.key_length + 1
            for hub in self.hubs.values()
        ]) - 1

    @property
    def hubs(self):
        return {
            k: self.project["hub", v]
            for k, v in self.definition["link_hubs"].items()
        }

    @property
    def unique_hubs(self):
        return {
            hub_name: self.project["hub", hub_name]
            for hub_name in set(x.name for x in self.hubs.values())
        }

    def hub_key_columns(self, satellite):
        columns = {}
        for alias, hub in self.link_hubs.items():
            columns.setdefault(hub.name, []).append(
                HubKeyColumn(f'hub_{alias}_key', f'hub_{hub.name}'))
        return columns
