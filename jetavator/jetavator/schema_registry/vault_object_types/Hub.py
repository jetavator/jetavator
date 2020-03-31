from .SatelliteOwner import SatelliteOwner
from ..VaultObject import HubKeyColumn


class Hub(SatelliteOwner, register_as="hub"):

    required_yaml_properties = ["key_length"]

    optional_yaml_properties = ["static_columns", "key_type"]

    @property
    def static_columns(self):
        return self.definition.get("static_columns", {})

    @property
    def satellites_containing_keys(self):
        return {
            key: sat
            for key, sat in self.project.satellites.items()
            if sat.parent.key == self.key
            or sat.parent.key in [link.key for link in self.links.values()]
            or self.name in sat.referenced_hubs.keys()
        }

    @property
    def links(self):
        return {
            key: link
            for key, link in self.project.links.items()
            if self.name in link.unique_hubs.keys()
        }

    def hub_key_columns(self, satellite):
        return {
            self.name: [HubKeyColumn(
                self.key_column_name, f'sat_{satellite.name}'
            )]
        }
