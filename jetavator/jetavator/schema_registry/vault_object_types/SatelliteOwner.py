from typing import Dict

from ..VaultObject import VaultObject, HubKeyColumn


class SatelliteOwner(VaultObject, register_as="satellite_owner"):

    required_yaml_properties = []

    optional_yaml_properties = ["options", "exclude_from_star_schema"]

    @property
    def satellites(self):
        return {
            satellite.name: satellite
            for satellite in self.project["satellite"].values()
            if satellite.definition["parent"]["type"] == self.type
            and satellite.definition["parent"]["name"] == self.name
        }

    @property
    def star_satellites(self):
        return {
            satellite.name: satellite
            for satellite in self.satellites.values()
            if satellite.action != "drop"
            and not satellite.exclude_from_star_schema
        }

    @property
    def satellites_containing_keys(self):
        raise NotImplementedError

    @property
    def satellite_columns(self):
        return {
            column_name: column
            for satellite in self.star_satellites.values()
            for column_name, column in satellite.columns.items()
        }

    @property
    def key_column_name(self):
        return f"{self.type}_{self.name}_key"

    @property
    def hash_column_name(self):
        return f"{self.type}_{self.name}_hash"

    @property
    def hashed_columns(self):
        return self.satellite_columns

    def hub_key_columns(self, satellite) -> Dict[str, HubKeyColumn]:
        raise NotImplementedError

    def option(self, option_name):
        if "options" not in self.definition:
            return False
        else:
            return any(
                option == option_name
                for option in self.definition["options"]
            )
