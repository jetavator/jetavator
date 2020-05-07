from typing import Dict, List

from abc import ABC, abstractmethod

from sqlalchemy import Column
from sqlalchemy.types import *

from jetavator import json_schema_objects as jso

from .SatelliteColumn import SatelliteColumn
from ..VaultObject import VaultObject, HubKeyColumn


class SatelliteOwner(VaultObject, ABC, register_as="satellite_owner"):
    options: List[str] = jso.Property(jso.List[jso.String], default=[])
    exclude_from_star_schema: bool = jso.Property(jso.Boolean, default=False)

    @property
    def satellites(self) -> Dict[str, VaultObject]:
        return {
            satellite.name: satellite
            for satellite in self.project.satellites.values()
            if satellite.parent.key == self.key
        }

    @property
    def star_satellites(self) -> Dict[str, VaultObject]:
        return {
            satellite.name: satellite
            for satellite in self.satellites.values()
            if not satellite.exclude_from_star_schema
        }

    @property
    @abstractmethod
    def satellites_containing_keys(self) -> Dict[str, VaultObject]:
        pass

    @property
    def satellite_columns(self) -> Dict[str, SatelliteColumn]:
        return {
            column_name: column
            for satellite in self.star_satellites.values()
            for column_name, column in satellite.columns.items()
        }

    @property
    def key_column_name(self) -> str:
        return f"{self.type}_{self.name}_key"

    @property
    def hash_column_name(self) -> str:
        return f"{self.type}_{self.name}_hash"

    @property
    def hashed_columns(self) -> Dict[str, SatelliteColumn]:
        return self.satellite_columns

    def hub_key_columns(self, satellite) -> Dict[str, HubKeyColumn]:
        raise NotImplementedError

    def option(self, option_name: str) -> bool:
        return any(
            option == option_name
            for option in self.options
        )

    @abstractmethod
    def validate(self) -> None:
        pass

    def alias_key_name(self, alias):
        return f"{self.type}_{alias}_key"

    def alias_hash_key_name(self, alias):
        return f"{self.type}_{alias}_hash"

    @property
    def key_name(self):
        return self.alias_key_name(self.name)

    @property
    def hash_key_name(self):
        return self.alias_hash_key_name(self.name)

    def alias_primary_key_name(self, alias):
        if self.option("hash_key"):
            return self.alias_hash_key_name(alias)
        else:
            return self.alias_key_name(alias)

    @abstractmethod
    def generate_key(self, from_table):
        pass

    @property
    @abstractmethod
    def link_key_columns(self):
        pass

    def generate_table_keys(self, source_table, alias=None):
        alias = alias or self.name
        if self.option("hash_key"):
            hash_key = [hash_keygen(
                source_table.c[self.alias_key_name(alias)]
                ).label(self.alias_hash_key_name(alias))]
        else:
            hash_key = []
        return hash_key + [
            source_table.c[self.alias_key_name(alias)]
        ]

    def alias_key_column(self, alias):
        return Column(
            self.alias_key_name(alias),
            CHAR(self.key_length),
            nullable=False
        )

    def alias_hash_key_column(self, alias):
        return Column(
            self.alias_hash_key_name(alias),
            CHAR(32),
            nullable=False
        )

    def alias_key_columns(self, alias):
        if self.option("hash_key"):
            return [
                self.alias_hash_key_column(alias),
                self.alias_key_column(alias)
            ]
        else:
            return [
                self.alias_key_column(alias)
            ]

    def alias_primary_key_column(self, alias):
        if self.option("hash_key"):
            return self.alias_hash_key_column(alias)
        else:
            return self.alias_key_column(alias)

    @property
    def star_table_name(self):
        return f"star_{self.star_prefix}_{self.name}"

    @property
    @abstractmethod
    def star_prefix(self):
        pass
