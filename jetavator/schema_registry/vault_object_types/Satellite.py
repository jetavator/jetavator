from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Dict, List, Iterable

from lazy_property import LazyProperty

from sqlalchemy import Column, CHAR

import wysdom

from ..VaultObject import VaultObject, VaultObjectKey, HubKeyColumn
from ..VaultObjectCollection import VaultObjectSet
from .SatelliteColumn import SatelliteColumn
from .ColumnType import ColumnType
from .pipelines import Pipeline, PipelineOwner


class VaultObjectReference(wysdom.UserObject):

    type: str = wysdom.UserProperty(str)
    name: str = wysdom.UserProperty(str)

    @LazyProperty
    def key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)


class Satellite(VaultObject, PipelineOwner, register_as="satellite"):

    _parent: VaultObjectReference = wysdom.UserProperty(
        VaultObjectReference, name="parent")

    columns: Dict[str, SatelliteColumn] = wysdom.UserProperty(
        wysdom.SchemaDict(SatelliteColumn))
    pipeline: Pipeline = wysdom.UserProperty(Pipeline)
    exclude_from_star_schema: bool = wysdom.UserProperty(bool, default=False)

    @property
    def parent(self) -> SatelliteOwner:
        parent = self.owner[self._parent.key]
        assert isinstance(parent, SatelliteOwner)
        return parent

    @property
    def default_pipeline_keys(self) -> Iterable[str]:
        return self.parent.hubs.keys()

    @property
    def hub_reference_columns(self) -> Dict[str, SatelliteColumn]:
        return {
            k: v
            for k, v in self.columns.items()
            if v.hub_reference
        }

    @property
    def referenced_hubs(self) -> Dict[str, SatelliteOwner]:
        return {
            hub_name: self.owner["hub", hub_name]
            for hub_name in VaultObjectSet(
                x.hub_reference
                for x in self.hub_reference_columns.values()
            )
        }

    @property
    def full_name(self) -> str:
        return f'sat_{self.name}'

    @property
    def hub_key_columns(self) -> Dict[str, List[HubKeyColumn]]:
        # check if this can be safely refactored to
        # a function hub_key_columns(self, hub_name)
        columns = self.parent.hub_key_columns(self)
        if (
            self.hub_reference_columns
            and not self.pipeline.performance_hints.no_update_referenced_hubs
        ):
            for column_name, column in self.hub_reference_columns.items():
                columns.setdefault(column.hub_reference, []).append(
                    HubKeyColumn(column_name, f'hub_{column.hub_reference}'))
        return columns

    @LazyProperty
    def input_keys(self) -> VaultObjectSet[SatelliteOwner]:
        return VaultObjectSet(
            satellite_owner
            for satellite in self._dependent_satellites
            for satellite_owner in satellite.output_keys
        )

    @LazyProperty
    def produced_keys(self) -> VaultObjectSet[SatelliteOwner]:
        if self.pipeline.performance_hints.no_update_hubs:
            keys = VaultObjectSet()
        else:
            keys = VaultObjectSet(
                self.owner.hubs[name]
                for name in self.hub_key_columns
            )
        if (
            self.parent.registered_name == 'link'
            and not self.pipeline.performance_hints.no_update_links
        ):
            keys.add(self.parent)
        return keys

    @LazyProperty
    def output_keys(self) -> VaultObjectSet[SatelliteOwner]:
        return self.produced_keys | self.input_keys

    def dependent_satellites_by_owner(self, satellite_owner: SatelliteOwner) -> List[Satellite]:
        return [
            satellite
            for satellite in self._dependent_satellites
            if satellite_owner in satellite.output_keys
        ]

    @property
    def _dependent_satellites(self) -> List[Satellite]:
        return [
            vault_object
            for vault_object in self._dependent_vault_objects
            if isinstance(vault_object, Satellite)
        ]

    @property
    def _dependent_vault_objects(self) -> List[VaultObject]:
        return [
            dep.object_reference
            for dep in self.pipeline.dependencies
        ]

    def validate(self) -> None:
        if self._parent.key not in self.owner:
            raise KeyError(
                f"Could not find parent object {self._parent.key}")
        self.pipeline.validate()

    @property
    def satellite_columns(self):
        return [
            Column(
                column_name,
                column.type.sqlalchemy_type,
                nullable=True
            )
            for column_name, column in self.columns.items()
        ]

    @property
    def table_name(self):
        return f"vault_sat_{self.name}"

    def depends_on(self, other_object: VaultObject) -> bool:
        return any(
            dependency.type == other_object.type
            and dependency.name == other_object.name
            for dependency in self.pipeline.dependencies
        )


class SatelliteOwner(VaultObject, ABC, register_as="satellite_owner"):

    key_length: int = None
    options: List[str] = wysdom.UserProperty(wysdom.SchemaArray(str), default=[])
    exclude_from_star_schema: bool = wysdom.UserProperty(bool, default=False)

    @property
    @abstractmethod
    def hubs(self) -> Dict[str, VaultObject]:
        pass

    @property
    def satellites(self) -> Dict[str, Satellite]:
        return {
            satellite.name: satellite
            for satellite in self.owner.satellites.values()
            if satellite.parent.key == self.key
        }

    @property
    def star_satellites(self) -> Dict[str, Satellite]:
        return {
            satellite.name: satellite
            for satellite in self.satellites.values()
            if not satellite.exclude_from_star_schema
        }

    @property
    @abstractmethod
    def satellites_containing_keys(self) -> Dict[str, Satellite]:
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

    @property
    @abstractmethod
    def key_type(self) -> ColumnType:
        pass

    # TODO: Move SQLAlchemy column generation to sql_model
    def alias_key_column(self, alias):
        return Column(
            self.alias_key_name(alias),
            self.key_type.sqlalchemy_type,
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
    def table_name(self) -> str:
        return f"vault_{self.type}_{self.name}"

    @property
    def star_table_name(self) -> str:
        return f"star_{self.star_prefix}_{self.name}"

    @property
    @abstractmethod
    def star_prefix(self):
        pass

    @property
    @abstractmethod
    def unique_hubs(self) -> Dict[str, SatelliteOwner]:
        pass
