from __future__ import annotations

from typing import Dict

from lazy_property import LazyProperty

from sqlalchemy import Column
from sqlalchemy.types import *

import jsdom

from ..VaultObject import (
    VaultObject, VaultObjectKey, HubKeyColumn
)
from ..VaultObjectCollection import VaultObjectSet
from .SatelliteColumn import SatelliteColumn
from .SatelliteOwner import SatelliteOwner
from .pipelines import SatellitePipeline


class VaultObjectReference(jsdom.Object):

    type: str = jsdom.Property(str)
    name: str = jsdom.Property(str)

    @LazyProperty
    def key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)


class Satellite(VaultObject, register_as="satellite"):

    _parent: VaultObjectReference = jsdom.Property(
        VaultObjectReference, name="parent")

    columns: Dict[str, SatelliteColumn] = jsdom.Property(
        jsdom.Dict(SatelliteColumn))
    pipeline: SatellitePipeline = jsdom.Property(SatellitePipeline)
    exclude_from_star_schema: bool = jsdom.Property(bool, default=False)

    @property
    def parent(self) -> SatelliteOwner:
        return self.project[self._parent.key]

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
            hub_name: self.project["hub", hub_name]
            for hub_name in VaultObjectSet(
                x.hub_reference
                for x in self.hub_reference_columns.values()
            )
        }

    @property
    def full_name(self) -> str:
        return f'sat_{self.name}'

    @property
    def hub_key_columns(self) -> Dict[str, HubKeyColumn]:
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
            owner
            for dep in self.pipeline.dependencies
            if type(dep.object_reference) is Satellite
            for owner in dep.object_reference.output_keys
        )

    @LazyProperty
    def produced_keys(self) -> VaultObjectSet[SatelliteOwner]:
        if self.pipeline.performance_hints.no_update_hubs:
            keys = VaultObjectSet()
        else:
            keys = VaultObjectSet(
                self.project.hubs[name]
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

    def dependent_satellites_by_owner(self, satellite_owner) -> List[Satellite]:
        return [
            dep.object_reference
            for dep in self.pipeline.dependencies
            if type(dep.object_reference) is Satellite
            for output_key in dep.object_reference.output_keys
            if output_key is satellite_owner
        ]

    def validate(self) -> None:
        if self._parent.key not in self.project:
            raise KeyError(
                f"Could not find parent object {self._parent.key}")
        self.pipeline.validate()

    @property
    def satellite_columns(self):
        return [
            Column(
                column_name,
                eval(column.type.upper().replace("MAX", "None")),
                nullable=True
            )
            for column_name, column in self.columns.items()
        ]

    @property
    def table_name(self):
        return f"vault_sat_{self.name}"
