from typing import Optional, Dict, List

from abc import ABC, abstractmethod

from sqlalchemy import Table, Column, Index, PrimaryKeyConstraint
from sqlalchemy.schema import SchemaItem, DDLElement
from sqlalchemy.types import *

from jetavator.schema_registry import SatelliteOwner

from ..VaultAction import VaultAction
from .BaseModel import BaseModel
from .SatelliteModelABC import SatelliteModelABC
from .functions import hash_keygen


class SatelliteOwnerModel(BaseModel[SatelliteOwner], ABC, register_as="satellite_owner"):

    @property
    def files(self) -> List[DDLElement]:
        return self.vault_files() + self.star_files()

    def vault_files(self) -> List[DDLElement]:
        if self.action == VaultAction.CREATE:
            return self.create_tables(self.tables)
        else:
            return []

    def star_files(self, with_index=False) -> List[DDLElement]:
        if self.definition.exclude_from_star_schema:
            return []
        else:
            return (
                self.create_or_alter_tables(self.star_tables, with_index)
            )

    # @property
    # def create_views(self) -> List[CreateView]:
    #     return [CreateView(
    #         self.updates_pit_view,
    #         self.updates_pit_view_query
    #     )]
    #
    # @property
    # def drop_views(self) -> List[DropView]:
    #     return [DropView(self.updates_pit_view)]

    @property
    def tables(self) -> List[Table]:
        return [
            self.table
        ]

    @property
    def star_tables(self) -> List[Table]:
        return [
            self.star_table
        ]

    @property
    def star_prefix(self) -> str:
        return self.definition.star_prefix

    @property
    def key_columns(self) -> List[Column]:
        return self.definition.alias_key_columns(self.definition.name)

    # TODO: Move MSSQL clustering options to a plugin

    def index(
            self,
            name: str,
            alias: Optional[str] = None,
            allow_clustered: bool = True
    ) -> Index:
        alias = alias or self.definition.name
        mssql_clustered = allow_clustered and self.definition.option(
            "mssql_clustered")
        return Index(
            f"ix_{name}",
            self.definition.alias_primary_key_column(alias),
            unique=False,
            mssql_clustered=mssql_clustered
        )

    def index_or_key(
            self,
            name: str,
            alias: Optional[str] = None,
            allow_clustered: bool = True
    ) -> SchemaItem:
        alias = alias or self.definition.name
        if self.definition.option("no_primary_key"):
            return self.index(name, alias, allow_clustered)
        else:
            mssql_clustered = allow_clustered and self.definition.option(
                "mssql_clustered")
            return PrimaryKeyConstraint(
                self.definition.alias_primary_key_name(alias),
                name=f"pk_{name}",
                mssql_clustered=mssql_clustered
            )

    def custom_indexes(self, table_name) -> List[Index]:
        return [
            index
            for satellite_model in self.star_satellite_models.values()
            for index in satellite_model.custom_indexes(table_name)
        ]

    @property
    def record_source_columns(self) -> List[Column]:
        return [
            Column(f"{self.definition.type}_load_dt", DateTime(), nullable=True),
            Column(f"{self.definition.type}_record_source", String(256), nullable=True),
        ]

    @property
    @abstractmethod
    def role_specific_columns(self) -> List[Column]:
        pass

    @property
    def table_columns(self) -> List[Column]:
        return [
            *self.key_columns,
            *self.record_source_columns,
            *self.role_specific_columns
        ]

    @property
    def table(self) -> Table:
        return self.define_table(
            f"vault_{self.definition.type}_{self.definition.name}",
            *self.table_columns,
            self.index_or_key(f"{self.definition.type}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"{self.definition.type}_{self.definition.name}"),
            schema=self.vault_schema
        )

    @property
    def star_satellite_columns(self) -> List[Column]:
        return [
            column
            for satellite_model in self.star_satellite_models.values()
            for column in satellite_model.satellite_columns
        ]

    @property
    def star_table(self) -> Table:
        return self.define_table(
            self.definition.star_table_name,
            *self.key_columns,
            *self.role_specific_columns,
            *self.star_satellite_columns,
            self.index_or_key(f"{self.star_prefix}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            *self.custom_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            schema=self.star_schema
        )

    @property
    def star_satellite_models(self) -> Dict[str, SatelliteModelABC]:
        return {
            satellite_model.definition.name: satellite_model
            for satellite_model in self.project.satellites.values()
            if satellite_model.definition.parent.key == self.definition.key
            and satellite_model.action != VaultAction.DROP
            and not satellite_model.definition.exclude_from_star_schema
        }

    @abstractmethod
    def satellite_owner_indexes(self, table_name: str) -> List[Index]:
        pass

    def generate_table_keys(self, source_table, alias=None):
        alias = alias or self.definition.name
        if self.definition.option("hash_key"):
            hash_key = [hash_keygen(
                source_table.c[self.definition.alias_key_name(alias)]
                ).label(self.definition.alias_hash_key_name(alias))]
        else:
            hash_key = []
        return hash_key + [
            source_table.c[self.definition.alias_key_name(alias)]
        ]
