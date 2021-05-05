from __future__ import annotations

from abc import ABC, abstractmethod

from typing import List, Set, Dict, Optional

from sqlalchemy import Table, Column, Index, PrimaryKeyConstraint
from sqlalchemy import select, case, and_
from sqlalchemy.schema import DDLElement, SchemaItem
from sqlalchemy.types import *
from sqlalchemy.sql.expression import Select, ColumnElement, func, literal_column
from sqlalchemy.sql.functions import Function, coalesce, max

from jetavator.schema_registry import SatelliteOwner, Satellite

from ..VaultAction import VaultAction
from .SQLModel import SQLModel
from .functions import hash_keygen


INDEX_OPTION_KWARGS = set()


class SatelliteModel(SQLModel[Satellite], register_as="satellite"):

    @property
    def files(self) -> List[DDLElement]:

        files = []

        if self.action in (VaultAction.CREATE, VaultAction.DROP):

            # tables must be created first
            if self.action == VaultAction.CREATE:
                files += self.create_tables(self.tables)

            # tables must be dropped last
            if self.action == VaultAction.DROP:
                files += self.drop_tables(self.tables)

        return files

    @property
    def tables(self) -> List[Table]:
        return [
            self.table
        ]

    @property
    def history_views(self) -> List[DDLElement]:
        return [
            self.create_or_drop_view(
                self.history_view,
                self.history_view_query)
        ]

    @property
    def current_views(self) -> List[DDLElement]:
        return [
            self.create_or_drop_view(
                self.current_view,
                self.current_view_query)
        ]

    @property
    def parent(self) -> SatelliteOwnerModel:
        return self.owner[self.definition.parent.key]

    @property
    def date_columns(self) -> List[Column]:
        return [
            Column("sat_load_dt", DateTime(), nullable=True),
            Column("sat_deleted_ind", Boolean(), nullable=True, default=0)
        ]

    @property
    def record_source_columns(self) -> List[Column]:
        return [
            Column("sat_record_source", String(), nullable=True),
            Column("sat_record_hash", CHAR(32), nullable=True)
        ]

    @property
    def expiry_date_column(self) -> Column:
        return Column("sat_expiry_dt", DateTime, nullable=True)

    @property
    def latest_version_ind_column(self) -> Column:
        return Column("sat_latest_version_ind", CHAR(1), nullable=True)

    @property
    def satellite_columns(self) -> List[Column]:
        return self.definition.satellite_columns

    @property
    def link_key_columns(self) -> List[Column]:
        return self.parent.definition.link_key_columns

    @property
    def table(self) -> Table:
        return self.define_table(
            self.definition.table_name,
            *self.parent.key_columns,
            *self.link_key_columns,
            *self.date_columns,
            *self.record_source_columns,
            *self.satellite_columns,
            schema=self.vault_schema
        )

    def custom_indexes(self, table_name: str) -> List[Index]:
        return [
            Index(
                f"ix_{table_name}_ix_{column_name}",
                column_name,
                unique=False
            )
            for column_name, column in self.definition.columns.items()
            if column.index
        ]

    @property
    def view_columns(self) -> List[Column]:
        return [
            *self.parent.key_columns,
            *self.link_key_columns,
            *self.date_columns,
            self.expiry_date_column,
            *self.record_source_columns,
            self.latest_version_ind_column,
            *self.satellite_columns
        ]

    @property
    def expiry_date_expression(self) -> Function:
        return coalesce(
            max(self.table.c.sat_load_dt).over(
                partition_by=self.columns_in_table(
                    self.table, self.parent.key_columns),
                order_by=self.table.c.sat_load_dt,
                rows=(1, 1)
            ),
            literal_column("CAST('9999-12-31 00:00' AS DATE)")
        )

    @property
    def latest_version_ind_expression(self) -> ColumnElement:
        return case(
            {1: 1},
            value=func.row_number().over(
                partition_by=self.columns_in_table(
                    self.table, self.parent.key_columns),
                order_by=self.table.c.sat_load_dt.desc()
            ),
            else_=0
        )

    @property
    def history_view(self) -> Table:
        return self.define_table(
            f'vault_history_{self.definition.name}',
            *self.view_columns,
            schema=self.vault_schema
        )

    @property
    def history_view_query(self) -> Select:
        return select([
            *self.columns_in_table(
                self.table, self.parent.key_columns),
            *self.columns_in_table(
                self.table, self.link_key_columns),
            *self.columns_in_table(
                self.table, self.date_columns),
            self.expiry_date_expression.label("sat_expiry_dt"),
            *self.columns_in_table(
                self.table, self.record_source_columns),
            self.latest_version_ind_expression.label("sat_latest_version_ind"),
            *self.columns_in_table(
                self.table, self.satellite_columns),
        ]).select_from(
            self.table
        )

    @property
    def current_view(self) -> Table:
        return self.define_table(
            f'vault_now_{self.definition.name}',
            *self.view_columns,
            schema=self.vault_schema
        )

    @property
    def current_view_query(self) -> Select:
        return select([
            *self.columns_in_table(
                self.history_view, self.parent.key_columns),
            *self.columns_in_table(
                self.history_view, self.link_key_columns),
            *self.columns_in_table(
                self.history_view, self.date_columns),
            self.history_view.c.sat_expiry_dt,
            *self.columns_in_table(
                self.history_view, self.record_source_columns),
            self.history_view.c.sat_latest_version_ind,
            *self.columns_in_table(
                self.history_view, self.satellite_columns),
        ]).where(
            and_(
                self.history_view.c.sat_latest_version_ind == 1,
                self.history_view.c.sat_deleted_ind == 0
            )
        )


class SatelliteOwnerModel(SQLModel[SatelliteOwner], ABC, register_as="satellite_owner"):

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

    @property
    def index_option_kwargs(self) -> Set[str]:
        return INDEX_OPTION_KWARGS

    @property
    def index_kwargs(self) -> Dict[str, bool]:
        return {
            x: self.definition.option(x)
            for x in self.index_option_kwargs
        }

    def index(
            self,
            name: str,
            alias: Optional[str] = None
    ) -> Index:
        alias = alias or self.definition.name
        return Index(
            f"ix_{name}",
            self.definition.alias_primary_key_column(alias),
            unique=False,
            **self.index_kwargs
        )

    def index_or_key(
            self,
            name: str,
            alias: Optional[str] = None
    ) -> SchemaItem:
        alias = alias or self.definition.name
        if self.definition.option("no_primary_key"):
            return self.index(name, alias)
        else:
            return PrimaryKeyConstraint(
                self.definition.alias_primary_key_name(alias),
                name=f"pk_{name}",
                **self.index_kwargs
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
            Column(f"{self.definition.type}_record_source", String(), nullable=True),
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
            self.definition.table_name,
            *self.table_columns,
            self.index_or_key(f"{self.definition.type}_{self.definition.name}"),
            *self.satellite_owner_indexes(f"{self.definition.type}_{self.definition.name}"),
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
            *self.satellite_owner_indexes(f"{self.star_prefix}_{self.definition.name}"),
            *self.custom_indexes(f"{self.star_prefix}_{self.definition.name}"),
            schema=self.star_schema
        )

    @property
    def star_satellite_models(self) -> Dict[str, SatelliteModel]:
        return {
            satellite_model.definition.name: satellite_model
            for satellite_model in self.owner.satellites.values()
            if satellite_model.definition.parent.key == self.definition.key
            and satellite_model.action != VaultAction.DROP
            and not satellite_model.definition.exclude_from_star_schema
        }

    @abstractmethod
    def satellite_owner_indexes(
            self,
            table_name: str
    ) -> List[Index]:
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
