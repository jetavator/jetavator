from typing import List

from sqlalchemy import Table, Column, Index
from sqlalchemy import select, case, and_
from sqlalchemy.schema import DDLElement

from sqlalchemy.types import *
from sqlalchemy.sql.expression import Select, ColumnElement, func, literal_column
from sqlalchemy.sql.functions import Function, coalesce, max

from jetavator.schema_registry import Satellite

from ..VaultAction import VaultAction
from .SatelliteModelABC import SatelliteModelABC
from .SatelliteOwnerModel import SatelliteOwnerModel


class SatelliteModel(SatelliteModelABC, register_as="satellite"):

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
        return self.project[self.definition.parent.key]

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
