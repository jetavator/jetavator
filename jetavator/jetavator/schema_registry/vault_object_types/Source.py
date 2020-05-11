from typing import Dict, List

from lazy_property import LazyProperty

from sqlalchemy import MetaData, Column, Table
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import *

import jsdom

from .SourceColumn import SourceColumn
from ..VaultObject import VaultObject


class Source(VaultObject, register_as="source"):
    columns: Dict[str, SourceColumn] = jsdom.Property(jsdom.Dict(SourceColumn))

    @property
    def primary_key_columns(self) -> Dict[str, SourceColumn]:
        return {
            k: v
            for k, v in self.columns.items()
            if v.pk
        }

    def validate(self) -> None:
        pass

    @LazyProperty
    def create_table_statement(self) -> CreateTable:
        return CreateTable(self.table)

    @LazyProperty
    def table(self) -> Table:
        return Table(
            self.full_name,
            MetaData(),
            *self._table_columns()
        )

    def _table_columns(self) -> List[Column]:
        # TODO: Spark/Hive does not allow PKs. Make this configurable per engine?
        use_primary_key = False
        return [
            *self._source_columns(use_primary_key),
            *self._date_columns(use_primary_key)
        ]

    def _source_columns(self, use_primary_key: bool = True) -> List[Column]:
        return [
            Column(
                column_name,
                eval(column.type.upper().replace("MAX", "None")),
                nullable=True,
                primary_key=(use_primary_key and column.pk)
            )
            for column_name, column in self.columns.items()
        ]

    @staticmethod
    def _date_columns(use_primary_key: bool = True) -> List[Column]:
        return [
            Column(
                "jetavator_load_dt",
                DATETIME,
                nullable=True,
                primary_key=use_primary_key),
            Column(
                "jetavator_deleted_ind",
                CHAR(1),
                nullable=True,
                default=0)
        ]
