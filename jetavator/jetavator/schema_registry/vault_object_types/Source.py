from typing import Dict, List, Optional

from lazy_property import LazyProperty

from sqlalchemy import MetaData, Column, Table
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import DateTime, Integer

import wysdom

from .SourceColumn import SourceColumn
from ..VaultObject import VaultObject

FilePath = str


class Source(VaultObject, register_as="source"):

    DELETED_INDICATOR_SYSTEM_COLUMN = "jetavator_deleted_ind"
    LOAD_TIMESTAMP_SYSTEM_COLUMN = "jetavator_load_dt"

    columns: Dict[str, SourceColumn] = wysdom.UserProperty(wysdom.SchemaDict(SourceColumn))
    csv_files: List[str] = []
    deleted_indicator_column: Optional[str] = wysdom.UserProperty(str, optional=True)
    load_timestamp_column: Optional[str] = wysdom.UserProperty(str, optional=True)
    date_format: Optional[str] = wysdom.UserProperty(str, optional=True)
    timestamp_format: Optional[str] = wysdom.UserProperty(str, optional=True)

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

    # TODO: Move to sql_model?
    @LazyProperty
    def table(self) -> Table:
        return Table(
            self.full_name,
            MetaData(),
            *self._table_columns()
        )

    def load_csvs(
            self,
            csv_files: List[FilePath]
            # , assume_schema_integrity=False
    ) -> None:
        """Loads a list of CSV files into a single named Source

        :param csv_files:  List of paths on disk of the CSV files
        """
        self.csv_files = csv_files

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
                column.type.sqlalchemy_type,
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
                DateTime(),
                nullable=True,
                primary_key=use_primary_key),
            Column(
                "jetavator_deleted_ind",
                # TODO: Loading as integer saves space in CSVs.
                #       Does this make sense for other file formats?
                #       Is there a more general solution?
                Integer(),
                nullable=True,
                default=0)
        ]
