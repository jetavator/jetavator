from typing import Dict, List

from lazy_property import LazyProperty

from sqlalchemy import MetaData, Column, Table
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import *

import wysdom
import pandas

from .SourceColumn import SourceColumn
from ..VaultObject import VaultObject


class Source(VaultObject, register_as="source"):
    columns: Dict[str, SourceColumn] = wysdom.UserProperty(wysdom.SchemaDict(SourceColumn))

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

    # TODO: Re-implement assume_schema_integrity for loading CSVs
    #       (perhaps with a header line safety check!)
    def load_csvs(
            self,
            csv_files: List[str]
            # , assume_schema_integrity=False
    ) -> None:
        """Loads a list of CSV files into a single named Source

        :param csv_files:  List of paths on disk of the CSV files
        """
        # if assume_schema_integrity:
        #     source = self.schema_registry.loaded["source", table_name]
        #     self.spark_runner.load_csv(csv_file, source)
        # else:
        self.compute_service.load_dataframe(
            dataframe=pandas.concat([
                self._csv_to_dataframe(csv_file)
                for csv_file in csv_files
            ]),
            source=self
        )

    def _csv_to_dataframe(
            self,
            csv_file: str
    ) -> pandas.DataFrame:
        date_columns = {
            "jetavator_load_dt"
        }
        dtypes = {
            "jetavator_deleted_ind": "int"
        }
        for k, v in self.columns.items():
            if v.pandas_dtype in ["datetime64[ns]", "timedelta64[ns]"]:
                date_columns.add(k)
            else:
                dtypes[k] = v.pandas_dtype
        try:
            return pandas.read_csv(
                csv_file,
                parse_dates=list(date_columns),
                dtype=dtypes
            )
        except ValueError:
            # File may be missing jetavator_load_dt column, so try again without it
            return pandas.read_csv(
                csv_file,
                parse_dates=list(date_columns - {"jetavator_load_dt"}),
                dtype=dtypes
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
