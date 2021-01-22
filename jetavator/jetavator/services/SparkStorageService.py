from typing import Iterable
from abc import ABC, abstractmethod

import datetime
import os
import tempfile

import numpy as np
import pyspark
import sqlalchemy
import sqlalchemy_views
import pandas

from jetavator.sqlalchemy_delta import HiveWithDDLDialect

from .StorageService import StorageService
from .HiveMetastoreInterface import HiveMetastoreInterface
from .ExecutesSparkSQL import ExecutesSparkSQL

SPARK_APP_NAME = 'jetavator'
DELTA_VERSION = 'delta-core_2.12:0.7.0'

PYSPARK_COLUMN_TYPE_MAPPINGS = [
    (sqlalchemy.types.String, pyspark.sql.types.StringType),
    (sqlalchemy.types.Integer, pyspark.sql.types.IntegerType),
    (sqlalchemy.types.Float, pyspark.sql.types.DoubleType),
    (sqlalchemy.types.Date, pyspark.sql.types.DateType),
    (sqlalchemy.types.DateTime, pyspark.sql.types.TimestampType)
]


def pyspark_column_type(sqlalchemy_column):
    for sqlalchemy_type, pyspark_type in PYSPARK_COLUMN_TYPE_MAPPINGS:
        if isinstance(sqlalchemy_column.type, sqlalchemy_type):
            return pyspark_type()


class SparkStorageService(StorageService, ExecutesSparkSQL, HiveMetastoreInterface, ABC):

    # TODO: Require to avoid need for try/except block
    # TODO: Don't hardcode DeltaDialect - make the storage configurable and separate from the compute

    def load_dataframe(
            self,
            dataframe: pandas.DataFrame,
            source_name: str,
            source_column_names: Iterable[str]
    ) -> None:
        for column in source_column_names:
            if column not in dataframe.columns:
                dataframe[column] = np.nan
        if 'jetavator_load_dt' not in dataframe.columns:
            dataframe['jetavator_load_dt'] = datetime.datetime.now()
        if 'jetavator_deleted_ind' not in dataframe.columns:
            dataframe['jetavator_deleted_ind'] = 0
        columns = list(source_column_names) + [
            'jetavator_load_dt',
            'jetavator_deleted_ind'
        ]
        filename = f'{source_name}.csv'
        with tempfile.TemporaryDirectory() as temp_path:
            temp_csv_file = os.path.join(temp_path, filename)
            (
                dataframe
                .reindex(
                    columns=columns)
                .to_csv(
                    temp_csv_file,
                    index=False)
            )
            self.load_csv(temp_csv_file, source_name)

    def load_csv(self, csv_file, source_name: str):
        raise NotImplementedError

    def csv_file_path(self, source_name: str):
        raise NotImplementedError

    def table_delta_path(self, sqlalchemy_table):
        return (
            '/tmp'
            f'/{self.config.schema}'
            f'/{sqlalchemy_table.name}'
        )

    def sql_query_single_value(self, sql):
        return self.execute(sql).iloc[0, 0]

    def test(self):
        assert self.sql_query_single_value("SELECT 1") == 1
        return True

    @staticmethod
    def check_valid_mode(mode):
        valid_modes = ("append", "overwrite")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}': must be one of the following: {valid_modes}")

    @abstractmethod
    def read_table(self, table_name: str) -> pyspark.sql.DataFrame:
        pass

    @abstractmethod
    def write_table(
            self,
            table_name: str,
            df: pyspark.sql.DataFrame,
            mode: str = "append"
    ) -> None:
        pass

    @abstractmethod
    def connect_storage_view(self, table_name: str) -> None:
        pass

    @abstractmethod
    def disconnect_storage_view(self, table_name: str) -> None:
        pass
