from typing import List, Dict
from abc import ABC, abstractmethod

import pyspark
import sqlalchemy

from jetavator.services.StorageService import StorageService
from .HiveMetastoreInterface import HiveMetastoreInterface
from .ExecutesSparkSQL import ExecutesSparkSQL

SPARK_APP_NAME = 'jetavator'

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

    spark_config_options: Dict[str, str] = {}
    spark_jars_packages: List[str] = []

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
