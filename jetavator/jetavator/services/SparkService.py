from typing import Iterable, List, Set
from abc import ABC

import datetime
import os
import tempfile

import numpy as np
import pyspark
import sqlalchemy
import pandas

from pyspark.sql import SparkSession

from lazy_property import LazyProperty

from jetavator.sqlalchemy_delta import HiveWithDDLDialect

from .ComputeService import ComputeService
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


class SparkService(ComputeService, ExecutesSparkSQL, HiveMetastoreInterface, ABC):

    @property
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        return HiveWithDDLDialect()

    @LazyProperty
    def spark(self):
        builder = (
            SparkSession
            .builder
            .appName(SPARK_APP_NAME)
            .enableHiveSupport()
            .config("spark.jars.packages", ",".join(self.all_spark_jars_packages))
        )
        for storage_service in self.storage_services.values():
            for k, v in storage_service.spark_config_options.items():
                builder = builder.config(k, v)
        spark_session = builder.getOrCreate()
        spark_session.sparkContext.setLogLevel('ERROR')
        return spark_session

    @property
    def spark_jars_packages(self) -> List[str]:
        return []

    @property
    def all_spark_jars_packages(self) -> Set[str]:
        return {
            *self.spark_jars_packages,
            *(
                package
                for storage_service in self.storage_services.values()
                for package in storage_service.spark_jars_packages
            )
        }

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

    def prepare_environment(self) -> None:
        # TODO: Make this platform-independent - currently HIVE specific
        # TODO: Is this obsolete now?
        self.execute(
            f'USE `{self.config.schema}`'
        )

    def sql_query_single_value(self, sql):
        return self.execute(sql).iloc[0, 0]

    def test(self):
        assert self.sql_query_single_value("SELECT 1") == 1
        return True


