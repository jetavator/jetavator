from typing import Iterable, List, Set
from abc import ABC

import sqlalchemy

from pyspark.sql import SparkSession

from lazy_property import LazyProperty

from jetavator.sqlalchemy_delta import HiveWithDDLDialect

from .ComputeService import ComputeService
from .HiveMetastoreInterface import HiveMetastoreInterface
from .ExecutesSparkSQL import ExecutesSparkSQL

SPARK_APP_NAME = 'jetavator'


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
            .config("spark.ui.showConsoleProgress", False)
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


