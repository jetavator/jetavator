from typing import Dict, List, Set
from abc import ABC

import sqlalchemy
import wysdom

from pyspark.sql import SparkSession

from lazy_property import LazyProperty

from jetavator.config import ConfigProperty, ComputeServiceConfig
from jetavator.sqlalchemy_delta import HiveDialect
from jetavator.services import ComputeService, Service, ServiceOwner

from .HiveMetastoreInterface import HiveMetastoreInterface
from .ExecutesSparkSQL import ExecutesSparkSQL

SPARK_APP_NAME = 'jetavator'


class SparkConfig(ComputeServiceConfig):
    spark_config: Dict[str, str] = ConfigProperty(wysdom.SchemaDict(str), default={})
    spark_packages: List[str] = ConfigProperty(wysdom.SchemaArray(str), default=[])


class SparkService(ComputeService, Service[SparkConfig, ServiceOwner], ExecutesSparkSQL, HiveMetastoreInterface, ABC):

    @property
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        return HiveDialect()

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
        # TODO: Throw error if conflicting config values are set
        for k, v in self.config.spark_config.items():
            builder = builder.config(k, v)
        for storage_service in self.storage_services.values():
            for k, v in storage_service.spark_config_options.items():
                builder = builder.config(k, v)
        spark_session = builder.getOrCreate()
        spark_session.sparkContext.setLogLevel('ERROR')
        return spark_session

    @property
    def spark_jars_packages(self) -> List[str]:
        return self.config.spark_packages

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
