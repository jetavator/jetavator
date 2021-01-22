from typing import Iterable
from abc import ABC

import datetime
import os
import tempfile

import numpy as np
import pyspark
import sqlalchemy
import sqlparse
import pandas

from jetavator.sqlalchemy_delta import HiveWithDDLDialect, DeltaDialect

from .ComputeService import ComputeService
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


class SparkService(ComputeService, ExecutesSparkSQL, HiveMetastoreInterface, ABC):

    # TODO: Require to avoid need for try/except block
    # TODO: Don't hardcode DeltaDialect - make the storage configurable and separate from the compute
    # TODO: Refactor compile_delta_lake and compile_hive back into one sensible framework
    @staticmethod
    def compile_delta_lake(sqlalchemy_executable):
        try:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=DeltaDialect(),
                    compile_kwargs={"literal_binds": True}
                )),
                reindent=True,
                keyword_case='upper'
            )
        except TypeError:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=DeltaDialect()
                )),
                reindent=True,
                keyword_case='upper'
            )
        return formatted

    @staticmethod
    def compile_hive(sqlalchemy_executable):
        try:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=HiveWithDDLDialect(),
                    compile_kwargs={"literal_binds": True}
                )),
                reindent=True,
                keyword_case='upper'
            )
        except TypeError:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=HiveWithDDLDialect()
                )),
                reindent=True,
                keyword_case='upper'
            )
        return formatted

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

    def create_table(self, sqlalchemy_table: sqlalchemy.schema.CreateTable) -> None:
        self.spark.sql(self.compile_delta_lake(sqlalchemy_table))

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

    def execute_sql_element(self, sql_element, async_cursor=False):
        return self.execute(
            self.compile_delta_lake(sql_element)
        )

