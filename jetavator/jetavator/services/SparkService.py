from typing import Iterable

import datetime
import os
import tempfile

import numpy as np
import pyspark
import sqlalchemy
import sqlparse
import pandas

from jetavator.sqlalchemy_delta import HiveWithDDLDialect, DeltaDialect

from .DBService import DBService

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


class SparkService(DBService):

    @property
    def spark(self):
        raise NotImplementedError

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

    def source_csv_exists(self, source_name: str):
        raise NotImplementedError

    def table_delta_path(self, sqlalchemy_table):
        return (
            '/tmp'
            f'/{self.config.schema}'
            f'/{sqlalchemy_table.name}'
        )

    def create_table(self, sqlalchemy_table):
        self.spark.sql(self.compile_delta_lake(sqlalchemy_table))

    def create_tables(self, sqlalchemy_tables):
        for table in sqlalchemy_tables:
            self.create_table(table)

    def deploy(self):
        self.engine.deploy()

    def execute(self, sql):
        try:
            return self.spark.sql(sql).toPandas()
        except Exception as e:
            raise Exception(
                f"""
                Config dump:
                {self.config}

                Error while trying to run script:
                {sql}
                """ + str(e)
            )

    def drop_schema(self):
        self.execute(
            f'DROP DATABASE `{self.config.schema}` CASCADE'
        )

    def create_schema(self):
        self.execute(
            f'CREATE DATABASE `{self.config.schema}`'
        )

    @property
    def schema_empty(self):
        return not any([
            row
            for row in self.execute(
                f'SHOW TABLES IN `{self.config.schema}`'
            )
        ])

    @property
    def schema_exists(self):
        return any([
            database == self.config.schema
            for database in self.execute('SHOW DATABASES')['namespace']
        ])

    def table_exists(self, table_name):
        return any([
            table == table_name
            for table in self.execute(
                f'SHOW TABLES IN `{self.config.schema}`')['tableName']
        ])

    def column_exists(self, table_name, column_name):
        return any([
            column == column_name
            for column in self.execute(
                f'DESCRIBE FORMATTED `{self.config.schema}`.`{table_name}`')['col_name']
        ])

    def sql_query_single_value(self, sql):
        return self.execute(sql).iloc[0, 0]

    def test(self):
        assert self.sql_query_single_value("SELECT 1") == 1
        return True

    def execute_sql_element(self, sql_element, async_cursor=False):
        return self.execute(
            self.compile_delta_lake(sql_element)
        )

    def execute_sql_elements_async(self, sql_elements):
        # Async not implemented!
        if type(sql_elements) is dict:
            jobs = sql_elements
        else:
            jobs = {
                self.sql_script_filename(sql_element): sql_element
                for sql_element in sql_elements
            }
        for job in jobs.values():
            self.execute_sql_element(job)
