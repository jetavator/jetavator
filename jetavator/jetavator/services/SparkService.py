import datetime
import logging
import logging.config
import os
import tempfile
from shutil import copyfile

import numpy as np
import pyspark
import sqlalchemy
import sqlparse
from jetavator.sqlalchemy_delta import DeltaDialect
from lazy_property import LazyProperty
from pyspark.sql import SparkSession

from jetavator import utils
from .DBService import DBService

SPARK_APP_NAME = 'jetavator'
DELTA_VERSION = 'delta-core_2.11:0.5.0'
DBFS_DATA_ROOT = 'dbfs:/jetavator/data'

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
    def logger_config(self):
        return {
            'version': 1,
            'formatters': {
                'simple': {
                    'format': '%(asctime)s %(message)s',
                }
            },
            'handlers': {
                'console': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
            },
            'loggers': {
                'jetavator': {
                    'handlers': ['console'],
                    'level': 'DEBUG',
                },
            }
        }

    @LazyProperty
    def logger(self):
        logging.config.dictConfig(self.logger_config)
        return logging.getLogger('jetavator')

    # In future, refactor elsewhere to separate sqlalchemy and spark concerns
    @LazyProperty
    def metadata(self):
        return sqlalchemy.MetaData()

    @property
    def spark(self):
        raise NotImplementedError

    def compile_sqlalchemy(self, sqlalchemy_executable):
        try:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=DeltaDialect(),
                    compile_kwargs={"literal_binds": True}
                )),
                reindent=True,
                keyword_case='upper'
            )
        except Exception as e:
            formatted = sqlparse.format(
                str(sqlalchemy_executable.compile(
                    dialect=DeltaDialect()
                )),
                reindent=True,
                keyword_case='upper'
            )
        return formatted

    def load_dataframe(self, dataframe, source):
        for column in source.columns.keys():
            if column not in dataframe.columns:
                dataframe[column] = np.nan
        if 'jetavator_load_dt' not in dataframe.columns:
            dataframe['jetavator_load_dt'] = datetime.datetime.now()
        if 'jetavator_deleted_ind' not in dataframe.columns:
            dataframe['jetavator_deleted_ind'] = 0
        columns = list(source.columns.keys()) + [
            'jetavator_load_dt',
            'jetavator_deleted_ind'
        ]
        filename = f'{source.name}.csv'
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
            self.load_csv(temp_csv_file, source)

    def load_csv(self, csv_file, source):
        raise NotImplementedError

    def csv_file_path(self, source):
        raise NotImplementedError

    def source_csv_exists(self, source):
        raise NotImplementedError

    def table_delta_path(self, sqlalchemy_table):
        return (
            '/tmp'
            f'/{self.config.schema}'
            f'/{sqlalchemy_table.name}'
        )

    def write_empty_table(self, sqlalchemy_table, overwrite_schema=True):
        (
            self.spark
            .createDataFrame(
                [],
                pyspark.sql.types.StructType([
                    pyspark.sql.types.StructField(
                        column.name,
                        pyspark_column_type(column),
                        True
                    )
                    for column in sqlalchemy_table.columns
                ])
            )
            .write
            .format('delta')
            .mode('overwrite')
            .option(
                'overwriteSchema',
                ('true' if overwrite_schema else 'false')
            )
            .save(self.table_delta_path(sqlalchemy_table))
        )

    def create_table(self, sqlalchemy_table):
        self.spark.sql(
            f'''
            DROP TABLE IF EXISTS
            `{self.config.schema}`.`{sqlalchemy_table.element.name}`
            '''
        )
        self.write_empty_table(sqlalchemy_table.element)
        self.spark.sql(
            f'''
            {self.compile_sqlalchemy(sqlalchemy_table)}
            USING DELTA
            LOCATION "{self.table_delta_path(sqlalchemy_table.element)}"
            '''
        )

    def create_tables(self, sqlalchemy_tables):
        for table in sqlalchemy_tables:
            self.create_table(table)

    def deploy(self):
        self.engine.deploy()

    def execute(self, sql):
        try:
            return self.spark.sql(sql).collect()
        except Exception as e:
            raise Exception(
                f"""
                Config dump:
                {self.config}

                Error while strying to run script:
                {sql}
                """ + str(e)
            )

    def execute_to_pandas(self, sql):
        try:
            return self.spark.sql(sql).toPandas()
        except Exception as e:
            raise Exception(
                f"""
                Config dump:
                {self.config}

                Error while strying to run script:
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
            row['databaseName'] == self.config.schema
            for row in self.execute('SHOW DATABASES')
        ])

    def table_exists(self, table_name):
        return any([
            row['tableName'] == table_name
            for row in self.execute(
                f'SHOW TABLES IN `{self.config.schema}`')
        ])

    def column_exists(self, table_name, column_name):
        return any([
            row['col_name'] == column_name
            for row in self.execute(
                f'DESCRIBE FORMATTED `{self.config.schema}`.`{table_name}`')
        ])

    def sql_query_single_value(self, sql):
        return self.execute(sql)[0][0]

    def test(self):
        assert self.execute("SELECT 1")[0][0] == 1
        return True

    def execute_sql_element(self, sql_element, async_cursor=False):
        return self.execute(
            self.compile_sqlalchemy(sql_element)
        )

    def execute_sql_elements_async(self, sql_elements):
        # Async not implemented!
        if type(sql_elements) is dict:
            jobs = sql_elements
        else:
            jobs = {
                utils.sql_script_filename(sql_element): sql_element
                for sql_element in sql_elements
            }
        for job in jobs.values():
            self.execute_sql_element(job)


class LocalSparkService(SparkService, register_as="local_spark"):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tempfolder = '/jetavator/data'
        # Figure out a better way to manage temporary folders -
        # requires storage of state between commands line calls!

    @LazyProperty
    def spark(self):
        os.environ['PYSPARK_SUBMIT_ARGS'] = (
            '--packages'
            f' io.delta:{DELTA_VERSION}'
            ' pyspark-shell'
        )
        spark_session = (
            SparkSession
            .builder
            .appName(SPARK_APP_NAME)
            .enableHiveSupport()
            .getOrCreate()
        )
        spark_session.sparkContext.setLogLevel('ERROR')
        return spark_session

    def csv_file_path(self, source):
        return (
            f'{self.tempfolder}/'
            f'{self.config.schema}/'
            f'{self.engine.config.session.run_uuid}/'
            f'{source.name}.csv'
        )

    def source_csv_exists(self, source):
        return os.path.exists(self.csv_file_path(source))

    def load_csv(self, csv_file, source):
        utils.print_to_console(f"{source.name}.csv: Uploading file")
        try:
            os.makedirs(
                os.path.dirname(
                    self.csv_file_path(source)))
        except FileExistsError:
            pass
        copyfile(csv_file, self.csv_file_path(source))
