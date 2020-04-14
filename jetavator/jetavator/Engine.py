import pandas
import os
from lazy_property import LazyProperty
import tempfile
import numpy as np
import sqlalchemy
import yaml
import datetime

from re import match
from functools import wraps

from . import utils

from .schema_registry import SchemaRegistry

from .config import Config

from .SparkRunner import SparkRunner
from .services import Service, DBService

DEFAULT_NUMERIC = 0
DEFAULT_STRING = ''
DEFAULT_DATE = '1970-01-01'
DEFAULT_TIME = '0000'
DEFAULT_BINARY = bin(0)

TYPE_DEFAULTS = {
    "bigint": DEFAULT_NUMERIC,
    "bit": DEFAULT_NUMERIC,
    "decimal": DEFAULT_NUMERIC,
    "int": DEFAULT_NUMERIC,
    "money": DEFAULT_NUMERIC,
    "numeric": DEFAULT_NUMERIC,
    "smallint": DEFAULT_NUMERIC,
    "smallmoney": DEFAULT_NUMERIC,
    "tinyint": DEFAULT_NUMERIC,
    "float": DEFAULT_NUMERIC,
    "real": DEFAULT_NUMERIC,
    "date": DEFAULT_DATE,
    "datetime": DEFAULT_DATE,
    "datetime2": DEFAULT_DATE,
    "smalldatetime": DEFAULT_DATE,
    "time": DEFAULT_TIME,
    "char": DEFAULT_STRING,
    "text": DEFAULT_STRING,
    "varchar": DEFAULT_STRING,
    "nchar": DEFAULT_STRING,
    "ntext": DEFAULT_STRING,
    "nvarchar": DEFAULT_STRING,
    "binary": DEFAULT_BINARY,
    "varbinary": DEFAULT_BINARY,
    "image": DEFAULT_BINARY
}

PANDAS_DTYPE_MAPPINGS = {
    "bigint": "Int64",
    "bit": "Int64",
    "decimal": "Int64",
    "int": "Int64",
    "money": "float",
    "numeric": "float",
    "smallint": "Int64",
    "smallmoney": "float",
    "tinyint": "Int64",
    "float": "float",
    "real": "float",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "datetime2": "datetime64[ns]",
    "smalldatetime": "datetime64[ns]",
    "time": "timedelta64[ns]",
    "char": "object",
    "text": "object",
    "varchar": "object",
    "nchar": "object",
    "ntext": "object",
    "nvarchar": "object",
    "binary": "object",
    "varbinary": "object",
    "image": "object"
}

PANDAS_TO_SQL_MAPPINGS = {
    "int": "bigint",
    "int32": "bigint",
    "int64": "bigint",
    "bool": "bit",
    "float": "float(53)",
    "float32": "float(53)",
    "float64": "float(53)",
    "datetime64[ns]": "datetime",
    "str": "nvarchar(255)",
    "object": "nvarchar(255)"
}

BASE_DTYPES = {
    "jetavator_load_dt": "datetime64[ns]",
    "jetavator_deleted_ind": "int"
}


def logged(function_to_decorate):
    @wraps(function_to_decorate)
    def decorated_function(self, *args, **kwargs):
        try:
            return function_to_decorate(self, *args, **kwargs)
        except Exception as e:
            self.logger.exception(str(e))
            raise
    return decorated_function


class Engine(object):

    def __init__(
        self,
        config=None,
        **kwargs
    ):
        self.config = config or Config(**kwargs)

    @property
    def connection(self):
        return self.services[self.config.compute]

    @property
    def source_storage_service(self):
        return self.services[self.config.storage.source]

    @property
    def vault_storage_service(self):
        return self.services[self.config.storage.vault]

    @property
    def star_storage_service(self):
        return self.services[self.config.storage.star]

    @property
    def logs_storage_service(self):
        return self.services[self.config.storage.logs]

    @property
    def db_services(self):
        return {
            k: v
            for k, v in self.services.items()
            if isinstance(v, DBService)
        }

    def drop_schemas(self):
        for service in self.db_services.values():
            if service.schema_exists:
                service.drop_schema()

    @property
    def logger(self):
        return self.connection.logger

    @LazyProperty
    def services(self):
        return {
            service_config.name: Service.from_config(self, service_config)
            for service_config in self.config.services
        }

    @LazyProperty
    def schema_registry(self):
        return SchemaRegistry(self.config, self.connection)

    @property
    def sql_model(self):
        return self.schema_registry.changeset.sql_model

    @property
    def project(self):
        return self.schema_registry.deployed

    @property
    def project_history(self):
        return self.schema_registry

    @logged
    def deploy(self):
        self.generate_database()

    @logged
    def run(self, load_type="delta"):
        if load_type not in ("delta", "full"):
            raise ValueError("load_type must be either 'delta' or 'full'")

        # Fix this later?
        if load_type == "full":
            raise NotImplementedError("Not implemented in Databricks version.")

        # TODO: Make this platform-independent - currently HIVE specific
        self.connection.execute(
            f'USE `{self.config.schema}`'
        )

        # self.connection.test()
        self.spark_runner.run()

        self.spark_runner.performance_data().to_csv(
            'performance.csv',
            index=False
        )

        # if return_val != 0:
        #     raise RuntimeError(
        #         f"Call to Jetavator.run failed with return code {return_val}. "
        #         "Changes rolled back. "
        #         "Source tables emptied and dumped to source_error schema.")

    def add(self, new_object, load_full_history=False, version=None):

        if isinstance(new_object, str):
            new_object_dict = yaml.load(new_object)
        elif isinstance(new_object, dict):
            new_object_dict = new_object
        else:
            raise Exception("Jetavator.add: new_object must be str or dict.")

        self.schema_registry.loaded.increment_version(version)
        new_vault_object = self.schema_registry.loaded.add(new_object_dict)
        self.update_database_model()

        if new_vault_object.type == "satellite":
            self.connection.execute_sql_element(
                new_vault_object.sql_model.sp_load("full").execute())

    def drop(self, object_type, object_name, version=None):
        self.schema_registry.load_from_database()
        self.schema_registry.loaded.increment_version(version)
        self.schema_registry.loaded.delete(object_type, object_name)
        self.update_database_model()

    def update(self, model_dir=None, load_full_history=False):
        if model_dir:
            self.config.model_path = model_dir
        self.schema_registry.load_from_disk()
        assert (
                self.schema_registry.loaded.checksum
                != self.schema_registry.deployed.checksum
        ), (
            f"""
            Cannot upgrade a project if the definitions have not changed.
            Checksum: {self.schema_registry.loaded.checksum.hex()}
            """
        )
        assert (
                self.schema_registry.loaded.version
                > self.schema_registry.deployed.version
        ), (
            "Cannot upgrade - version number must be incremented "
            f"from {self.schema_registry.deployed.version}"
        )
        self.update_database_model(
            load_full_history=load_full_history
        )

    def table_dtypes(self, table_name, registry=None):
        registry = registry or self.schema_registry.loaded
        try:
            user_defined_dtypes = {
                k: PANDAS_DTYPE_MAPPINGS[utils.sql_basic_type(v["type"])]
                for k, v in registry[
                    "source", table_name
                ].definition[
                    "columns"
                ].items()
            }
        except KeyError:
            raise Exception(f"Table source.{table_name} does not exist.")
        return {
            **BASE_DTYPES,
            **user_defined_dtypes
        }

    def csv_to_dataframe(
            self,
            csv_file,
            table_name,
            registry=None
    ):
        registry = registry or self.schema_registry.loaded
        try:
            return pandas.read_csv(
                csv_file,
                parse_dates=[
                    k
                    for k, v in self.table_dtypes(table_name, registry).items()
                    if v in ["datetime64[ns]", "timedelta64[ns]"]
                ],
                dtype={
                    k: v
                    for k, v in self.table_dtypes(table_name, registry).items()
                    if v not in ["datetime64[ns]", "timedelta64[ns]"]
                }
            )
        except ValueError:
            return pandas.read_csv(
                csv_file,
                parse_dates=[
                    k
                    for k, v in self.table_dtypes(table_name, registry).items()
                    if v in ["datetime64[ns]", "timedelta64[ns]"]
                       and k != "jetavator_load_dt"
                ],
                dtype={
                    k: v
                    for k, v in self.table_dtypes(table_name, registry).items()
                    if v not in ["datetime64[ns]", "timedelta64[ns]"]
                }
            )

    def source_def_from_dataframe(self, df, table_name, use_as_pk):

        pd_dtypes_dict = df.dtypes.apply(lambda x: x.name).to_dict()
        col_dict_list = [
            {
                k: {
                    "type": PANDAS_TO_SQL_MAPPINGS[v],
                    "nullable": False  # in Python e.g. int is not nullable
                }
            }
            for k, v in pd_dtypes_dict.items()
        ]
        col_dict = {k: v for d in col_dict_list for k, v in d.items()}

        col_dict[use_as_pk]["pk"] = True

        source_def = {
            "name": table_name,
            "type": "source",
            "schema": "source",
            "columns": col_dict
        }
        return source_def

    def add_dataframe_as_source(self, df, table_name, use_as_pk):
        source_def = self.source_def_from_dataframe(
            df=df,
            table_name=table_name,
            use_as_pk=use_as_pk
        )
        self.add(
            # This is supposed to fail if table_name already exist in the
            # source schema of the database.
            source_def
        )
        self.insert_to_sql_from_pandas(
            df=df,
            schema="source",
            table_name=table_name,
        )

    def load_csvs(
        self, table_name, csv_files
        # , assume_schema_integrity=False
    ):
        # if assume_schema_integrity:
        #     source = self.schema_registry.loaded["source", table_name]
        #     self.spark_runner.load_csv(csv_file, source)
        # else:
        self.connection.load_dataframe(
            pandas.concat([
                self.csv_to_dataframe(
                    csv_file,
                    table_name,
                    self.schema_registry.loaded
                )
                for csv_file in csv_files
            ]),
            self.schema_registry.loaded["source", table_name]
        )

    def load_csv_folder(self, folder_path):
        assert os.path.isdir(folder_path), (
            f"{folder_path} is not a valid directory."
        )
        for dir_entry in os.scandir(folder_path):
            filename, file_extension = os.path.splitext(dir_entry.name)
            if file_extension == ".csv" and dir_entry.is_file():
                self.load_csvs(filename, [dir_entry.path])

    def call_stored_procedure(self, procedure_name):
        return self.connection.call_stored_procedure(procedure_name)

    def sql_query_single_value(self, sql):
        return self.connection.sql_query_single_value(sql)

    def get_performance_data(self):
        raise NotImplementedError

    def generate_database(self):
        self.logger.info('Testing database connection')
        self.connection.test(master=True)
        if self.connection.schema_exists:
            if self.config.drop_schema_if_exists:
                self.logger.info('Dropping and recreating database')
                self.connection.drop_schema()
                self.connection.create_schema()
            elif (
                not self.connection.schema_empty
                and not self.config.skip_deploy
            ):
                raise Exception(
                    f"Database {self.config.schema} already exists, "
                    "is not empty, and config.drop_schema_if_exists "
                    "is set to False."
                )
        else:
            self.logger.info(f'Creating database {self.config.schema}')
            self.connection.create_schema()
        self._deploy_template(action="create")

    def update_database_model(self, load_full_history=False):
        self._deploy_template(action="alter")
        if load_full_history:
            self.connection.execute_sql_element(
                self.sql_model.sp_jetavator_load_full.execute()
            )

    def update_model_from_dir(self, new_model_path=None):
        if new_model_path:
            self.config.model_path = new_model_path
        self.schema_registry.load_from_disk()

    @LazyProperty
    def spark_runner(self):
        return SparkRunner(
            self,
            self.sql_model.definition
        )

    def clear_database(self):
        self.logger.info('Starting: clear_database')
        for table in self.sql_model.tables:
            self.connection.write_empty_table(table, overwrite_schema=False)
        self.logger.info('Finished: clear_database')

    def _deploy_template(self, action):

        if not self.schema_registry.loaded.valid:
            raise Exception(self.schema_registry.loaded.validation_error)

        # self.connection.reset_metadata()

        self.logger.info(f'Creating tables...')

        self.vault_storage_service.create_tables(
            self.sql_model.create_tables_ddl(action)
        )

        self.logger.info(f'Creating history views...')

        self.vault_storage_service.execute_sql_elements_async(
            self.sql_model.create_history_views(action)
        )

        self.logger.info(f'Creating current views...')

        self.vault_storage_service.execute_sql_elements_async(
            self.sql_model.create_current_views(action)
        )

        # self.logger.info(f'Updating schema registry...')
        # self.schema_registry.write_definitions_to_sql()

        # Using Delta tables for this doesn't really make sense!
        # We need a better way of storing the application state and
        # deployment history, like writing JSON direct to DBFS
