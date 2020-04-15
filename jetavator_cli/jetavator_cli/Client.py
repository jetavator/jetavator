import pandas
import os
from lazy_property import LazyProperty
import tempfile
import numpy as np
import sqlalchemy
import yaml
import datetime
import inspect

from re import match

from . import utils

from .config import BaseConfig

import jetavator

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


class Client(object):

    def __init__(
        self,
        config
    ):
        self.engine = jetavator.Engine(config)

    @property
    def config(self):
        return self.engine.config

    @property
    def connection(self):
        return self.engine.compute_service

    @property
    def schema_registry(self):
        return self.engine.schema_registry

    @property
    def sql_model(self):
        return self.schema_registry.changeset.sql_model

    @property
    def project(self):
        return self.schema_registry.deployed

    @property
    def project_history(self):
        return self.schema_registry

    def deploy(self):
        self.connection.deploy()

    # TODO: Deprecate environment_type.
    #       Remove this hard-coded conditional switching and base this on
    #       the Compute setting in the config YAML
    def run(self, load_type="delta"):
        if self.config.environment_type == 'local_spark':
            self.engine.run()
        elif self.config.environment_type == 'remote_databricks':
            self.connection.run_remote()
        else:
            raise NotImplementedError(self.config.environment_type)

    def add(self, new_object, load_full_history=False, version=None):

        if isinstance(new_object, str):
            new_object_dict = yaml.load(new_object)
        elif isinstance(new_object, dict):
            new_object_dict = new_object
        else:
            raise Exception("Client.add: new_object must be str or dict.")

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

    def call_stored_procedure(self, procedure_name):
        return self.connection.call_stored_procedure(procedure_name)

    def sql_query_single_value(self, sql):
        return self.connection.sql_query_single_value(sql)

    def get_performance_data(self):
        raise NotImplementedError

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

    def clear_database(self):
        self.engine.clear_database()

    def build_wheel(self):
        self.config.wheel_path = utils.build_wheel(
            self.config.jetavator_source_path
        )
        self.config.save()

    def _deploy_template(self, action):

        if not self.schema_registry.loaded.valid:
            raise Exception(self.schema_registry.loaded.validation_error)

        self.connection.reset_metadata()

        if not self.config.deploy_scripts_only:

            utils.print_to_console(f'Creating tables...')

            self.connection.execute_sql_elements_async(
                self.sql_model.create_tables(action)
            )

            utils.print_to_console(f'Creating history views...')

            self.connection.execute_sql_elements_async(
                self.sql_model.history_views(action)
            )

            utils.print_to_console(f'Creating current views...')

            self.connection.execute_sql_elements_async(
                self.sql_model.current_views(action)
            )

        else:

            utils.print_to_console(f'Creating tables... [SKIPPED]')
            utils.print_to_console(f'Creating history views... [SKIPPED]')
            utils.print_to_console(f'Creating current views... [SKIPPED]')

        if not self.config.deploy_scripts_only:
            utils.print_to_console(f'Updating schema registry...')
            self.schema_registry.write_definitions_to_sql()
        else:
            utils.print_to_console(f'Updating schema registry... [SKIPPED]')
