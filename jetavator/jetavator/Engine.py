import pandas
import os
import tempfile
import numpy as np
import sqlalchemy
import yaml
import datetime

from typing import Union, List, Dict, Callable

from lazy_property import LazyProperty

from re import match
from functools import wraps

from . import utils

from .schema_registry import SchemaRegistry

from .config import Config
from .schema_registry import Project

from .SparkRunner import SparkRunner
from .services import Service, DBService
from .sql_model import BaseModel

from logging import Logger

from enum import Enum

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


class LoadType(Enum):
    DELTA = 'delta'
    FULL = 'full'


def logged(function_to_decorate: Callable) -> Callable:
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
        config: Config = None,
        **kwargs
    ) -> None:
        self.config = config or Config(**kwargs)

    @property
    def connection(self) -> DBService:
        return self.services[self.config.compute]

    @property
    def source_storage_service(self) -> DBService:
        return self.services[self.config.storage.source]

    @property
    def vault_storage_service(self) -> DBService:
        return self.services[self.config.storage.vault]

    @property
    def star_storage_service(self) -> DBService:
        return self.services[self.config.storage.star]

    @property
    def logs_storage_service(self) -> DBService:
        return self.services[self.config.storage.logs]

    @property
    def db_services(self) -> Dict[str, DBService]:
        return {
            k: v
            for k, v in self.services.items()
            if isinstance(v, DBService)
        }

    def drop_schemas(self) -> None:
        for service in self.db_services.values():
            if service.schema_exists:
                service.drop_schema()

    @property
    def logger(self) -> Logger:
        return self.connection.logger

    @LazyProperty
    def services(self) -> Dict[str, Service]:
        return {
            service_config.name: Service.from_config(self, service_config)
            for service_config in self.config.services
        }

    @LazyProperty
    def schema_registry(self) -> SchemaRegistry:
        return SchemaRegistry(self.config, self.connection)

    @property
    def sql_model(self) -> BaseModel:
        return self.schema_registry.changeset.sql_model

    @property
    def project(self) -> Project:
        return self.schema_registry.deployed

    # TODO: project_history is reduntant - can this or schema_registry
    #       be deprecated?
    @property
    def project_history(self) -> SchemaRegistry:
        return self.schema_registry

    @logged
    def deploy(self) -> None:
        self.generate_database()

    @logged
    def run(
        self,
        load_type: LoadType = LoadType.DELTA
    ) -> None:

        # TODO: Fix full load capability for Spark/Hive
        if load_type == LoadType.FULL:
            raise NotImplementedError("Not implemented in Databricks version.")

        # TODO: Make this platform-independent - currently HIVE specific
        self.connection.execute(
            f'USE `{self.config.schema}`'
        )

        # TODO: Test DB connection before execution
        # self.connection.test()
        self.spark_runner.run()

        self.spark_runner.performance_data().to_csv(
            'performance.csv',
            index=False
        )

    # TODO: Reintroduce tests for Engine.add
    # TODO: Refactor new_object so it takes a VaultObject which can then
    #       be extended with more constructors
    def add(
        self,
        new_object: Union[str, dict],
        load_full_history: bool = False,
        version: str = None
    ) -> None:

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

    # TODO: Move Engine.drop to VaultObject.drop in order to allow
    #       multiple lookup methods, e.g. by type+name, composite key,
    #       iteration through a list of VaultObjects - possibly decouple
    #       from the version increment to give users more control?
    def drop(
        self,
        object_type: str,
        object_name: str,
        version: str = None
    ) -> None:
        self.schema_registry.load_from_database()
        self.schema_registry.loaded.increment_version(version)
        self.schema_registry.loaded.delete(object_type, object_name)
        self.update_database_model()

    # TODO: Refactor so the Engine doesn't need physical disk paths for
    #       the YAML folder - move this functionality into an extendable
    #       loader object elsewhere. Instead pass in a whole project object.
    def update(
        self,
        model_path: str = None,
        load_full_history: bool = False
    ) -> None:
        if model_path:
            self.config.model_path = model_path
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

    # TODO: Review if Engine.table_dtypes is still needed. Refactor it to
    #       somewhere more appropriate (Source?) if it is, deprecate it if not
    def table_dtypes(
        self,
        table_name: str,
        registry: Project = None   # rename this to project?
    ) -> Dict[str, str]:
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

    # TODO: Refactor file loading responsibilities out of Engine
    # TODO: DRY in the try/except block of Engine.csv_to_dataframe
    def csv_to_dataframe(
        self,
        csv_file: str,
        table_name: str,
        registry: Project = None   # rename this to project?
    ) -> pandas.DataFrame :
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

    # TODO: Refactor Engine.source_def_from_dataframe
    #       as a constructor for the Source object
    # TODO: Nothing should ever return a type signature this complex!
    #       It should be an object instead
    def source_def_from_dataframe(
        self,
        df: pandas.DataFrame,
        table_name: str,
        use_as_pk: str
    ) -> Dict[str, Union[str, Dict[str, Dict[str, Union[str, bool]]]]]:

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

    # TODO: Deprecate after Engine.source_def_from_dataframe has been added
    #       as a constructor for the Source object
    def add_dataframe_as_source(
        self,
        df: pandas.DataFrame,
        table_name: str,
        use_as_pk: str
    ) -> None:
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

    # TODO: Refactor responsibility for loading CSV files away from
    #       Engine. Tie this directly to the Source object so we don't force a
    #       particular lookup method here.
    # TODO: Re-implement assume_schema_integrity for loading CSVs
    #       (perhaps with a header line safety check!)
    def load_csvs(
        self,
        table_name: str,
        csv_files: List[str]
        # , assume_schema_integrity=False
    ) -> None:
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

    # TODO: Refactor responsibility for loading CSV files away from
    #       Engine. For Engine.load_csv_folder, these should be two methods
    #       of an extendable loader class (or possibly subclasses?)
    def load_csv_folder(
        self,
        folder_path: str
    ) -> None:
        assert os.path.isdir(folder_path), (
            f"{folder_path} is not a valid directory."
        )
        for dir_entry in os.scandir(folder_path):
            filename, file_extension = os.path.splitext(dir_entry.name)
            if file_extension == ".csv" and dir_entry.is_file():
                self.load_csvs(filename, [dir_entry.path])

    # TODO: Deprecate Engine.call_stored_procedure as it's specific to MSSQL
    def call_stored_procedure(
        self,
        procedure_name: str
    ):
        return self.connection.call_stored_procedure(procedure_name)

    # TODO: Deprecate Engine.sql_query_single_value
    def sql_query_single_value(
        self,
        sql: str
    ):
        return self.connection.sql_query_single_value(sql)

    # TODO: Investigate if Engine.get_performance_data is still needed
    #       and deprecate it if not
    def get_performance_data(self):
        raise NotImplementedError

    # TODO: Engine.generate_database doesn't belong as a public method
    def generate_database(self) -> None:
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

    # TODO: Engine.update_database_model doesn't belong as a public method
    def update_database_model(
        self,
        load_full_history: bool=False
    ) -> None:
        self._deploy_template(action="alter")
        if load_full_history:
            self.connection.execute_sql_element(
                self.sql_model.sp_jetavator_load_full.execute()
            )

    # TODO: Refactor responsibility for loading YAML files away from Engine.
    def update_model_from_dir(
        self,
        new_model_path: str = None
    ) -> None:
        if new_model_path:
            self.config.model_path = new_model_path
        self.schema_registry.load_from_disk()

    # TODO: Refactor Engine.spark_runner to make Runner a generic interface
    @LazyProperty
    def spark_runner(self) -> SparkRunner:
        return SparkRunner(
            self,
            self.sql_model.definition
        )

    def clear_database(self) -> None:
        self.logger.info('Starting: clear_database')
        for table in self.sql_model.tables:
            self.connection.write_empty_table(table, overwrite_schema=False)
        self.logger.info('Finished: clear_database')

    # TODO: Make action an enum?
    def _deploy_template(
        self,
        action: str
    ) -> None:

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