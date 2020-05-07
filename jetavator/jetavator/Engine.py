# TODO: Support all possible operations for upgrading from one version of a project to another.
#       Operations to support:
#           Add source
#           Delete source - requires all satellites referencing it to be deleted
#           Modify source
#           Rename source - requires all satellites referencing it to be modified
#           Add hub
#           Delete hub - requires all links and satellites referencing it to be deleted
#           Modify hub - requires all links and satellites referencing it to be modified
#           Rename hub - requires all links and satellites referencing it to be modified
#           Add link
#           Delete link - requires all satellites referencing it to be deleted
#           Modify link - requires all satellites referencing it to be modified
#           Rename link - requires all satellites referencing it to be modified
#           Add satellite
#           Delete satellite - requires all satellites referencing it as a dependency to be deleted
#           Modify satellite - requires all satellites referencing it as a dependency to be modified
#           Rename satellite - requires all satellites referencing it as a dependency to be modified
#           Backfill satellite to date (by calculating)
#           Backfill satellite to date (by copying from previous version)

import os
from enum import Enum
from functools import wraps
from logging import Logger
from typing import Union, List, Dict, Callable

import pandas
import yaml
from lazy_property import LazyProperty

from .runners import Runner
from .config import Config
from .schema_registry import Project
from .schema_registry import SchemaRegistry
from .VaultAction import VaultAction
from .services import Service, DBService
from .sql_model import ProjectModel

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
    """The core Jetavator engine. Pass this object a valid engine
    configuration, and use it to perform operations like deploying a project
    or loading new data.

    :param config: A `jetavator.Config` object containing the desired
                   configuration for the Engine
    """

    def __init__(
            self,
            config: Config
    ) -> None:
        """Default constructor
        """
        self.config = config

    @property
    def compute_service(self) -> DBService:
        """The storage service used for computation
        """
        return self.services[self.config.compute]

    @property
    def source_storage_service(self) -> DBService:
        """The storage service used for the source layer
        """
        return self.services[self.config.storage.source]

    @property
    def vault_storage_service(self) -> DBService:
        """The storage service used for the data vault layer
        """
        return self.services[self.config.storage.vault]

    @property
    def star_storage_service(self) -> DBService:
        """The storage service used for the star schema layer
        """
        return self.services[self.config.storage.star]

    @property
    def logs_storage_service(self) -> DBService:
        """The storage service used for logs
        """
        return self.services[self.config.storage.logs]

    @property
    def db_services(self) -> Dict[str, DBService]:
        """All the registered database services in the engine config

        :return: A dictionary of class:`jetavator.services.DBService` by name
        """
        return {
            k: v
            for k, v in self.services.items()
            if isinstance(v, DBService)
        }

    # TODO: Engine.drop_schemas be moved to Project if the schema to drop
    #       is specific to a Project?
    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        for service in self.db_services.values():
            if service.schema_exists:
                service.drop_schema()

    # TODO: Should <object>.logger be part of the public API?
    @property
    def logger(self) -> Logger:
        """Python logging service to receive log messages
        """
        return self.compute_service.logger

    @LazyProperty
    def services(self) -> Dict[str, Service]:
        """All the registered services as defined in the engine config

        :return: A dictionary of class:`jetavator.services.Service` by name
        """
        return {
            name: Service.from_config(self, config)
            for name, config in self.config.services.items()
        }

    # TODO: The role of SchemaRegistry is unclear. Can we refactor its
    #       responsibilities into Engine and Project?
    @LazyProperty
    def schema_registry(self) -> SchemaRegistry:
        return SchemaRegistry(self.config, self.compute_service)

    # TODO: See above - unclear how this relates to SchemaRegistry
    @LazyProperty
    def sql_model(self) -> ProjectModel:
        return ProjectModel(
            self.config,
            self.compute_service,
            self.schema_registry.loaded,
            self.schema_registry.deployed
        )

    # TODO: Is there any reason for an Engine only to have one project?
    #       Should this be Engine.projects?
    @property
    def project(self) -> Project:
        return self.schema_registry.deployed

    # TODO: project_history is redundant - can this or schema_registry
    #       be deprecated?
    @property
    def project_history(self) -> SchemaRegistry:
        return self.schema_registry

    @logged
    def deploy(self) -> None:
        """Deploy the current project to the configured storage services
        """
        self.logger.info('Testing database connection')
        self.compute_service.test()
        if self.compute_service.schema_exists:
            if self.config.drop_schema_if_exists:
                self.logger.info('Dropping and recreating database')
                self.compute_service.drop_schema()
                self.compute_service.create_schema()
            elif (
                    not self.compute_service.schema_empty
                    and not self.config.skip_deploy
            ):
                raise Exception(
                    f"Database {self.config.schema} already exists, "
                    "is not empty, and config.drop_schema_if_exists "
                    "is set to False."
                )
        else:
            self.logger.info(f'Creating database {self.config.schema}')
            self.compute_service.create_schema()
        self._deploy_template(action=VaultAction.CREATE)

    @logged
    def run(
            self,
            load_type: LoadType = LoadType.DELTA
    ) -> None:
        """Run the data pipelines for the current project

        :param load_type: A `LoadType` that tells the pipelines how to behave
                          with respect to historic data
        """

        # TODO: Fix full load capability for Spark/Hive
        if load_type == LoadType.FULL:
            raise NotImplementedError("Not implemented in Spark/Hive version.")

        # TODO: Make this platform-independent - currently HIVE specific
        self.compute_service.execute(
            f'USE `{self.config.schema}`'
        )

        # TODO: Test DB connection before execution
        # self.connection.test()
        self.runner.run()

        self.runner.performance_data().to_csv(
            'performance.csv',
            index=False
        )

    # TODO: Reintroduce tests for Engine.add
    # TODO: Refactor new_object so it takes a VaultObject which can then
    #       be extended with more constructors
    # TODO: Move load_full_history functionality to a new VaultObject method
    # TODO: Enforce semver compatibility for user-supplied version strings?
    def add(
            self,
            new_object: Union[str, dict],
            load_full_history: bool = False,
            version: str = None
    ) -> None:
        """Add a new object to the current project

        :param new_object:        The definition of the new object as a
                                  dictionary or valid YAML string
        :param load_full_history: True if the new object should be loaded
                                  with full historic data
        :param version:           New version string for the amended project
                                  (optional - will be auto-incremented if
                                  not supplied)
        """

        if isinstance(new_object, str):
            new_object_dict = yaml.safe_load(new_object)
        elif isinstance(new_object, dict):
            new_object_dict = new_object
        else:
            raise Exception("Jetavator.add: new_object must be str or dict.")

        self.schema_registry.loaded.increment_version(version)
        new_vault_object = self.schema_registry.loaded.add(new_object_dict)
        self._update_database_model()

        if new_vault_object.type == "satellite":
            self.compute_service.execute_sql_element(
                new_vault_object.sql_model.sp_load("full").execute())

    # TODO: Move Engine.drop to VaultObject.drop in order to allow
    #       multiple lookup methods, e.g. by type+name, composite key,
    #       iteration through a list of VaultObjects? Possibly decouple
    #       from the version increment to give users more control?
    def drop(
            self,
            object_type: str,
            object_name: str,
            version: str = None
    ) -> None:
        """Drop an object from the current project

        :param object_type:       Type of the object to drop e.g. 'source'
        :param object_name:       Name of the object to drop
                                  with full historic data
        :param version:           New version string for the amended project
                                  (optional - will be auto-incremented if
                                  not supplied)
        """
        self.schema_registry.load_from_database()
        self.schema_registry.loaded.increment_version(version)
        self.schema_registry.loaded.delete(object_type, object_name)
        self._update_database_model()

    # TODO: Refactor so the Engine doesn't need physical disk paths for
    #       the YAML folder - move this functionality into an extendable
    #       loader object elsewhere. Instead pass in a whole project object.
    def update(
            self,
            model_path: str = None,
            load_full_history: bool = False
    ) -> None:
        """Updates the current project with a new set of object definitions
        from disk

        :param model_path:        Path on disk to load the new project
                                  definition (optional, will use the existing
                                  configured path if not supplied)
        :param load_full_history: True if the new object(s) should be loaded
                                  with full historic data
        """
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
        self._update_database_model(
            load_full_history=load_full_history
        )

    # TODO: Review if Engine.table_dtypes is still needed. Refactor it to
    #       somewhere more appropriate (Source?) if it is, deprecate it if not
    def table_dtypes(
            self,
            table_name: str,
            registry: Project = None  # rename this to project?
    ) -> Dict[str, str]:
        """Generates a set of pandas dtypes for a given named Source

        :param table_name: Name of the Source object to generate dtypes for
        """
        registry = registry or self.schema_registry.loaded
        try:
            user_defined_dtypes = {
                k: PANDAS_DTYPE_MAPPINGS[v["type"].split("(")[0].lower()]
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
            registry: Project = None  # rename this to project?
    ) -> pandas.DataFrame:
        """Loads a CSV file from disk and into a compatible pandas `DataFrame`
        """
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
        """Constructs a valid Source definition from a pandas dataframe

        :param df:         Dataframe to construct Source definition for
        :param table_name: Name of the new Source object to construct
        :param use_as_pk:  Column name to use as the primary key
        """

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
        """Adds a pandas dataframe as a Source for this project

        :param df:         Dataframe to add as a Source
        :param table_name: Name of the new Source object to construct
        :param use_as_pk:  Column name to use as the primary key
        """
        raise NotImplementedError

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
        """Loads a list of CSV files into a single named Source

        :param table_name: Name of the new Source object to load
        :param csv_files:  List of paths on disk of the CSV files
        """
        # if assume_schema_integrity:
        #     source = self.schema_registry.loaded["source", table_name]
        #     self.spark_runner.load_csv(csv_file, source)
        # else:
        self.compute_service.load_dataframe(
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
        """Loads a folder of CSV files into a set of Sources. The CSV files
        must each be named <source>.csv where <source> is a valid Source name

        :param folder_path:  Folder path on disk containing the CSV files
        """
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
        raise NotImplementedError

    # TODO: Deprecate Engine.sql_query_single_value
    def sql_query_single_value(
            self,
            sql: str
    ):
        raise NotImplementedError

    # TODO: Investigate if Engine.get_performance_data is still needed
    #       and deprecate it if not
    def get_performance_data(self):
        raise NotImplementedError

    def _update_database_model(
            self,
            load_full_history: bool = False
    ) -> None:
        self._deploy_template(action=VaultAction.ALTER)
        if load_full_history:
            raise NotImplementedError

    # TODO: Refactor responsibility for loading YAML files away from Engine.
    def update_model_from_dir(
            self,
            new_model_path: str = None
    ) -> None:
        if new_model_path:
            self.config.model_path = new_model_path
        self.schema_registry.load_from_disk()

    @LazyProperty
    def runner(self) -> Runner:
        return Runner.from_compute_service(
            self,
            self.compute_service,
            self.schema_registry.loaded
        )

    # TODO: Do we need both clear_database and drop_schemas?
    def clear_database(self) -> None:
        self.logger.info('Starting: clear_database')
        for table in self.sql_model.tables:
            self.compute_service.write_empty_table(table, overwrite_schema=False)
        self.logger.info('Finished: clear_database')

    # TODO: Make action an enum?
    def _deploy_template(
            self,
            action: VaultAction
    ) -> None:

        if not self.schema_registry.loaded.valid:
            raise Exception(self.schema_registry.loaded.validation_error)

        # self.connection.reset_metadata()

        self.logger.info(f'Creating tables...')

        self.vault_storage_service.create_tables(
            self.sql_model.create_tables_ddl()
        )

        self.logger.info(f'Creating history views...')

        self.vault_storage_service.execute_sql_elements_async(
            self.sql_model.create_history_views()
        )

        self.logger.info(f'Creating current views...')

        self.vault_storage_service.execute_sql_elements_async(
            self.sql_model.create_current_views()
        )

        # self.logger.info(f'Updating schema registry...')
        # self.schema_registry.write_definitions_to_sql()

        # Using Delta tables for this doesn't really make sense!
        # We need a better way of storing the application state and
        # deployment history, like writing JSON direct to DBFS
