from __future__ import annotations

from enum import Enum
from functools import wraps
from typing import Any, Union, Dict, Callable

import logging
import logging.config
import yaml
from lazy_property import LazyProperty

from jetavator.EngineABC import EngineABC
from .runners import Runner
from .config import Config, EngineConfig
from .schema_registry import Project, RegistryService
from .services import ComputeService
from .sql_model import ProjectModel
from .default_logger import DEFAULT_LOGGER_CONFIG
from .DDLDeployer import DDLDeployer

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


class Engine(EngineABC):

    def __init__(
            self,
            app: App,
            config: EngineConfig
    ) -> None:
        """Default constructor
        """
        self.app = app
        self._config = config

    @property
    def config(self) -> EngineConfig:
        return self._config

    @LazyProperty
    def compute_service(self) -> ComputeService:
        """The storage service used for computation
        """
        return ComputeService.from_config(self, self.config.compute)

    # TODO: Engine.drop_schemas be moved to Project if the schema to drop
    #       is specific to a Project?
    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        self.compute_service.drop_schemas()

    @property
    def logger(self) -> logging.Logger:
        return self.app.logger

    @property
    def schema_registry(self) -> RegistryService:
        return self.app.schema_registry

    @LazyProperty
    def sql_model(self) -> ProjectModel:
        return ProjectModel(
            self.app.config,
            self.compute_service,
            self.app.loaded_project,
            self.schema_registry.deployed
        )

    # TODO: Is there any reason for an Engine only to have one project?
    #       Should this be Engine.projects?
    @property
    def project(self) -> Project:
        return self.schema_registry.deployed

    @logged
    def deploy(self) -> None:
        """Deploy the current project to the configured storage services
        """
        self.compute_service.create_schemas()
        self._deploy_template()

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

        self.compute_service.prepare_environment()

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    @LazyProperty
    def runner(self) -> Runner:
        return Runner.from_compute_service(
            self,
            self.compute_service,
            self.app.loaded_project
        )

    @LazyProperty
    def ddl_deployer(self) -> DDLDeployer:
        return DDLDeployer(
            self.sql_model,
            self.compute_service.vault_storage_service
        )

    def _deploy_template(self) -> None:

        if not self.app.loaded_project.valid:
            raise Exception(self.app.loaded_project.validation_error)

        # self.connection.reset_metadata()

        self.ddl_deployer.deploy()

        # self.logger.info(f'Updating schema registry...')
        # self.schema_registry.write_definitions_to_sql()

        # Using Delta tables for this doesn't really make sense!
        # We need a better way of storing the application state and
        # deployment history, like writing JSON direct to DBFS


class App(EngineABC):
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
        self._config = config

    @property
    def config(self) -> Config:
        return self._config

    @LazyProperty
    def engine(self) -> Engine:
        return Engine(self, self.config.engine)

    @LazyProperty
    def loaded_project(self) -> Project:
        """The current project as specified by the YAML files in self.config.model_path
        """
        return Project.from_directory(
            self.config,
            self.engine.compute_service,
            self.config.model_path)

    # TODO: Engine.drop_schemas be moved to Project if the schema to drop
    #       is specific to a Project?
    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        self.engine.drop_schemas()

    @property
    def logger_config(self) -> Dict[str, Any]:
        return DEFAULT_LOGGER_CONFIG

    @LazyProperty
    def logger(self) -> logging.Logger:
        logging.config.dictConfig(self.logger_config)
        return logging.getLogger('jetavator')

    # TODO: The role of SchemaRegistry is unclear. Can we refactor its
    #       responsibilities into Engine and Project?
    # TODO: Rename schema_registry to something more appropriate?
    @LazyProperty
    def schema_registry(self) -> RegistryService:
        return RegistryService.from_config(self, self.config.registry)

    # TODO: Is there any reason for an Engine only to have one project?
    #       Should this be Engine.projects?
    @property
    def project(self) -> Project:
        return self.schema_registry.deployed

    @logged
    def deploy(self) -> None:
        """Deploy the current project to the configured storage services
        """
        self.engine.deploy()

    @logged
    def run(
            self,
            load_type: LoadType = LoadType.DELTA
    ) -> None:
        """Run the data pipelines for the current project

        :param load_type: A `LoadType` that tells the pipelines how to behave
                          with respect to historic data
        """
        self.engine.run(load_type)

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
        self.engine.add(new_object, load_full_history, version)

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
        self.engine.drop(object_type, object_name, version)

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
        self.engine.update(model_path, load_full_history)

    # TODO: Refactor responsibility for loading YAML files away from Engine.
    def update_model_from_dir(
            self,
            new_model_path: str = None
    ) -> None:
        if new_model_path:
            self.config.model_path = new_model_path
        self.schema_registry.load_from_disk()

