import logging
from typing import Union

from lazy_property import LazyProperty

from jetavator import App, LoadType
from jetavator.logged import logged
from jetavator.EngineService import EngineService
from jetavator.DDLDeployer import DDLDeployer
from jetavator.config import LocalEngineServiceConfig
from jetavator.runners import Runner
from jetavator.schema_registry import RegistryService, Project
from jetavator.services import Service, ComputeService
from jetavator.sql_model import ProjectModel


class LocalEngineService(EngineService, Service[LocalEngineServiceConfig, App], register_as="local"):

    @LazyProperty
    def compute_service(self) -> ComputeService:
        """The storage service used for computation
        """
        return ComputeService.from_config(self.config.compute, self)

    # TODO: Engine.drop_schemas be moved to Project if the schema to drop
    #       is specific to a Project?
    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        self.compute_service.drop_schemas()

    @property
    def logger(self) -> logging.Logger:
        return self.owner.logger

    # TODO: The role of SchemaRegistry is unclear. Can we refactor its
    #       responsibilities into Engine and Project?
    # TODO: Rename schema_registry to something more appropriate?
    @LazyProperty
    def schema_registry(self) -> RegistryService:
        return RegistryService.from_config(self.config.registry, self)

    @LazyProperty
    def sql_model(self) -> ProjectModel:
        return ProjectModel(
            self.owner.config,
            self.compute_service.vault_storage_service.config.schema,
            self.compute_service.star_storage_service.config.schema,
            self.owner.loaded_project,
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
        return Runner.from_config(
            self.config.compute.runner,
            self.compute_service,
            self.owner.loaded_project
        )

    @LazyProperty
    def ddl_deployer(self) -> DDLDeployer:
        return DDLDeployer(
            self.sql_model,
            self.compute_service.vault_storage_service
        )

    def _deploy_template(self) -> None:

        if not self.owner.loaded_project.valid:
            raise Exception(self.owner.loaded_project.validation_error)

        # self.connection.reset_metadata()

        self.ddl_deployer.deploy()

        # self.logger.info(f'Updating schema registry...')
        # self.schema_registry.write_definitions_to_sql()

        # Using Delta tables for this doesn't really make sense!
        # We need a better way of storing the application state and
        # deployment history, like writing JSON direct to DBFS