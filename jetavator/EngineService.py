from abc import ABC, abstractmethod
from typing import Union

from jetavator.LoadType import LoadType
from jetavator.ServiceOwner import ServiceOwner
from jetavator.DDLDeployer import DDLDeployer
from jetavator.config import EngineServiceConfig
from jetavator.runners import Runner
from jetavator.schema_registry import RegistryService, Project
from jetavator.services import Service, ComputeService
from jetavator.sql_model import ProjectModel


class EngineService(Service[EngineServiceConfig, ServiceOwner], ABC):

    @property
    @abstractmethod
    def compute_service(self) -> ComputeService:
        """The storage service used for computation
        """
        pass

    @abstractmethod
    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        pass

    # TODO: The role of SchemaRegistry is unclear. Can we refactor its
    #       responsibilities into Engine and Project?
    # TODO: Rename schema_registry to something more appropriate?
    @property
    @abstractmethod
    def schema_registry(self) -> RegistryService:
        pass

    @property
    @abstractmethod
    def sql_model(self) -> ProjectModel:
        pass

    # TODO: Is there any reason for an Engine only to have one project?
    #       Should this be Engine.projects?
    @property
    @abstractmethod
    def project(self) -> Project:
        pass

    @abstractmethod
    def deploy(self) -> None:
        """Deploy the current project to the configured storage services
        """
        pass

    @abstractmethod
    def run(
            self,
            load_type: LoadType = LoadType.DELTA
    ) -> None:
        pass

    # TODO: Reintroduce tests for Engine.add
    # TODO: Refactor new_object so it takes a VaultObject which can then
    #       be extended with more constructors
    # TODO: Move load_full_history functionality to a new VaultObject method
    # TODO: Enforce semver compatibility for user-supplied version strings?
    @abstractmethod
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
        pass

    # TODO: Move Engine.drop to VaultObject.drop in order to allow
    #       multiple lookup methods, e.g. by type+name, composite key,
    #       iteration through a list of VaultObjects? Possibly decouple
    #       from the version increment to give users more control?
    @abstractmethod
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
        pass

    # TODO: Refactor so the Engine doesn't need physical disk paths for
    #       the YAML folder - move this functionality into an extendable
    #       loader object elsewhere. Instead pass in a whole project object.
    @abstractmethod
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
        pass

    @property
    @abstractmethod
    def runner(self) -> Runner:
        pass

    @property
    @abstractmethod
    def ddl_deployer(self) -> DDLDeployer:
        pass