from typing import Dict, Any
from abc import ABC, abstractmethod

from lazy_property import LazyProperty

from jetavator.EngineABC import EngineABC
from .StorageService import StorageService

from .ComputeServiceABC import ComputeServiceABC
from .ExecutesSQL import ExecutesSQL


class ComputeService(ComputeServiceABC, ExecutesSQL, ABC):

    @property
    def engine(self) -> EngineABC:
        return self.owner

    @LazyProperty
    def storage_services(self) -> Dict[str, StorageService]:
        """All the registered storage services as defined in the compute service config

        :return: A dictionary of class:`jetavator.services.StorageService` by name
        """
        return {
            name: StorageService.from_config(self, config)
            for name, config in self.config.storage_services.items()
        }

    @property
    def vault_storage_service(self) -> StorageService:
        """The storage service used for the data vault layer
        """
        return self.storage_services[self.config.storage.vault]

    @property
    def star_storage_service(self) -> StorageService:
        """The storage service used for the star schema layer
        """
        return self.storage_services[self.config.storage.star]
    
    def create_schemas(self) -> None:
        self.create_schema_if_missing()
        for storage_service in self.storage_services.values():
            storage_service.create_schema_if_missing()

    # TODO: Refactor common elements of ComputeService and StorageService.
    #       Does the ComputeService always need a database (e.g. the Hive
    #       metastore?)
    def create_schema_if_missing(self) -> None:
        if self.schema_exists:
            if self.config.drop_schema_if_exists:
                self.logger.info('Dropping and recreating database')
                self.drop_schema()
                self.create_schema()
            elif (
                    not self.schema_empty
                    and not self.engine.config.skip_deploy
            ):
                raise Exception(
                    f"Database {self.config.schema} already exists, "
                    "is not empty, and config.drop_schema_if_exists "
                    "is set to False."
                )
        else:
            self.logger.info(f'Creating database {self.config.schema}')
            self.create_schema()

    # TODO: Engine.drop_schemas be moved to Project if the schema to drop
    #       is specific to a Project?

    def drop_schemas(self) -> None:
        """Drop this project's schema on all storage services
        """
        self.drop_schema()
        for service in self.storage_services.values():
            if service.schema_exists:
                service.drop_schema()

    def test(self) -> None:
        for service in self.storage_services.values():
            service.test()

    @abstractmethod
    def prepare_environment(self) -> None:
        """
        Perform any necessary steps to prepare the compute environment to be used.
        """
        pass

    @abstractmethod
    def source_csv_exists(self, source_name: str) -> bool:
        pass
