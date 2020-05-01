from typing import List

from sqlalchemy.schema import DDLElement

from jetavator.config import Config
from jetavator.services import DBService
from jetavator.schema_registry import Project, VaultObjectMapping

from .BaseModel import BaseModel

SCHEMAS = [
    "jetavator",
    "source",
    "source_history",
    "source_error",
    "source_updates",
    "vault",
    "vault_history",
    "vault_updates",
    "vault_now",
    "star",
]


class ProjectModel(VaultObjectMapping[BaseModel]):

    def __init__(
            self,
            config: Config,
            compute_service: DBService,
            new_definition: Project,
            old_definition: Project
    ) -> None:
        super().__init__()
        self.config = config
        self.compute_service = compute_service
        self.new_definition = new_definition
        self.old_definition = old_definition
        keys = (
                set(self.new_definition.keys()) |
                set(self.old_definition.keys())
        )
        self._data = {
            key: BaseModel.subclass_instance(
                self,
                self.new_definition.get(key),
                self.old_definition.get(key)
            )
            for key in keys
        }

    def create_tables_ddl(self) -> List[DDLElement]:
        files = []

        for satellite_owner_model in self.satellite_owners.values():
            files += satellite_owner_model.files

        for satellite_model in self.satellites.values():
            files += satellite_model.files

        return files

    def create_history_views(self) -> List[DDLElement]:
        return [
            view
            for satellite_model in self.satellites.values()
            for view in satellite_model.history_views
        ]

    def create_current_views(self) -> List[DDLElement]:
        return [
            view
            for satellite_model in self.satellites.values()
            for view in satellite_model.current_views
        ]
