import semver
from typing import List

from lazy_property import LazyProperty

from sqlalchemy.schema import DDLElement
from sqlalchemy import MetaData

from jetavator.schema_registry import Project

from .SQLModel import SQLModel, SQLModelOwner
from .SatelliteModel import SatelliteModel

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


class ProjectModel(SQLModelOwner):

    def __init__(
            self,
            new_definition: Project,
            old_definition: Project,
            vault_schema: str,
            star_schema: str
    ) -> None:
        super().__init__()
        self.new_definition = new_definition
        self.old_definition = old_definition
        self._vault_schema = vault_schema
        self._star_schema = star_schema
        keys = (
                set(self.new_definition.keys()) |
                set(self.old_definition.keys())
        )
        self._data = {
            key: SQLModel.subclass_instance(
                self,
                self.new_definition.get(key),
                self.old_definition.get(key)
            )
            for key in keys
        }

    @property
    def vault_schema(self) -> str:
        return self._vault_schema

    @property
    def star_schema(self) -> str:
        return self._star_schema

    @property
    def version(self) -> semver.VersionInfo:
        return self.new_definition.version

    @LazyProperty
    def metadata(self) -> MetaData:
        return MetaData()

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
            if isinstance(satellite_model, SatelliteModel)
            for view in satellite_model.history_views
        ]

    def create_current_views(self) -> List[DDLElement]:
        return [
            view
            for satellite_model in self.satellites.values()
            if isinstance(satellite_model, SatelliteModel)
            for view in satellite_model.current_views
        ]
