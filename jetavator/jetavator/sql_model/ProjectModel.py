from .BaseModel import BaseModel

from ..schema_registry.sqlalchemy_tables import (
    Deployment,
    Log,
    ObjectDefinition,
    ObjectLoad,
    PerformanceLog
)

from sqlalchemy import text, Column, literal_column
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import *

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


class ProjectModel(BaseModel, register_as="project"):

    def create_tables_ddl(self, action):
        files = []

        # if action == "create":
        #     files += self.create_tables(self.registry_tables)

        for satellite_owner in self.satellite_owners:
            files += satellite_owner.sql_model.files

        for satellite in self.definition.satellites.values():
            files += satellite.sql_model.files

        return files

    def create_star_tables_ddl(self, action, with_index=False):
        files = []

        for satellite_owner in self.satellite_owners:
            files += satellite_owner.sql_model.star_files(with_index)

        return files

    @property
    def tables(self):
        return [
            satellite.sql_model.table
            for satellite in self.definition.satellites.values()
            if satellite.action != "drop"
        ] + [
            satellite_owner.sql_model.table
            for satellite_owner in self.satellite_owners
        ] + [
            satellite_owner.sql_model.star_table
            for satellite_owner in self.satellite_owners
            if not satellite_owner.exclude_from_star_schema
        ]

    def create_history_views(self, action):
        return [
            view
            for satellite in self.definition.satellites.values()
            for view in satellite.sql_model.history_views
        ]

    def create_current_views(self, action):
        return [
            view
            for satellite in self.definition.satellites.values()
            for view in satellite.sql_model.current_views
        ]

    def dml_scripts(self, action):
        return [
            script
            for satellite in self.definition.satellites.values()
            for script in satellite.sql_model.dml_scripts
        ]

    @property
    def registry_tables(self):
        return [
            Deployment.__table__,
            ObjectDefinition.__table__,
            ObjectLoad.__table__,
            Log.__table__,
            PerformanceLog.__table__
        ]

    @property
    def satellite_owners(self):
        return [
            *self.definition.hubs.values(),
            *self.definition.links.values()
        ]
