from __future__ import annotations

from typing import Tuple, Optional, Dict, List, Any

import semver

from datetime import datetime
from itertools import groupby

from jetavator import __version__

from .VaultObject import VaultObject, VaultObjectKey
from .VaultObjectCollection import VaultObjectMapping
from .YamlProjectLoader import YamlProjectLoader
from .ProjectABC import ProjectABC

from .sqlalchemy_tables import Deployment, ObjectDefinition


class Project(VaultObjectMapping, ProjectABC):
    _vault_objects: Dict[Tuple[str, str], VaultObject] = None
    _sqlalchemy_object = None

    def __init__(
            self,
            object_definitions: List[ObjectDefinition],
            sqlalchemy_object: Deployment
    ) -> None:
        super().__init__(
            VaultObject.subclass_instance(self, x)
            for x in object_definitions
        )
        self._sqlalchemy_object = sqlalchemy_object
        for vault_object in self.values():
            vault_object.validate()

    @classmethod
    def from_directory(
            cls,
            directory_path: str
    ) -> Project:

        file_dicts = YamlProjectLoader(directory_path).load_files()

        projects = [x for x in file_dicts if x["type"] == "project"]
        non_projects = [x for x in file_dicts if x["type"] != "project"]

        if len(projects) > 1:
            raise Exception(
                "Cannot have multiple YAML definitions of type 'project'")
        if len(projects) == 0:
            raise Exception("Deployment failed due to missing project.yaml")

        sqlalchemy_object = Deployment(
            name=projects[0]["name"],
            version=projects[0]["version"])

        return cls(
            object_definitions=[
                ObjectDefinition.from_dict(sqlalchemy_object, definition_dict)
                for definition_dict in non_projects
            ],
            sqlalchemy_object=sqlalchemy_object)

    @classmethod
    def from_sqlalchemy_object(
            cls,
            sqlalchemy_object: Deployment
    ) -> Project:
        return cls(
            object_definitions=sqlalchemy_object.object_definitions,
            sqlalchemy_object=sqlalchemy_object)

    def add(
            self,
            definition_dict: Dict[str, Any]
    ) -> VaultObject:
        key = (definition_dict["type"], definition_dict["name"])

        if key in self._vault_objects:
            raise Exception(f"Table definition {key} already exists.")

        self._vault_objects[key] = self._vault_object_from_dict(
            definition_dict)

        return self._vault_objects[key]

    def delete(
            self,
            object_type: str,
            object_name: str
    ) -> None:
        key = VaultObjectKey(object_type, object_name)
        if key not in self._vault_objects:
            raise Exception(
                f"""
                Cannot delete the table {key} as it does not exist.
                """
            )
        table = self._vault_objects[key]
        if table.dependent_satellites:
            raise Exception(
                f"""
                Cannot delete table that has dependencies.
                The following satellites are dependent on the table {key}:
                {[satellite.name for satellite in table.dependent_satellites]}
                """
            )
        del self._vault_objects[key]

    def export_sqlalchemy_object(self) -> Deployment:
        self._sqlalchemy_object.jetavator_version = __version__
        self._sqlalchemy_object.checksum = self.checksum
        self._sqlalchemy_object.deploy_dt = str(datetime.now())
        return self._sqlalchemy_object

    @property
    def checksum(self) -> str:
        return ObjectDefinition.dict_checksum({
            f"{x.type}.{x.name}": x.definition
            for x in self._vault_objects.values()
            if x.type != "project"
        })

    @property
    def version(self) -> semver.VersionInfo:
        return semver.VersionInfo.parse(self._sqlalchemy_object.version)

    @property
    def name(self) -> str:
        return self._sqlalchemy_object.name

    @property
    def deployed_time(self) -> datetime:
        return self._sqlalchemy_object.deploy_dt

    @property
    def is_latest_version(self) -> bool:
        if self._sqlalchemy_object:
            return self._sqlalchemy_object.is_latest
        else:
            raise RuntimeError(
                'Cannot check latest_version as underlying database '
                'object is not present.'
            )

    def increment_version(self, new_version: Optional[str] = None) -> None:
        self._sqlalchemy_object = Deployment(
            name=self._sqlalchemy_object.name,
            version=(new_version or semver.bump_patch(self._sqlalchemy_object.version)))
        self._vault_objects = {
            k: self._vault_object_from_dict(v.definition)
            for k, v in self._vault_objects.items()
        }

    @property
    def valid(self) -> bool:
        return not self._duplicate_satellite_columns

    def _vault_object_from_dict(
            self,
            definition_dict: Dict[str, Any]
    ) -> VaultObject:
        return VaultObject.subclass_instance(
            self,
            ObjectDefinition.from_dict(self._sqlalchemy_object, definition_dict)
        )

    @property
    def _duplicate_satellite_columns(self) -> Dict[str, List[str]]:
        column_groups = {
            name: [x[0] for x in group]
            for name, group in groupby(self._satellite_columns, lambda x: x[1])
        }
        return {
            k: v for k, v in column_groups.items()
            if len(v) > 1
        }

    @property
    def _satellite_columns(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        return {
            (satellite.definition["name"], column_name): column
            for satellite in self.items_by_type(["satellite"]).values()
            for column_name, column in satellite.definition["columns"].items()
        }
