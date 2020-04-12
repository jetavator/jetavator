import semver

from datetime import datetime
from itertools import groupby
from ast import literal_eval

from ..utils import dict_checksum, load_yamls_in_dir

from ..sql_model.ProjectModel import ProjectModel

from .VaultObject import VaultObject
from .sqlalchemy_tables import Deployment, ObjectDefinition

from jetavator import __version__ as VERSION


class ProjectOrChangeSet(object):

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._get_items_by_type([key])
        if isinstance(key, tuple) and len(key) == 2:
            found_object = self.get(key)
            if found_object:
                return found_object
            else:
                raise KeyError(f'Could not find referenced object: {key}')
        else:
            raise Exception(
                "Item key must be in the form ['type'] or ['type', 'name']")

    def __iter__(self):
        return iter(self.keys())

    def get(self, key, default=None):
        return self.object_definitions.get(key, default)

    def keys(self):
        return self.object_definitions.keys()

    def items(self):
        return (
            (key, self.get(key))
            for key in self.keys()
        )

    def values(self):
        return (
            self.get(key)
            for key in self.keys()
        )

    def _get_items_by_type(self, keys):
        return {
            name: self[type, name]
            for type, name in self.keys()
            if type in keys
        }

    @property
    def hubs(self):
        return self._get_items_by_type(["hub"])

    @property
    def links(self):
        return self._get_items_by_type(["link"])

    @property
    def satellite_owners(self):
        return self._get_items_by_type(["hub", "link"])

    @property
    def satellites(self):
        return self._get_items_by_type(["satellite"])

    @property
    def sources(self):
        return self._get_items_by_type(["source"])

    @property
    def sql_model(self):
        return ProjectModel(self)


class Project(ProjectOrChangeSet):

    def __init__(self, registry, object_definitions, sqlalchemy_object):
        self.registry = registry
        self.object_definitions = {
            (x.type, x.name): VaultObject.subclass_instance(self, x)
            for x in object_definitions
        }
        self.sqlalchemy_object = sqlalchemy_object

    @classmethod
    def from_directory(cls, registry, directory_path):

        file_dicts = load_yamls_in_dir(directory_path)

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
            registry,
            object_definitions=[
                ObjectDefinition.from_dict(sqlalchemy_object, definition_dict)
                for definition_dict in non_projects
            ],
            sqlalchemy_object=sqlalchemy_object)

    @classmethod
    def from_sqlalchemy_object(cls, registry, sqlalchemy_object):
        return cls(
            registry,
            object_definitions=sqlalchemy_object.object_definitions,
            sqlalchemy_object=sqlalchemy_object)

    @property
    def indexed_objects(self):
        return {
            (x.type, x.name): x
            for x in self.object_definitions.values()
        }

    def add(self, definition_dict):
        key = (definition_dict["type"], definition_dict["name"])

        if key in self.object_definitions:
            raise Exception(f"Table definition {key} already exists.")

        self.object_definitions[key] = self.vault_object_from_dict(
            definition_dict)

        return self.object_definitions[key]

    def delete(self, type, name):
        key = (type, name)
        if key not in self.object_definitions:
            raise Exception(
                f"""
                Cannot delete the table {key} as it does not exist.
                """
            )
        table = self.object_definitions[key]
        if table.dependent_satellites:
            raise Exception(
                f"""
                Cannot delete table that has dependencies.
                The following satellites are dependent on the table {key}:
                {[satellite.name for satellite in table.dependent_satellites]}
                """
            )
        del self.object_definitions[key]

    def export_sqlalchemy_object(self):
        self.sqlalchemy_object.jetavator_version = VERSION
        self.sqlalchemy_object.checksum = self.checksum
        self.sqlalchemy_object.deploy_dt = str(datetime.now())
        return self.sqlalchemy_object

    @property
    def checksum(self):
        return dict_checksum({
            f"{x.type}.{x.name}": x.definition
            for x in self.object_definitions.values()
            if x.type != "project"
        })

    @property
    def version(self):
        return semver.VersionInfo.parse(self.sqlalchemy_object.version)

    @property
    def name(self):
        return self.sqlalchemy_object.name

    @property
    def deployed_time(self):
        return self.sqlalchemy_object.deploy_dt

    @property
    def latest_version(self):
        if self.sqlalchemy_object:
            return self.sqlalchemy_object.is_latest
        else:
            return None

    def vault_object_from_dict(self, deployment, definition_dict):
        return VaultObject.subclass_instance(
            self,
            ObjectDefinition.from_dict(self.sqlalchemy_object, definition_dict)
        )

    def increment_version(self, new_version=None):
        self.sqlalchemy_object = Deployment(
            name=self.sqlalchemy_object.name,
            version=(
                new_version or semver.bump_patch(
                    self.sqlalchemy_object.version)))
        self.object_definitions = {
            k: self.vault_object_from_dict(v.definition)
            for k, v in self.object_definitions.items()
        }

    # Unused method?
    #
    # @property
    # def project(self):
    #     if self["project"]:
    #         return list(self["project"].values())[0]
    #     else:
    #         return None

    @property
    def connection(self):
        return self.registry.connection

    @property
    def definitions(self):
        return [
            row.definition
            for row in self.object_definitions.values()
        ]

    @property
    def hub_aliases(self):
        return {
            alias: hub
            for link in self["link"].values()
            for alias, hub in link.hubs.items()
        }

    @property
    def validation_error(self):
        if self._duplicate_satellite_columns:
            return (
                "Duplicate columns: "
                f"{self._duplicate_satellite_columns}."
            )
        else:
            return None

    @property
    def valid(self):
        return (self.validation_error is None)

    @property
    def _duplicate_satellite_columns(self):
        column_groups = {
            name: [x[0] for x in group]
            for name, group in groupby(self._satellite_columns, lambda x: x[1])
        }
        return {
            k: v for k, v in column_groups.items()
            if len(v) > 1
        }

    @property
    def _satellite_columns(self):
        return {
            (satellite.definition["name"], column_name): column
            for satellite in self._get_items_by_type(["satellite"]).values()
            for column_name, column in satellite.definition["columns"].items()
        }


class ProjectChangeSet(ProjectOrChangeSet):

    def __init__(self, new_definition, old_definition):
        self.new_definition = new_definition
        self.old_definition = old_definition
        keys = (
            set(self.new_definition.keys()) |
            set(self.old_definition.keys())
        )
        self.object_definitions = {
            key: VaultObject.subclass_instance(
                self,
                self.new_definition.object_definitions.get(key),
                self.old_definition.object_definitions.get(key)
            )
            for key in keys
        }

    def __getattr__(self, key):
        if self.new_definition:
            return getattr(self.new_definition, key)
        else:
            return getattr(self.old_definition, key)

    def by_action(self, action):
        return {
            (type, name): self[type, name]
            for type, name in self.keys()
            if self[type, name].action == action
        }
