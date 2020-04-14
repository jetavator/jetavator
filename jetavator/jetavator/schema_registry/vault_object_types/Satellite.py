from ..VaultObject import VaultObject, HubKeyColumn
from .pipelines import SatellitePipeline

from jetavator import utils

from functools import lru_cache


class SatelliteColumn(object):

    def __init__(self, definition):
        self.definition = definition

    def __getattr__(self, key):
        return self.definition.get(key)


class Satellite(VaultObject, register_as="satellite"):

    required_yaml_properties = ["parent", "columns", "pipeline"]

    optional_yaml_properties = ["exclude_from_star_schema"]

    # Do this to trigger validation of self.pipeline at init time
    # Improve this in future - implement generic mechanism for populating
    # and validating object tree from YAML
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.pipeline
        except Exception as e:
            raise Exception(
                str(e) + '\n'
                f' in object [{self.name}]'
                f' with type [{self.type}]'
            )

    @property
    def columns(self):
        return {
            k: SatelliteColumn(v)
            for k, v in self.definition["columns"].items()
        }

    @property
    def pipeline(self):
        return SatellitePipeline.registered_subclass_instance(
            name=self.definition["pipeline"]["type"],
            project=self.project,
            parent=self,
            definition=self.definition["pipeline"]
        )

    @property
    def hub_reference_columns(self):
        return {
            k: v
            for k, v in self.columns.items()
            if v.hub_reference
        }

    @property
    def referenced_hubs(self):
        return {
            hub_name: self.project["hub", hub_name]
            for hub_name in set(
                x.hub_reference
                for x in self.hub_reference_columns.values()
            )
        }

    @property
    def parent(self):
        return self._resolve_reference(self.definition["parent"])

    @property
    def dependencies_by_owner(self):
        dependencies = [
            x.object_reference
            for x in self.pipeline.dependencies
            if x.type == 'satellite'
        ]
        return [
            (self.project[k], list(v))
            for k, v in utils.sort_and_group_by(
                dependencies, key=lambda x: (x.parent.type, x.parent.name))
        ]

    @property
    def has_dependencies_in_same_owner(self):
        return any([
            True
            for x in self.pipeline.dependencies
            if x.type == 'satellite'
            and x.object_reference.parent.name == self.parent.name
            and x.object_reference.parent.type == self.parent.type
        ])

    @property
    def full_name(self):
        return f'sat_{self.name}'

    @property
    def hub_key_columns(self):
        # check if this can be safely refactored to
        # a function hub_key_columns(self, hub_name)
        columns = self.parent.hub_key_columns(self)
        if (
            self.hub_reference_columns
            and not self.pipeline.performance_hints.no_update_referenced_hubs
        ):
            for column_name, column in self.hub_reference_columns.items():
                columns.setdefault(column.hub_reference, []).append(
                    HubKeyColumn(column_name, f'hub_{column.hub_reference}'))
        return columns

    @lru_cache(maxsize=None)
    def input_keys(self, type):
        return {
            name: self.project[type, name]
            for name in set(
                x for dep in self.pipeline.dependencies
                if dep.type == 'satellite'
                for x in dep.object_reference.output_keys(type)
            )
        }

    @lru_cache(maxsize=None)
    def produced_keys(self, type):
        if (
            type == 'hub'
            and not self.pipeline.performance_hints.no_update_hubs
        ):
            return {
                name: self.project['hub', name]
                for name in self.hub_key_columns
            }
        elif (
            type == 'link'
            and self.parent.type == 'link'
            and not self.pipeline.performance_hints.no_update_links
        ):
            return {
                self.parent.name: self.parent
            }
        else:
            return {}

    @lru_cache(maxsize=None)
    def output_keys(self, type):
        return {
            name: self.project[type, name]
            for name in (
                set(self.produced_keys(type).keys()) |
                set(self.input_keys(type).keys())
            )
        }

    def dependent_satellites_by_owner(self, key):
        return [
            dep.object_reference
            for dep in self.pipeline.dependencies
            if dep.type == 'satellite'
            for output_key in dep.object_reference.output_keys(key.type)
            if output_key == key.name
        ]