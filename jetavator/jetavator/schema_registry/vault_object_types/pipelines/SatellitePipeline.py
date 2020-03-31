from jetavator.mixins import RegistersSubclasses, HasYamlProperties
from jetavator.sql_model import HasSQLModel


class PerformanceHints(object):

    def __init__(self, definition):
        self.definition = definition

    def __getattr__(self, key):
        return self.definition.get(key)


class SatellitePipelineDependency(HasYamlProperties):
    required_yaml_properties = ["name", "type"]
    optional_yaml_properties = ["view"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # validate that the object reference exists
        self.object_reference

    @property
    def object_reference(self):
        return self.project[self.type, self.name]


class SatellitePipeline(RegistersSubclasses, HasSQLModel, HasYamlProperties):
    required_yaml_properties = ["type"]
    optional_yaml_properties = ["key_columns", "performance_hints"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.satellite = self.parent
        # validate the dependencies
        self.dependencies

    @property
    def key_columns(self):
        raise NotImplementedError

    @property
    def performance_hints(self):
        return PerformanceHints(
            self.definition.get("performance_hints", {})
        )

    @property
    def dependencies(self):
        raise NotImplementedError
