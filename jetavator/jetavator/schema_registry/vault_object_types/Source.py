from ..VaultObject import VaultObject


class SourceColumn(object):

    def __init__(self, definition):
        self.definition = definition

    def __getattr__(self, key):
        return self.definition.get(key)


class Source(VaultObject, register_as="source"):

    required_yaml_properties = ["columns"]

    optional_yaml_properties = []

    @property
    def columns(self):
        return {
            k: SourceColumn(v)
            for k, v in self.definition["columns"].items()
        }

    @property
    def primary_key_columns(self):
        return {
            k: v
            for k, v in self.columns.items()
            if v.pk
        }
