import os

from ...config.BaseConfig import BaseConfig

from jetavator.schema_registry import YamlProjectLoader

BEHAVE_ALIASES = {
    "db": "schema",
    "dropdb": "drop_schema_if_exists"
}

IGNORE_KEYS = ['config']


class BehaveConfig(BaseConfig):

    def __init__(
        self,
        behave_context,
        **kwargs
    ):
        self.behave_context = behave_context

        property_values = {}

        # Layer 1: load YAML file if specified
        if 'config' in behave_context.config.userdata:
            property_values.update(YamlProjectLoader.load_yaml(
                behave_context.config.userdata['config']
            ))

        # layer 2: override with any behave userdata values
        try:
            property_values.update({
                BEHAVE_ALIASES.get(k, k): v
                for k, v in behave_context.config.userdata.items()
                if k not in IGNORE_KEYS
            })
        except AttributeError:
            pass

        # layer 3: override with any values specified in kwargs
        property_values.update(kwargs)

        super().__init__(**property_values)

    @property
    def model_path(self):
        return self.get(
            "model_path",
            # get the parent directory of the behave features directory
            os.path.split(self.behave_context.config.paths[0])[0]
        )

    @model_path.setter
    def model_path(self, value):
        self["model_path"] = value
