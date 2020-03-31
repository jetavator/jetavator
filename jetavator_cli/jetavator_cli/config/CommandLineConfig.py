from .BaseConfig import BaseConfig

from ..utils import load_yaml


class CommandLineConfig(BaseConfig):

    def __init__(
        self,
        options,
        **kwargs
    ):

        property_values = {}

        # Layer 1: load YAML file if specified
        if options.get("--config-file"):
            property_values.update(load_yaml(options["--config-file"]))

        # layer 2: override with any command line parameters
        property_values.update(dict(
            tuple(option_string.split("=", 1))
            for option_string in options["<option>=<value>"]
        ))

        # layer 3: override with any values specified in kwargs
        property_values.update(kwargs)

        super().__init__(**property_values)
