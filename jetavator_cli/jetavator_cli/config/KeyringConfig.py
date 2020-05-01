import os
import uuid
import json
import yaml

from .BaseConfig import BaseConfig
from .CommandLineConfig import CommandLineConfig
from jetavator_cli.utils import print_to_console

from pathlib import Path


class KeyringConfig(BaseConfig):

    @classmethod
    def load(cls, other_config=None):
        cls.make_config_dir()
        if os.path.exists(cls.config_file()):
            with open(cls.config_file(), 'r') as f:
                new_object = cls(yaml.safe_load(f) or {})
        else:
            new_object = cls({})
        if other_config:
            new_object.update(other_config)
        return new_object

    @classmethod
    def config_dir(cls):
        return os.path.join(Path.home(), '.jetavator')

    @classmethod
    def config_file(cls):
        return os.path.join(cls.config_dir(), 'config.yml')

    @classmethod
    def make_config_dir(cls):
        if not os.path.exists(cls.config_dir()):
            os.makedirs(cls.config_dir())

    def save(self):
        config_dict = yaml.safe_load(self._to_json())
        # Don't save session specific config info
        if 'session' in config_dict:
            del config_dict['session']
        self.make_config_dir()
        with open(self.config_file(), 'w') as f:
            f.write(
                yaml.dump(
                    config_dict,
                    default_flow_style=False
                )
            )

    @classmethod
    def command_line_options_to_keyring(cls, options, delete_previous=True):
        keyring_config = cls(CommandLineConfig(options))
        keyring_config.save()
        return keyring_config
