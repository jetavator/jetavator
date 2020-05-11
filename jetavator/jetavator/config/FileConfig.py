import os
import yaml

from .Config import Config
from .CommandLineConfig import CommandLineConfig

from pathlib import Path


# TODO: Review if this class is necessary or if it can be integrated into Config

class FileConfig(Config):

    @classmethod
    def load(cls, other_config=None):
        cls.make_config_dir()
        if os.path.exists(cls.config_file()):
            with open(cls.config_file(), 'r') as f:
                new_object = cls(yaml.safe_load(f) or {})
        else:
            new_object = cls({})
        if other_config:
            new_object.update(other_config._value)
        return new_object

    @classmethod
    def config_dir(cls):
        return os.path.join(str(Path.home()), '.jetavator')

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

    # TODO: Rename to reflect actual role
    @classmethod
    def command_line_options_to_keyring(cls, options, delete_previous=True):
        keyring_config = cls(CommandLineConfig(options)._value)
        keyring_config.save()
        return keyring_config
