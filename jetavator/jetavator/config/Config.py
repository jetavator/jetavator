from typing import Dict

import os
import uuid
import yaml
import wysdom

from lazy_property import LazyProperty

from pathlib import Path

from .secret_lookup import SecretLookup
from .ConfigProperty import ConfigProperty

PROPERTIES_TO_PRINT = [
    "model_path",
    "schema",
    "drop_schema_if_exists",
    "skip_deploy",
    "environment_type"
]

# TODO: Co-locate config classes with their related service classes
# TODO: Split services into service types in config file structure


class ServiceConfig(wysdom.UserObject, wysdom.RegistersSubclasses):
    type: str = ConfigProperty(str)

    @property
    def name(self) -> str:
        return wysdom.key(self)


class DBServiceConfig(ServiceConfig):

    def _get_default_schema(self):
        if not wysdom.document(self) is self:
            return wysdom.document(self).schema

    type: str = ConfigProperty(str)
    schema: str = ConfigProperty(str, default_function=_get_default_schema)


class LocalSparkConfig(DBServiceConfig):
    type: str = ConfigProperty(wysdom.SchemaConst('local_spark'))


class StorageConfig(wysdom.UserObject):
    source: str = ConfigProperty(str)
    vault: str = ConfigProperty(str)
    star: str = ConfigProperty(str)
    logs: str = ConfigProperty(str)


class RegistryServiceConfig(ServiceConfig):
    service_type: str = ConfigProperty(wysdom.SchemaConst('registry'))


class SQLAlchemyRegistryServiceConfig(RegistryServiceConfig):
    type: str = ConfigProperty(wysdom.SchemaConst('sqlalchemy_registry'))
    sqlalchemy_uri: str = ConfigProperty(str)


class SimpleFileRegistryServiceConfig(RegistryServiceConfig):
    type: str = ConfigProperty(wysdom.SchemaConst('simple_file_registry'))
    storage_path: str = ConfigProperty(str)


class SessionConfig(wysdom.UserObject):
    run_uuid = ConfigProperty(
        str,
        name="run_uuid",
        default_function=lambda self: str(uuid.uuid4()),
        persist_defaults=True
    )


class Config(wysdom.UserObject, wysdom.ReadsJSON, wysdom.ReadsYAML):
    model_path: str = ConfigProperty(str, default_function=lambda self: os.getcwd())
    schema: str = ConfigProperty(str)
    drop_schema_if_exists: bool = ConfigProperty(bool, default=False)
    skip_deploy: bool = ConfigProperty(bool, default=False)
    environment_type: str = ConfigProperty(str, default="local_spark")
    session: SessionConfig = ConfigProperty(SessionConfig, default={}, persist_defaults=True)
    services: Dict[str, ServiceConfig] = ConfigProperty(
        wysdom.SchemaDict(ServiceConfig), default={}, persist_defaults=True)
    storage: StorageConfig = ConfigProperty(StorageConfig)
    compute: str = ConfigProperty(str)
    registry: str = ConfigProperty(str)

    @LazyProperty
    def secret_lookup(self) -> SecretLookup:
        return SecretLookup.registered_subclass_instance(
            self._secret_lookup_name
        )

    _secret_lookup_name: str = wysdom.UserProperty(str, name="secret_lookup")

    def reset_session(self):
        self.session.clear()

    def __str__(self):
        return yaml.dump(
            self.to_builtin(),
            default_flow_style=False
        )

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
        config_dict = self.to_builtin()
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
