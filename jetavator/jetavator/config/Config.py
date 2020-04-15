import os
import random
import uuid

import yaml
from lazy_property import LazyProperty

from . import json_schema_objects as jso
from .ConfigProperty import ConfigProperty
from .secret_lookup import SecretLookup

PROPERTIES_TO_PRINT = [
    "model_path",
    "schema",
    "drop_schema_if_exists",
    "skip_deploy",
    "environment_type"
]


class SecretSubstitutingConfig(jso.Object):

    @LazyProperty
    def _secret_lookup(self):
        return SecretLookup.registered_subclass_instance(
            self._document['secret_lookup']
        )

    def __getattr__(self, key):
        if key not in self.properties.keys():
            raise AttributeError(f'Attribute not found: {key}')
        # TODO: Add default values here as well as just None
        return self._secret_lookup(self.get(key, None))


class ServiceConfig(SecretSubstitutingConfig):
    properties = {
        'type': jso.String,
        'name': jso.String
    }
    index = 'name'

    def __new__(cls, *args, **kwargs):
        subclass = cls.registered_subclass(
            dict(*args, **kwargs)['type']
        )
        return super().__new__(subclass, *args, **kwargs)


class DBServiceConfig(ServiceConfig):
    properties = {
        'type': jso.String,
        'name': jso.String,
        'schema': jso.String
    }

    @property
    def schema(self):
        if self.get('schema'):
            return self['schema']
        else:
            return self._document.schema


class LocalSparkConfig(DBServiceConfig, register_as='local_spark'):
    properties = {
        'type': jso.Const['local_spark'],
        'name': jso.String,
        'schema': jso.String
    }


class MSSQLConfig(DBServiceConfig, register_as='mssql'):
    properties = {
        'type': jso.Const['mssql'],
        'name': jso.String,
        'schema': jso.String,
        'database': jso.String,
        'server': jso.String,
        'username': jso.String,
        'password': jso.String,
        'trusted_connection': jso.Bool
    }


class StorageConfig(SecretSubstitutingConfig):
    properties = {
        'source': jso.String,
        'vault': jso.String,
        'star': jso.String,
        'logs': jso.String
    }


class SessionConfig(SecretSubstitutingConfig):
    properties = {
        'run_uuid': jso.String
    }
    run_uuid = ConfigProperty("run_uuid", lambda self: str(uuid.uuid4()))


# TODO: add validation (or defaults) for required properties e.g. services

class Config(SecretSubstitutingConfig):
    properties = {
        'model_path': jso.String,
        'schema': jso.String,
        'prefix': jso.String,
        'drop_schema_if_exists': jso.String,
        'skip_deploy': jso.String,
        'environment_type': jso.String,
        'session': SessionConfig,
        'services': jso.List[ServiceConfig],
        'storage': StorageConfig,
        'compute': jso.String,
        'secret_lookup': jso.String
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate()

    def _generate_uid(self):
        return self.prefix + "_" + "_".join([
            "%04x" % random.randrange(16**4)
            for i in range(0, 3)
        ])

    model_path = ConfigProperty("model_path", lambda self: os.getcwd())
    schema = ConfigProperty("schema", _generate_uid)
    prefix = ConfigProperty("prefix", "jetavator")
    drop_schema_if_exists = ConfigProperty("drop_schema_if_exists", False)
    skip_deploy = ConfigProperty("skip_deploy", False)
    environment_type = ConfigProperty("environment_type", "local_spark")
    session = ConfigProperty("session", SessionConfig({}))

    def reset_session(self):
        self['session'] = SessionConfig({})

    def __str__(self):
        return yaml.dump(
            yaml.load(self._to_json()),
            default_flow_style=False
        )
