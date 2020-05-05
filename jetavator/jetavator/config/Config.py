from typing import List

import os
import random
import uuid

import yaml
from lazy_property import LazyProperty

from .. import json_schema_objects as jso
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
            self._document.secret_lookup
        )

    def __getattr__(self, key):
        if key not in self.properties.keys():
            raise AttributeError(f'Attribute not found: {key}')
        # TODO: Add default values here as well as just None
        return self._secret_lookup(self.get(key, None))


class ServiceConfig(SecretSubstitutingConfig):

    index = 'name'

    type: str = jso.Property(jso.String)
    name: str = jso.Property(jso.String)

    def __new__(cls, *args, **kwargs):
        subclass = cls.registered_subclass(
            dict(*args, **kwargs)['type']
        )
        return super().__new__(subclass, *args, **kwargs)


class DBServiceConfig(ServiceConfig):

    type: str = jso.Property(jso.String)
    name: str = jso.Property(jso.String)
    schema: str = jso.Property(jso.String, default=lambda self: self._document.schema)


class LocalSparkConfig(DBServiceConfig, register_as='local_spark'):

    type: str = jso.Property(jso.Const['local_spark'])


class StorageConfig(SecretSubstitutingConfig):

    source: str = jso.Property(jso.String)
    vault: str = jso.Property(jso.String)
    star: str = jso.Property(jso.String)
    logs: str = jso.Property(jso.String)


class SessionConfig(SecretSubstitutingConfig):
    run_uuid = jso.Property(jso.String, default=lambda self: str(uuid.uuid4()))


# TODO: add validation (or defaults) for required properties e.g. services

class Config(SecretSubstitutingConfig):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate()

    def _generate_uid(self):
        return self.prefix + "_" + "_".join([
            "%04x" % random.randrange(16**4)
            for i in range(0, 3)
        ])

    model_path: str = jso.Property(jso.String, default=lambda self: os.getcwd())
    schema: str = jso.Property(jso.String, default=_generate_uid)
    prefix: str = jso.Property(jso.String, default="jetavator")
    drop_schema_if_exists: bool = jso.Property(jso.Bool, default=False)
    skip_deploy: bool = jso.Property(jso.Bool, default=False)
    environment_type: str = jso.Property(jso.String, default="local_spark")
    session: SessionConfig = jso.Property(SessionConfig, default=SessionConfig({}))
    services: List[ServiceConfig] = jso.Property(jso.List[ServiceConfig])
    storage: StorageConfig = jso.Property(StorageConfig)
    compute: str = jso.Property(jso.String)
    secret_lookup: str = jso.Property(jso.String)

    def reset_session(self):
        self['session'] = SessionConfig({})

    def __str__(self):
        return yaml.dump(
            yaml.safe_load(self._to_json()),
            default_flow_style=False
        )
