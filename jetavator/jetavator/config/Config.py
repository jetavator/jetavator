from typing import Dict

import os
import random
import uuid

import yaml

from lazy_property import LazyProperty

from .. import json_schema_objects as jso

from .secret_lookup import SecretLookup
from .ConfigProperty import ConfigProperty

PROPERTIES_TO_PRINT = [
    "model_path",
    "schema",
    "drop_schema_if_exists",
    "skip_deploy",
    "environment_type"
]


class ServiceConfig(jso.Object):

    type: str = ConfigProperty(jso.String)

    def name(self) -> str:
        return jso.key(self)


class DBServiceConfig(ServiceConfig):

    def _get_default_schema(self):
        if not jso.document(self) is self:
            return jso.document(self).schema

    type: str = ConfigProperty(jso.String)
    schema: str = ConfigProperty(jso.String, default_function=_get_default_schema)


class LocalSparkConfig(DBServiceConfig, register_as='local_spark'):

    type: str = ConfigProperty(jso.Const['local_spark'])


class StorageConfig(jso.Object):

    source: str = ConfigProperty(jso.String)
    vault: str = ConfigProperty(jso.String)
    star: str = ConfigProperty(jso.String)
    logs: str = ConfigProperty(jso.String)


class SessionConfig(jso.Object):
    run_uuid = ConfigProperty(jso.String, default_function=lambda self: str(uuid.uuid4()))


# TODO: add validation (or defaults) for required properties e.g. secret_lookup, services

class Config(jso.Object):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate()

    def _generate_uid(self):
        return self.prefix + "_" + "_".join([
            "%04x" % random.randrange(16**4)
            for i in range(0, 3)
        ])

    model_path: str = ConfigProperty(jso.String, default_function=lambda self: os.getcwd())
    schema: str = ConfigProperty(jso.String, default_function=_generate_uid)
    prefix: str = ConfigProperty(jso.String, default="jetavator")
    drop_schema_if_exists: bool = ConfigProperty(jso.Boolean, default=False)
    skip_deploy: bool = ConfigProperty(jso.Boolean, default=False)
    environment_type: str = ConfigProperty(jso.String, default="local_spark")
    session: SessionConfig = ConfigProperty(SessionConfig, default={})
    services: Dict[str, ServiceConfig] = ConfigProperty(
        jso.Dict[ServiceConfig], default={})
    storage: StorageConfig = ConfigProperty(StorageConfig)
    compute: str = ConfigProperty(jso.String)

    @LazyProperty
    def secret_lookup(self) -> SecretLookup:
        return SecretLookup.registered_subclass_instance(
            self._secret_lookup_name
        )

    _secret_lookup_name: str = jso.Property(jso.String, name="secret_lookup")

    def reset_session(self):
        self.session.clear()

    def __str__(self):
        return yaml.dump(
            yaml.safe_load(self._to_json()),
            default_flow_style=False
        )
