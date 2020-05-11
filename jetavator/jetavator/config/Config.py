from typing import Dict

import os
import random
import uuid
import yaml
import jsdom

from lazy_property import LazyProperty

from .secret_lookup import SecretLookup
from .ConfigProperty import ConfigProperty

PROPERTIES_TO_PRINT = [
    "model_path",
    "schema",
    "drop_schema_if_exists",
    "skip_deploy",
    "environment_type"
]


class ServiceConfig(jsdom.Object):

    type: str = ConfigProperty(str)

    def name(self) -> str:
        return jsdom.key(self)


class DBServiceConfig(ServiceConfig):

    def _get_default_schema(self):
        if not jsdom.document(self) is self:
            return jsdom.document(self).schema

    type: str = ConfigProperty(str)
    # TODO: Update jso behaviour so default and default_function
    #       don't set a value persistently
    schema: str = ConfigProperty(str, default_function=_get_default_schema)


class LocalSparkConfig(DBServiceConfig, register_as='local_spark'):

    type: str = ConfigProperty(jsdom.Const('local_spark'))


class StorageConfig(jsdom.Object):

    source: str = ConfigProperty(str)
    vault: str = ConfigProperty(str)
    star: str = ConfigProperty(str)
    logs: str = ConfigProperty(str)


class SessionConfig(jsdom.Object):
    run_uuid = ConfigProperty(str, default_function=lambda self: str(uuid.uuid4()))


# TODO: add validation (or defaults) for required properties e.g. secret_lookup, services

class Config(jsdom.Object):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate()

    # TODO: Move this to testing framework - this isn't a feature
    #       required outside of self-testing
    def _generate_uid(self):
        return self.prefix + "_" + "_".join([
            "%04x" % random.randrange(16**4)
            for i in range(0, 3)
        ])

    model_path: str = ConfigProperty(str, default_function=lambda self: os.getcwd())
    schema: str = ConfigProperty(str, default_function=_generate_uid)
    prefix: str = ConfigProperty(str, default="jetavator")
    drop_schema_if_exists: bool = ConfigProperty(bool, default=False)
    skip_deploy: bool = ConfigProperty(bool, default=False)
    environment_type: str = ConfigProperty(str, default="local_spark")
    session: SessionConfig = ConfigProperty(SessionConfig, default={})
    services: Dict[str, ServiceConfig] = ConfigProperty(
        jsdom.Dict(ServiceConfig), default={})
    storage: StorageConfig = ConfigProperty(StorageConfig)
    compute: str = ConfigProperty(str)

    @LazyProperty
    def secret_lookup(self) -> SecretLookup:
        return SecretLookup.registered_subclass_instance(
            self._secret_lookup_name
        )

    _secret_lookup_name: str = jsdom.Property(str, name="secret_lookup")

    def reset_session(self):
        self.session.clear()

    def __str__(self):
        return yaml.dump(
            yaml.safe_load(self._to_json()),
            default_flow_style=False
        )
