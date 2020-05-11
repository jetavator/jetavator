from typing import Dict

from jetavator import json_schema_objects as jso


class ServiceExample(jso.Object):

    type: str = jso.Property(str)

    def name(self) -> str:
        return jso.key(self)


class DBServiceExample(ServiceExample):

    type: str = jso.Property(str)


class LocalSparkExample(DBServiceExample):

    type: str = jso.Property(jso.Const('local_spark'))


class StorageExample(jso.Object):

    source: str = jso.Property(str)
    vault: str = jso.Property(str)
    star: str = jso.Property(str)
    logs: str = jso.Property(str)


class Example(jso.Object):

    prefix: str = jso.Property(str, default="jetavator")
    drop_schema_if_exists: bool = jso.Property(bool, default=False)
    skip_deploy: bool = jso.Property(bool, default=False)
    environment_type: str = jso.Property(str, default="local_spark")
    services: Dict[str, ServiceExample] = jso.Property(
        jso.Dict(ServiceExample), default={})
    storage: StorageExample = jso.Property(StorageExample)
    compute: str = jso.Property(str)
