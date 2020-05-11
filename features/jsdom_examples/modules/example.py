from typing import Dict

import jsdom


class ServiceExample(jsdom.Object):

    type: str = jsdom.Property(str)

    def name(self) -> str:
        return jsdom.key(self)


class DBServiceExample(ServiceExample):

    type: str = jsdom.Property(str)


class LocalSparkExample(DBServiceExample):

    type: str = jsdom.Property(jsdom.Const('local_spark'))


class StorageExample(jsdom.Object):

    source: str = jsdom.Property(str)
    vault: str = jsdom.Property(str)
    star: str = jsdom.Property(str)
    logs: str = jsdom.Property(str)


class Example(jsdom.Object):

    prefix: str = jsdom.Property(str, default="jetavator")
    drop_schema_if_exists: bool = jsdom.Property(bool, default=False)
    skip_deploy: bool = jsdom.Property(bool, default=False)
    environment_type: str = jsdom.Property(str, default="local_spark")
    services: Dict[str, ServiceExample] = jsdom.Property(
        jsdom.Dict(ServiceExample), default={})
    storage: StorageExample = jsdom.Property(StorageExample)
    compute: str = jsdom.Property(str)
