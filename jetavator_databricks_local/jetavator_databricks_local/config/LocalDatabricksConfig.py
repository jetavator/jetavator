from jetavator.config import DBServiceConfig
from jetavator import json_schema_objects as jso


class LocalDatabricksConfig(DBServiceConfig, register_as='local_databricks'):

    type: str = jso.Property(jso.Const('local_databricks'))
    name: str = jso.Property(str)
    schema: str = jso.Property(str)
