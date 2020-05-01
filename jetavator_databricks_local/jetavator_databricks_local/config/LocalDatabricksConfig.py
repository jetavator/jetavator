from jetavator.config import DBServiceConfig
from jetavator import json_schema_objects as jso


class LocalDatabricksConfig(DBServiceConfig, register_as='local_databricks'):

    type: str = jso.Property(jso.Const['local_databricks'])
    name: str = jso.Property(jso.String)
    schema: str = jso.Property(jso.String)
