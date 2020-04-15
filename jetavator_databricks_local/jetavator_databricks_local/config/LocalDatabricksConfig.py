from jetavator.config import DBServiceConfig
from jetavator.config import json_schema_objects as jso


class LocalDatabricksConfig(DBServiceConfig, register_as='local_databricks'):
    properties = {
        'type': jso.Const['local_databricks'],
        'name': jso.String,
        'schema': jso.String
    }
