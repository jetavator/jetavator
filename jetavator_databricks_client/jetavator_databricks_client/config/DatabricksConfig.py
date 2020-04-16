from jetavator.config import DBServiceConfig
from jetavator.config import json_schema_objects as jso

from .DatabricksLibraryConfig import DatabricksLibraryConfig


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):
    properties = {
        'type': jso.Const['remote_databricks'],
        'name': jso.String,
        'schema': jso.String,
        'host': jso.String,
        'cluster_id': jso.String,
        'org_id': jso.String,
        'token': jso.String,
        'libraries': jso.List[DatabricksLibraryConfig]
    }
