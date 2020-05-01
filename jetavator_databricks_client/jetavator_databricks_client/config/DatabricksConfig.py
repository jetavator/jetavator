from typing import List

from jetavator.config import DBServiceConfig
from jetavator import json_schema_objects as jso

from .DatabricksLibraryConfig import DatabricksLibraryConfig


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):

    type: str = jso.Property(jso.Const['remote_databricks'])
    name: str = jso.Property(jso.String)
    schema: str = jso.Property(jso.String)
    host: str = jso.Property(jso.String)
    cluster_id: str = jso.Property(jso.String)
    org_id: str = jso.Property(jso.String)
    token: str = jso.Property(jso.String)
    libraries: List[DatabricksLibraryConfig] = jso.Property(jso.List[DatabricksLibraryConfig])
