from typing import List

from jetavator.config import DBServiceConfig
from jetavator import json_schema_objects as jso

from .DatabricksLibraryConfig import DatabricksLibraryConfig


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):

    type: str = jso.Property(jso.Const('remote_databricks'))
    name: str = jso.Property(str)
    schema: str = jso.Property(str)
    host: str = jso.Property(str)
    cluster_id: str = jso.Property(str)
    org_id: str = jso.Property(str)
    token: str = jso.Property(str)
    libraries: List[DatabricksLibraryConfig] = jso.Property(jso.List(DatabricksLibraryConfig))
