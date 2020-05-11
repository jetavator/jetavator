from typing import List

from jetavator.config import DBServiceConfig
import jsdom

from .DatabricksLibraryConfig import DatabricksLibraryConfig


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):

    type: str = jsdom.Property(jsdom.Const('remote_databricks'))
    name: str = jsdom.Property(str)
    schema: str = jsdom.Property(str)
    host: str = jsdom.Property(str)
    cluster_id: str = jsdom.Property(str)
    org_id: str = jsdom.Property(str)
    token: str = jsdom.Property(str)
    libraries: List[DatabricksLibraryConfig] = jsdom.Property(jsdom.List(DatabricksLibraryConfig))
