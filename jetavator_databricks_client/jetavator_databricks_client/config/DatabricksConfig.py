from typing import List

from jetavator.config import DBServiceConfig
import wysdom

from .DatabricksLibraryConfig import DatabricksLibraryConfig


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('remote_databricks'))
    name: str = wysdom.UserProperty(str)
    schema: str = wysdom.UserProperty(str)
    host: str = wysdom.UserProperty(str)
    cluster_id: str = wysdom.UserProperty(str)
    org_id: str = wysdom.UserProperty(str)
    token: str = wysdom.UserProperty(str)
    libraries: List[DatabricksLibraryConfig] = wysdom.UserProperty(wysdom.SchemaArray(DatabricksLibraryConfig))
