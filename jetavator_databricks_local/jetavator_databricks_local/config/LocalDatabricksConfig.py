from jetavator.config import DBServiceConfig
import wysdom


class LocalDatabricksConfig(DBServiceConfig, register_as='local_databricks'):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('local_databricks'))
    name: str = wysdom.UserProperty(str)
    schema: str = wysdom.UserProperty(str)
