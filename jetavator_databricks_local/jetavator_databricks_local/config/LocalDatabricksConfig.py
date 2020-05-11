from jetavator.config import DBServiceConfig
import jsdom


class LocalDatabricksConfig(DBServiceConfig, register_as='local_databricks'):

    type: str = jsdom.Property(jsdom.Const('local_databricks'))
    name: str = jsdom.Property(str)
    schema: str = jsdom.Property(str)
